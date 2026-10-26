package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// catalogProxy sits between the master and the Iceberg REST catalog and can
// fail one chosen request, so a test controls exactly where a registration
// breaks. Every other request passes through unchanged.
type catalogProxy struct {
	upstream *url.URL
	pass     *httputil.ReverseProxy
	srv      *http.Server
	ln       net.Listener

	mu    sync.Mutex
	fault *catalogFault
	// reached counts requests that got to the catalog, by "METHOD path".
	reached map[string]int
}

// faultKind says where the injected failure happens relative to the catalog
// commit, which decides whether O_Rabbit may retry or must reconcile.
type faultKind int

const (
	// faultBeforeCommit holds the request without forwarding it, then drops
	// the connection. The catalog never sees it, so the table is unchanged and
	// a plain retry is safe.
	faultBeforeCommit faultKind = iota
	// faultLoseCommitReply forwards the request, so the catalog applies the
	// commit, then holds and drops the reply. O_Rabbit cannot know whether
	// the commit happened and must reconcile instead of committing again.
	faultLoseCommitReply
)

type catalogFault struct {
	kind   faultKind
	method string
	path   string
	hold   time.Duration
	// fired is closed when the fault has been applied once.
	fired chan struct{}
	// upstreamStatus is the catalog's real status code for a lost reply.
	upstreamStatus int
}

func startCatalogProxy(listen, upstream string) (*catalogProxy, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse catalog url: %w", err)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("listen for catalog proxy on %s: %w", listen, err)
	}
	p := &catalogProxy{upstream: u, pass: httputil.NewSingleHostReverseProxy(u), ln: ln, reached: map[string]int{}}
	p.srv = &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = p.srv.Serve(ln) }()
	return p, nil
}

func (p *catalogProxy) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.srv.Shutdown(ctx)
}

// inject arms a one-shot fault for the next request with this method and
// exact path. It returns the fault so the caller can wait for it to fire.
func (p *catalogProxy) inject(kind faultKind, method, path string, hold time.Duration) *catalogFault {
	f := &catalogFault{kind: kind, method: method, path: path, hold: hold, fired: make(chan struct{})}
	p.mu.Lock()
	p.fault = f
	p.mu.Unlock()
	return f
}

// clear disarms a fault that has not fired, so it cannot leak into the next
// scenario.
func (p *catalogProxy) clear() {
	p.mu.Lock()
	p.fault = nil
	p.mu.Unlock()
}

// reachedCatalog returns how many requests with this method and path the
// proxy has delivered to the catalog so far.
func (p *catalogProxy) reachedCatalog(method, path string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reached[method+" "+path]
}

// take returns the armed fault if it matches the request. Otherwise the
// request will reach the catalog, and it is counted.
func (p *catalogProxy) take(r *http.Request) *catalogFault {
	p.mu.Lock()
	defer p.mu.Unlock()
	f := p.fault
	if f == nil || r.Method != f.method || r.URL.Path != f.path {
		p.reached[r.Method+" "+r.URL.Path]++
		return nil
	}
	p.fault = nil
	if f.kind == faultLoseCommitReply {
		p.reached[r.Method+" "+r.URL.Path]++
	}
	return f
}

func (p *catalogProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f := p.take(r)
	if f == nil {
		p.pass.ServeHTTP(w, r)
		return
	}
	defer close(f.fired)
	if f.kind == faultLoseCommitReply {
		status, err := p.forward(r)
		if err != nil {
			// The commit may not have reached the catalog; the scenario's
			// assertions would be meaningless, so fail loudly in the reply.
			http.Error(w, "e2e proxy: forward failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		f.upstreamStatus = status
	}
	// Hold the connection past the master's registration timeout, or until
	// the master gives up, then close it without a reply.
	select {
	case <-time.After(f.hold):
	case <-r.Context().Done():
	}
	panic(http.ErrAbortHandler)
}

// forward sends the request to the catalog and drains the reply, returning
// the catalog's status code.
func (p *catalogProxy) forward(r *http.Request) (int, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return 0, err
	}
	target := *p.upstream
	target.Path = r.URL.Path
	target.RawQuery = r.URL.RawQuery
	// Not bound to r.Context(): the commit must complete even when the master
	// times out first, which is the ambiguous case under test.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	out.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(out)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, err
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, errors.New("catalog rejected the forwarded commit: " + resp.Status)
	}
	return resp.StatusCode, nil
}
