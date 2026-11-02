package ratelimit

import (
	"fmt"
	"net/netip"
	"strings"
)

// UnknownClient is the key for a request whose address cannot be parsed. All
// such requests share one budget.
const UnknownClient = "unknown"

// ipv6ClientPrefixBits groups IPv6 clients by their /64: a single host
// usually controls a whole /64 and could otherwise rotate addresses to get a
// fresh budget for each.
const ipv6ClientPrefixBits = 64

// ParseTrustedProxies parses a comma-separated list of IP addresses and CIDR
// prefixes. Empty entries are ignored.
func ParseTrustedProxies(raw string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			p, err := netip.ParsePrefix(entry)
			if err != nil {
				return nil, fmt.Errorf("invalid trusted proxy %q: %w", entry, err)
			}
			out = append(out, p.Masked())
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q: %w", entry, err)
		}
		addr = addr.Unmap()
		out = append(out, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return out, nil
}

// ClientResolver derives the client address of an HTTP request. X-Forwarded-For
// is honoured only when the connection comes from a trusted proxy, and then
// only up to the first hop that is not itself a trusted proxy, so a client
// cannot choose its own address by sending the header.
type ClientResolver struct {
	trusted []netip.Prefix
}

// NewClientResolver returns a resolver that trusts the given proxies.
func NewClientResolver(trusted []netip.Prefix) ClientResolver {
	return ClientResolver{trusted: append([]netip.Prefix(nil), trusted...)}
}

// ClientIP returns the client address for a request received from remoteAddr
// (host:port) carrying the given X-Forwarded-For header values. It returns the
// zero Addr when remoteAddr cannot be parsed.
func (c ClientResolver) ClientIP(remoteAddr string, forwardedFor []string) netip.Addr {
	client := parseHost(remoteAddr)
	if !client.IsValid() || !c.isTrusted(client) {
		return client
	}
	hops := forwardedHops(forwardedFor)
	for i := len(hops) - 1; i >= 0; i-- {
		// Some proxies record host:port rather than a bare address.
		hop := parseHost(hops[i])
		if !hop.IsValid() {
			// A malformed hop cannot be keyed; the last trusted proxy is the
			// most specific address still known.
			return client
		}
		client = hop
		if !c.isTrusted(hop) {
			return hop
		}
	}
	return client
}

func (c ClientResolver) isTrusted(addr netip.Addr) bool {
	for _, p := range c.trusted {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// Key returns the limiter key for a client address.
func Key(addr netip.Addr) string {
	if !addr.IsValid() {
		return UnknownClient
	}
	addr = addr.Unmap()
	if addr.Is6() {
		p, err := addr.Prefix(ipv6ClientPrefixBits)
		if err != nil {
			return UnknownClient
		}
		return p.String()
	}
	return addr.String()
}

// PeerKey returns the limiter key for a connection's remote address
// (host:port), as reported by net.Addr.String.
func PeerKey(remoteAddr string) string {
	return Key(parseHost(remoteAddr))
}

func forwardedHops(values []string) []string {
	var hops []string
	for _, v := range values {
		for hop := range strings.SplitSeq(v, ",") {
			hops = append(hops, strings.TrimSpace(hop))
		}
	}
	return hops
}

func parseHost(hostPort string) netip.Addr {
	if ap, err := netip.ParseAddrPort(hostPort); err == nil {
		return ap.Addr().Unmap()
	}
	return parseAddr(hostPort)
}

func parseAddr(s string) netip.Addr {
	addr, err := netip.ParseAddr(strings.Trim(strings.TrimSpace(s), "[]"))
	if err != nil {
		return netip.Addr{}
	}
	return addr.Unmap()
}
