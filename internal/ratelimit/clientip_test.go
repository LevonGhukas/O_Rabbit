package ratelimit

import (
	"net/netip"
	"testing"
)

func mustProxies(t *testing.T, raw string) []netip.Prefix {
	t.Helper()
	p, err := ParseTrustedProxies(raw)
	if err != nil {
		t.Fatalf("ParseTrustedProxies(%q): %v", raw, err)
	}
	return p
}

func TestParseTrustedProxies(t *testing.T) {
	got := mustProxies(t, " 10.0.0.0/8, 192.168.1.5 ,::1 ,")
	want := []string{"10.0.0.0/8", "192.168.1.5/32", "::1/128"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Fatalf("prefix %d = %s, want %s", i, got[i], want[i])
		}
	}
	if p := mustProxies(t, ""); len(p) != 0 {
		t.Fatalf("empty input parsed to %v", p)
	}
	if _, err := ParseTrustedProxies("10.0.0.0/8,not-an-ip"); err == nil {
		t.Fatal("invalid entry accepted")
	}
}

func TestClientIPIgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	got := r.ClientIP("203.0.113.7:5000", []string{"198.51.100.1"})
	if got.String() != "203.0.113.7" {
		t.Fatalf("client = %s, want the peer address", got)
	}
}

func TestClientIPWithoutTrustedProxiesUsesPeer(t *testing.T) {
	r := NewClientResolver(nil)

	got := r.ClientIP("10.0.0.2:5000", []string{"198.51.100.1"})
	if got.String() != "10.0.0.2" {
		t.Fatalf("client = %s, want the peer address", got)
	}
}

func TestClientIPUsesRightmostUntrustedHop(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	// The client spoofed 1.1.1.1; the edge proxy appended the real address,
	// then an internal proxy appended the edge proxy's.
	got := r.ClientIP("10.0.0.2:5000", []string{"1.1.1.1, 198.51.100.9", "10.0.0.3"})
	if got.String() != "198.51.100.9" {
		t.Fatalf("client = %s, want 198.51.100.9", got)
	}
}

func TestClientIPAcceptsHopsWithPorts(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	if got := r.ClientIP("10.0.0.2:5000", []string{"198.51.100.9:5678"}); got.String() != "198.51.100.9" {
		t.Fatalf("client = %s, want 198.51.100.9", got)
	}
	if got := r.ClientIP("10.0.0.2:5000", []string{"[2001:db8::7]:443"}); got.String() != "2001:db8::7" {
		t.Fatalf("client = %s, want 2001:db8::7", got)
	}
}

func TestClientIPAllHopsTrustedUsesLeftmost(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	got := r.ClientIP("10.0.0.2:5000", []string{"10.0.0.4, 10.0.0.3"})
	if got.String() != "10.0.0.4" {
		t.Fatalf("client = %s, want 10.0.0.4", got)
	}
}

func TestClientIPMalformedHopFallsBackToLastTrustedHop(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	got := r.ClientIP("10.0.0.2:5000", []string{"garbage, 10.0.0.3"})
	if got.String() != "10.0.0.3" {
		t.Fatalf("client = %s, want 10.0.0.3", got)
	}
}

func TestClientIPTrustedPeerWithoutHeaderUsesPeer(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "10.0.0.0/8"))

	if got := r.ClientIP("10.0.0.2:5000", nil); got.String() != "10.0.0.2" {
		t.Fatalf("client = %s, want the peer address", got)
	}
}

func TestClientIPAcceptsBracketedIPv6AndMappedIPv4(t *testing.T) {
	r := NewClientResolver(mustProxies(t, "::1"))

	if got := r.ClientIP("[::1]:5000", []string{"[2001:db8::7]"}); got.String() != "2001:db8::7" {
		t.Fatalf("client = %s, want 2001:db8::7", got)
	}
	if got := r.ClientIP("[::ffff:203.0.113.7]:5000", nil); got.String() != "203.0.113.7" {
		t.Fatalf("client = %s, want the unmapped IPv4 address", got)
	}
}

func TestKeyGroupsIPv6ByPrefix(t *testing.T) {
	a := Key(netip.MustParseAddr("2001:db8:1:2::1"))
	b := Key(netip.MustParseAddr("2001:db8:1:2:ffff::9"))
	c := Key(netip.MustParseAddr("2001:db8:1:3::1"))
	if a != b {
		t.Fatalf("addresses in one /64 got different keys: %s, %s", a, b)
	}
	if a == c {
		t.Fatalf("addresses in different /64s share key %s", a)
	}
	if got := Key(netip.MustParseAddr("203.0.113.7")); got != "203.0.113.7" {
		t.Fatalf("IPv4 key = %s", got)
	}
	if got := Key(netip.Addr{}); got != UnknownClient {
		t.Fatalf("invalid address key = %s, want %s", got, UnknownClient)
	}
}

func TestPeerKey(t *testing.T) {
	if got := PeerKey("203.0.113.7:443"); got != "203.0.113.7" {
		t.Fatalf("PeerKey = %s", got)
	}
	if got := PeerKey("not an address"); got != UnknownClient {
		t.Fatalf("PeerKey = %s, want %s", got, UnknownClient)
	}
}
