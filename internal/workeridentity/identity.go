// Package workeridentity implements master-issued worker identities: a
// private CA held by the master, certificates whose only identity claim is a
// URI SAN spiffe://orabbit/worker/<uuid>, and helpers for workers to create
// their key pair and certificate signing request.
package workeridentity

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// URIPrefix is the SAN prefix carrying a worker's identity.
const URIPrefix = "spiffe://orabbit/worker/"

const caValidity = 10 * 365 * 24 * time.Hour

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// NewWorkerID returns a random (version 4) UUID.
func NewWorkerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

// ValidWorkerID reports whether id is a canonical lowercase version 4 UUID.
func ValidWorkerID(id string) bool { return uuidPattern.MatchString(id) }

// CA signs worker certificates.
type CA struct {
	cert    *x509.Certificate
	key     crypto.Signer
	certPEM string
}

// GenerateCA creates a new self-signed worker CA and returns its certificate
// PEM and PKCS#8 DER private key.
func GenerateCA(now time.Time) (string, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return "", nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "O_Rabbit worker CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(caValidity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", nil, err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), keyDER, nil
}

// ParseCA loads a CA from its certificate PEM and PKCS#8 DER private key.
func ParseCA(certPEM string, keyDER []byte) (*CA, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("worker CA certificate is not PEM encoded")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse worker CA certificate: %w", err)
	}
	if !cert.IsCA {
		return nil, errors.New("worker CA certificate is not a CA")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, fmt.Errorf("parse worker CA key: %w", err)
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, errors.New("worker CA key cannot sign")
	}
	return &CA{cert: cert, key: signer, certPEM: certPEM}, nil
}

// CertificatePEM returns the CA certificate.
func (ca *CA) CertificatePEM() string { return ca.certPEM }

// Pool returns a pool containing only this CA, for verifying worker
// certificates.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// Issued describes a newly signed worker certificate.
type Issued struct {
	CertificatePEM string
	Serial         string
	NotAfter       time.Time
}

// Issue signs a client certificate for workerID from csrPEM. Every identity
// claim in the CSR is ignored: the certificate carries only the URI SAN for
// workerID, chosen by the master.
func (ca *CA) Issue(csrPEM, workerID string, ttl time.Duration, now time.Time) (Issued, error) {
	if !ValidWorkerID(workerID) {
		return Issued{}, errors.New("invalid worker identity")
	}
	if ttl <= 0 {
		return Issued{}, errors.New("certificate lifetime must be positive")
	}
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return Issued{}, errors.New("certificate request is not PEM encoded")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return Issued{}, fmt.Errorf("parse certificate request: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return Issued{}, fmt.Errorf("certificate request signature: %w", err)
	}
	if err := checkPublicKey(csr.PublicKey); err != nil {
		return Issued{}, err
	}
	serial, err := randomSerial()
	if err != nil {
		return Issued{}, err
	}
	notAfter := now.Add(ttl)
	if notAfter.After(ca.cert.NotAfter) {
		notAfter = ca.cert.NotAfter
	}
	uri, _ := url.Parse(URIPrefix + workerID)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: workerID},
		URIs:         []*url.URL{uri},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, csr.PublicKey, ca.key)
	if err != nil {
		return Issued{}, fmt.Errorf("sign worker certificate: %w", err)
	}
	return Issued{
		CertificatePEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		Serial:         hex.EncodeToString(serial.Bytes()),
		NotAfter:       notAfter,
	}, nil
}

// IdentityFromCertificate extracts the worker ID from a verified worker
// certificate. The certificate must carry exactly one URI SAN, in the worker
// namespace, naming a valid worker ID.
func IdentityFromCertificate(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", errors.New("missing worker certificate")
	}
	if len(cert.URIs) != 1 {
		return "", errors.New("worker certificate must carry exactly one URI SAN")
	}
	uri := cert.URIs[0].String()
	if !strings.HasPrefix(uri, URIPrefix) {
		return "", errors.New("worker certificate URI SAN is outside the worker namespace")
	}
	id := strings.TrimPrefix(uri, URIPrefix)
	if !ValidWorkerID(id) {
		return "", errors.New("worker certificate carries an invalid worker ID")
	}
	return id, nil
}

// NewKeyAndCSR creates a worker private key (PKCS#8 PEM) and a CSR for it.
// The CSR carries no identity claims; the master assigns the identity.
func NewKeyAndCSR() (keyPEM, csrPEM string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "orabbit-worker"}}, key)
	if err != nil {
		return "", "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})), nil
}

func checkPublicKey(pub any) error {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() && k.Curve != elliptic.P384() {
			return errors.New("certificate request uses an unsupported ECDSA curve")
		}
	case ed25519.PublicKey:
	case *rsa.PublicKey:
		if k.N.BitLen() < 2048 {
			return errors.New("certificate request RSA key must be at least 2048 bits")
		}
	default:
		return errors.New("certificate request uses an unsupported key type")
	}
	return nil
}

func randomSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}
