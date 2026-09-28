package grpcapi

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ServerTLSConfig builds the master's gRPC TLS configuration. When
// workerCAs is set, a client certificate is verified against it whenever one
// is presented; the identity interceptor then requires one on every worker
// RPC except enrollment.
func ServerTLSConfig(certFile, keyFile string, workerCAs *x509.CertPool) (*tls.Config, error) {
	certFile, keyFile = strings.TrimSpace(certFile), strings.TrimSpace(keyFile)
	if certFile == "" || keyFile == "" {
		return nil, errors.New("gRPC TLS requires both a certificate and a private key")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load gRPC server certificate: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if workerCAs != nil {
		cfg.ClientCAs = workerCAs
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// ClientTLSConfig builds a worker's gRPC TLS configuration. caFile pins the
// CA that signed the master certificate (system roots when empty).
// clientCert, when set, supplies the worker identity certificate for each
// handshake, so a renewed certificate is used on the next connection.
func ClientTLSConfig(caFile, serverName string, clientCert func() (*tls.Certificate, error)) (*tls.Config, error) {
	caFile = strings.TrimSpace(caFile)
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: strings.TrimSpace(serverName),
	}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("load gRPC server CA: %w", err)
		}
		cfg.RootCAs = pool
	}
	if clientCert != nil {
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return clientCert()
		}
	}
	return cfg, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s contains no PEM certificates", path)
	}
	return pool, nil
}
