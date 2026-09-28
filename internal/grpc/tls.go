package grpcapi

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// ServerTLSConfig builds the master's gRPC TLS configuration. When
// clientCAFile is set, every worker must present a certificate signed by that
// CA (mutual TLS); otherwise only the server is authenticated.
func ServerTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	certFile, keyFile, clientCAFile = strings.TrimSpace(certFile), strings.TrimSpace(keyFile), strings.TrimSpace(clientCAFile)
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
	if clientCAFile != "" {
		pool, err := loadCertPool(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("load gRPC client CA: %w", err)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientTLSConfig builds a worker's gRPC TLS configuration. caFile pins the
// CA that signed the master certificate (system roots when empty); certFile
// and keyFile provide the worker's client certificate for mutual TLS and must
// be set together.
func ClientTLSConfig(caFile, serverName, certFile, keyFile string) (*tls.Config, error) {
	caFile, certFile, keyFile = strings.TrimSpace(caFile), strings.TrimSpace(certFile), strings.TrimSpace(keyFile)
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
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("worker client certificate and key must be set together")
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load worker client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func serverTransportCredentials(cfg Config) (credentials.TransportCredentials, error) {
	tlsCfg, err := ServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
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
