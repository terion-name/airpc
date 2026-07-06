package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerTLS builds the tls.Config for public edge listeners (no client
// certificate requirement).
func (t *TLSServerConfig) ServerTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load edge TLS key pair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// DataTLS builds the tls.Config for the data-tunnel listener; when
// client_ca_file is set it requires and verifies connector client
// certificates (mTLS).
func (t *TLSServerConfig) DataTLS() (*tls.Config, error) {
	cfg, err := t.ServerTLS()
	if err != nil {
		return nil, err
	}
	if t.ClientCAFile == "" {
		return cfg, nil
	}
	pool, err := loadCertPool(t.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("load edge TLS client CA: %w", err)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return cfg, nil
}

// ClientTLS builds the tls.Config for the connector's wss data-tunnel dial.
func (t *TLSClientConfig) ClientTLS() (*tls.Config, error) {
	cfg := &tls.Config{ServerName: t.ServerName, MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		pool, err := loadCertPool(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load connector TLS CA: %w", err)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load connector TLS key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
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
