package integration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/terion-name/airpc/internal/config"
	"github.com/terion-name/airpc/internal/edge"
)

// TestTLSTerminationAndTunnelMTLS covers the full TLS surface in one stack:
// HTTPS unary on the public listener, a TLS-wrapped public TCP route, and a
// wss data tunnel that requires a connector client certificate.
func TestTLSTerminationAndTunnelMTLS(t *testing.T) {
	natsURL := startNATS(t)
	certs := writeTestCerts(t)

	httpBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("secure-ok"))
	}))
	defer httpBackend.Close()
	tcpBackend := startTCPRawEcho(t)

	cfg := config.Config{
		NATS: config.NATSConfig{URL: natsURL},
		Edge: config.EdgeConfig{
			HTTPAddr: "127.0.0.1:0",
			DataAddr: "127.0.0.1:0",
			TLS: &config.TLSServerConfig{
				CertFile:     certs.serverCert,
				KeyFile:      certs.serverKey,
				ClientCAFile: certs.caCert,
			},
		},
		Connector: config.ConnectorConfig{
			EdgeDataURL: "wss://127.0.0.1:1/_airpc/data",
			TLS: &config.TLSClientConfig{
				CAFile:   certs.caCert,
				CertFile: certs.clientCert,
				KeyFile:  certs.clientKey,
			},
		},
		Routes: []config.Route{
			{
				Name:         "secure",
				Mode:         config.ModeHTTP,
				PublicPrefix: "/secure",
				Target:       httpBackend.URL,
				Timeout:      config.Duration{Duration: 5 * time.Second},
			},
			{
				Name:    "secure_tcp",
				Mode:    config.ModeTCP,
				Listen:  "127.0.0.1:0",
				Target:  tcpBackend,
				TLS:     true,
				Timeout: config.Duration{Duration: 5 * time.Second},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config.Validate(): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	cfg.Connector.EdgeDataURL = "wss://" + edgeServer.DataAddr() + "/_airpc/data"
	startConnectorOrFatal(t, ctx, cfg, "test-tls")

	caPool := x509.NewCertPool()
	caPEM, err := os.ReadFile(certs.caCert)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	caPool.AppendCertsFromPEM(caPEM)

	// HTTPS unary through the TLS public listener.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: caPool}}, Timeout: 10 * time.Second}
	resp, err := client.Get("https://" + edgeServer.Addr() + "/secure/")
	if err != nil {
		t.Fatalf("https request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(body) != "secure-ok" {
		t.Fatalf("https body = %q, err = %v", body, err)
	}

	// TLS-wrapped public TCP route relayed over the mTLS tunnel.
	tcpConn, err := tls.Dial("tcp", edgeServer.TCPAddrs()[0], &tls.Config{RootCAs: caPool, ServerName: "127.0.0.1"})
	if err != nil {
		t.Fatalf("tls dial tcp route: %v", err)
	}
	defer tcpConn.Close()
	if _, err := tcpConn.Write([]byte("secure-echo")); err != nil {
		t.Fatalf("write tls tcp: %v", err)
	}
	if err := tcpConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	got := make([]byte, len("secure-echo"))
	if _, err := io.ReadFull(tcpConn, got); err != nil {
		t.Fatalf("read tls tcp echo: %v", err)
	}
	if string(got) != "secure-echo" {
		t.Fatalf("echo = %q", got)
	}
}

// TestTunnelMTLSRejectsMissingClientCert proves the data listener requires a
// client certificate when client_ca_file is set.
func TestTunnelMTLSRejectsMissingClientCert(t *testing.T) {
	natsURL := startNATS(t)
	certs := writeTestCerts(t)
	tcpBackend := startTCPRawEcho(t)

	cfg := config.Config{
		NATS: config.NATSConfig{URL: natsURL},
		Edge: config.EdgeConfig{
			HTTPAddr: "127.0.0.1:0",
			DataAddr: "127.0.0.1:0",
			TLS: &config.TLSServerConfig{
				CertFile:     certs.serverCert,
				KeyFile:      certs.serverKey,
				ClientCAFile: certs.caCert,
			},
		},
		Connector: config.ConnectorConfig{
			EdgeDataURL: "wss://127.0.0.1:1/_airpc/data",
			// CA only, no client certificate.
			TLS: &config.TLSClientConfig{CAFile: certs.caCert},
		},
		Routes: []config.Route{{
			Name:    "mtls_tcp",
			Mode:    config.ModeTCP,
			Listen:  "127.0.0.1:0",
			Target:  tcpBackend,
			Timeout: config.Duration{Duration: 2 * time.Second},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	edgeServer, err := edge.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("edge.Start(): %v", err)
	}
	cfg.Connector.EdgeDataURL = "wss://" + edgeServer.DataAddr() + "/_airpc/data"
	startConnectorOrFatal(t, ctx, cfg, "test-nomtls")

	// The tunnel can never come up, so the route must fail.
	if err := tryEcho(edgeServer.TCPAddrs()[0]); err == nil {
		t.Fatalf("echo succeeded without connector client certificate")
	}
}

type testCerts struct {
	caCert     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

// writeTestCerts generates a throwaway CA plus a 127.0.0.1 server cert and a
// connector client cert signed by it, written as PEM files under t.TempDir.
func writeTestCerts(t *testing.T) testCerts {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "airpc-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCertParsed, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	issue := func(name string, template *x509.Certificate) (certFile, keyFile string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCertParsed, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s cert: %v", name, err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("marshal %s key: %v", name, err)
		}
		certFile = writePEM(t, filepath.Join(dir, name+".crt"), "CERTIFICATE", der)
		keyFile = writePEM(t, filepath.Join(dir, name+".key"), "EC PRIVATE KEY", keyDER)
		return certFile, keyFile
	}

	serverCert, serverKey := issue("server", &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	})
	clientCert, clientKey := issue("client", &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "airpc-test-connector"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})

	return testCerts{
		caCert:     writePEM(t, filepath.Join(dir, "ca.crt"), "CERTIFICATE", caDER),
		serverCert: serverCert,
		serverKey:  serverKey,
		clientCert: clientCert,
		clientKey:  clientKey,
	}
}

func writePEM(t *testing.T, path, blockType string, der []byte) string {
	t.Helper()
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
