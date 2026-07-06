package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

const (
	ModeHTTP      = "http"
	ModeGRPC      = "grpc"
	ModeWebSocket = "websocket"
	ModeTCP       = "tcp"
)

// Config is the small static configuration shape used by the current scaffold.
type Config struct {
	NATS      NATSConfig      `yaml:"nats"`
	Edge      EdgeConfig      `yaml:"edge"`
	Connector ConnectorConfig `yaml:"connector"`
	Routes    []Route         `yaml:"routes"`
}

type NATSConfig struct {
	URL          string `yaml:"url"`
	EdgeURL      string `yaml:"edge_url"`
	ConnectorURL string `yaml:"connector_url"`
	CredsFile    string `yaml:"creds_file"`
}

type EdgeConfig struct {
	HTTPAddr string `yaml:"http_addr"`
	DataAddr string `yaml:"data_addr"`
	// MetricsAddr serves Prometheus metrics at /metrics; empty disables it.
	// Keep it on a private interface — it is not authenticated.
	MetricsAddr string           `yaml:"metrics_addr"`
	TLS         *TLSServerConfig `yaml:"tls"`
}

type ConnectorConfig struct {
	EdgeDataURL string `yaml:"edge_data_url"`
	TunnelToken string `yaml:"tunnel_token"`
	// MetricsAddr serves Prometheus metrics at /metrics; empty disables it.
	MetricsAddr string           `yaml:"metrics_addr"`
	TLS         *TLSClientConfig `yaml:"tls"`
}

// TLSServerConfig terminates TLS on edge listeners. ClientCAFile, when set,
// additionally requires and verifies client certificates on the data-tunnel
// listener only (mTLS for connectors); public listeners never demand client
// certificates.
type TLSServerConfig struct {
	CertFile     string `yaml:"cert_file"`
	KeyFile      string `yaml:"key_file"`
	ClientCAFile string `yaml:"client_ca_file"`
}

// TLSClientConfig configures the connector's wss data-tunnel dial: a private
// CA to trust and an optional client certificate for mTLS.
type TLSClientConfig struct {
	CAFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	ServerName string `yaml:"server_name"`
}

type Route struct {
	Name                   string   `yaml:"name"`
	Mode                   string   `yaml:"mode"`
	PublicHost             string   `yaml:"public_host"`
	PublicPrefix           string   `yaml:"public_prefix"`
	PublicPath             string   `yaml:"public_path"`
	Listen                 string   `yaml:"listen"`
	Target                 string   `yaml:"target"`
	MaxInlineRequestBytes  Bytes    `yaml:"max_inline_request"`
	MaxInlineResponseBytes Bytes    `yaml:"max_inline_response"`
	MaxRequestBodyBytes    Bytes    `yaml:"max_request_body_bytes"`
	MaxResponseBodyBytes   Bytes    `yaml:"max_response_body_bytes"`
	Timeout                Duration `yaml:"timeout"`
	IdleTimeout            Duration `yaml:"idle_timeout"`
	ForwardedHeaders       []string `yaml:"forwarded_headers"`
	// TLS serves this route's public TCP listener with the edge.tls
	// certificate. Only valid for tcp/grpc routes; http/websocket routes are
	// covered by edge.tls on the shared HTTP listener.
	TLS bool `yaml:"tls"`
}

type Duration struct {
	time.Duration
}

type Bytes int64

func LoadFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	return Load(data)
}

func Load(data []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if err := validateNATSURL("nats.url", c.NATS.URL); err != nil {
		return err
	}
	if c.NATS.EdgeURL != "" {
		if err := validateNATSURL("nats.edge_url", c.NATS.EdgeURL); err != nil {
			return err
		}
	}
	if c.NATS.ConnectorURL != "" {
		if err := validateNATSURL("nats.connector_url", c.NATS.ConnectorURL); err != nil {
			return err
		}
	}
	if err := validateHostPort("edge.http_addr", c.Edge.HTTPAddr); err != nil {
		return err
	}
	if err := validateHostPort("edge.data_addr", c.Edge.DataAddr); err != nil {
		return err
	}
	if err := validateConnectorURL(c.Connector.EdgeDataURL); err != nil {
		return err
	}
	if c.Edge.MetricsAddr != "" {
		if err := validateHostPort("edge.metrics_addr", c.Edge.MetricsAddr); err != nil {
			return err
		}
	}
	if c.Connector.MetricsAddr != "" {
		if err := validateHostPort("connector.metrics_addr", c.Connector.MetricsAddr); err != nil {
			return err
		}
	}
	if c.Edge.TLS != nil && (c.Edge.TLS.CertFile == "" || c.Edge.TLS.KeyFile == "") {
		return fmt.Errorf("edge.tls requires both cert_file and key_file")
	}
	if c.Connector.TLS != nil {
		if (c.Connector.TLS.CertFile == "") != (c.Connector.TLS.KeyFile == "") {
			return fmt.Errorf("connector.tls requires cert_file and key_file together")
		}
		if !strings.HasPrefix(c.Connector.EdgeDataURL, "wss://") {
			return fmt.Errorf("connector.tls requires a wss:// connector.edge_data_url")
		}
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("routes must contain at least one route")
	}

	seen := make(map[string]struct{}, len(c.Routes))
	for i, route := range c.Routes {
		if err := route.validate(i); err != nil {
			return err
		}
		if route.TLS {
			if route.Mode != ModeTCP && route.Mode != ModeGRPC {
				return fmt.Errorf("routes[%d].tls is only supported for tcp and grpc routes", i)
			}
			if c.Edge.TLS == nil {
				return fmt.Errorf("routes[%d].tls requires edge.tls to be configured", i)
			}
		}
		if _, ok := seen[route.Name]; ok {
			return fmt.Errorf("routes[%d].name %q is duplicated", i, route.Name)
		}
		seen[route.Name] = struct{}{}
	}
	return nil
}

func (n NATSConfig) EdgeURLOrDefault() string {
	if n.EdgeURL != "" {
		return n.EdgeURL
	}
	return n.URL
}

func (n NATSConfig) ConnectorURLOrDefault() string {
	if n.ConnectorURL != "" {
		return n.ConnectorURL
	}
	return n.URL
}

func (r Route) UnarySubject() string {
	return "airpc.v1.route." + r.Name + ".unary"
}

func (r Route) OpenSubject() string {
	return "airpc.v1.route." + r.Name + ".open"
}

func (r Route) QueueGroup() string {
	return "airpc.route." + r.Name + ".connectors"
}

const cancelSubjectPrefix = "airpc.v1.cancel."

// CancelSubjectWildcard is subscribed by connectors to observe all cancels.
const CancelSubjectWildcard = cancelSubjectPrefix + ">"

func CancelSubject(requestID string) string {
	return cancelSubjectPrefix + requestID
}

// RequestIDFromCancelSubject extracts and validates the request ID token of a
// cancel subject; ok is false for foreign or malformed subjects.
func RequestIDFromCancelSubject(subject string) (string, bool) {
	requestID := strings.TrimPrefix(subject, cancelSubjectPrefix)
	if requestID == subject || ValidateSubjectToken("request_id", requestID) != nil {
		return "", false
	}
	return requestID, true
}

func (r Route) MaxInlineRequest() Bytes {
	if r.MaxInlineRequestBytes > 0 {
		return r.MaxInlineRequestBytes
	}
	return r.MaxRequestBodyBytes
}

func (r Route) MaxInlineResponse() Bytes {
	if r.MaxInlineResponseBytes > 0 {
		return r.MaxInlineResponseBytes
	}
	return r.MaxResponseBodyBytes
}

func (r Route) validate(i int) error {
	prefix := fmt.Sprintf("routes[%d]", i)
	if err := ValidateSubjectToken(prefix+".name", r.Name); err != nil {
		return err
	}
	if !validMode(r.Mode) {
		return fmt.Errorf("%s.mode %q is not supported", prefix, r.Mode)
	}
	if r.Target == "" {
		return fmt.Errorf("%s.target is required", prefix)
	}
	if err := validateRouteTarget(prefix+".target", r.Mode, r.Target); err != nil {
		return err
	}
	if err := validateRoutePublicFields(prefix, r); err != nil {
		return err
	}
	if int64(r.MaxInlineRequestBytes) < 0 {
		return fmt.Errorf("%s.max_inline_request must not be negative", prefix)
	}
	if int64(r.MaxInlineResponseBytes) < 0 {
		return fmt.Errorf("%s.max_inline_response must not be negative", prefix)
	}
	if int64(r.MaxRequestBodyBytes) < 0 {
		return fmt.Errorf("%s.max_request_body_bytes must not be negative", prefix)
	}
	if int64(r.MaxResponseBodyBytes) < 0 {
		return fmt.Errorf("%s.max_response_body_bytes must not be negative", prefix)
	}
	if r.Timeout.Duration < 0 {
		return fmt.Errorf("%s.timeout must not be negative", prefix)
	}
	if r.IdleTimeout.Duration < 0 {
		return fmt.Errorf("%s.idle_timeout must not be negative", prefix)
	}
	for j, header := range r.ForwardedHeaders {
		if !validHeaderName(header) {
			return fmt.Errorf("%s.forwarded_headers[%d] %q is not a valid header name", prefix, j, header)
		}
	}
	return nil
}

func ValidateSubjectToken(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s is too long", field)
	}
	for _, r := range value {
		if r == '*' || r == '>' || r == '.' {
			return fmt.Errorf("%s must not contain NATS subject separators or wildcards", field)
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("%s contains invalid character %q", field, r)
	}
	return nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar")
	}
	if value.Value == "" {
		return nil
	}
	duration, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value.Value, err)
	}
	d.Duration = duration
	return nil
}

func (b *Bytes) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("bytes must be a scalar")
	}
	if value.Value == "" {
		return nil
	}
	n, err := parseBytes(value.Value)
	if err != nil {
		return err
	}
	*b = Bytes(n)
	return nil
}

func validHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r > unicode.MaxASCII || !isTokenChar(byte(r)) {
			return false
		}
	}
	return true
}

func isTokenChar(c byte) bool {
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

func validMode(mode string) bool {
	switch mode {
	case ModeHTTP, ModeGRPC, ModeWebSocket, ModeTCP:
		return true
	default:
		return false
	}
}

func validateNATSURL(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", field)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s must be a valid URL", field)
	}
	switch u.Scheme {
	case "nats", "tls", "ws", "wss":
		return nil
	default:
		return fmt.Errorf("%s scheme %q is not supported", field, u.Scheme)
	}
}

func validateConnectorURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("connector.edge_data_url is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("connector.edge_data_url must be a valid URL")
	}
	if u.User != nil {
		return fmt.Errorf("connector.edge_data_url must not contain credentials")
	}
	switch u.Scheme {
	case "ws", "wss":
		return nil
	default:
		return fmt.Errorf("connector.edge_data_url scheme %q is not supported", u.Scheme)
	}
}

func validateRouteTarget(field, mode, raw string) error {
	switch mode {
	case ModeHTTP:
		return validateURLTarget(field, raw, "http", "https")
	case ModeWebSocket:
		return validateURLTarget(field, raw, "ws", "wss")
	case ModeTCP, ModeGRPC:
		return validateHostPort(field, raw)
	default:
		return nil
	}
}

func validateURLTarget(field, raw string, schemes ...string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s must be a valid URL", field)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not contain credentials", field)
	}
	for _, scheme := range schemes {
		if u.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("%s scheme %q is not supported", field, u.Scheme)
}

func validateRoutePublicFields(prefix string, r Route) error {
	switch r.Mode {
	case ModeHTTP, ModeWebSocket:
		if r.PublicHost != "" && strings.ContainsAny(r.PublicHost, "/?#") {
			return fmt.Errorf("%s.public_host must be a host name without scheme, path, query, or fragment", prefix)
		}
		if r.PublicPrefix == "" && r.PublicPath == "" {
			return fmt.Errorf("%s.public_prefix or %s.public_path is required for %s routes", prefix, prefix, r.Mode)
		}
		if err := validatePath(prefix+".public_prefix", r.PublicPrefix); err != nil {
			return err
		}
		return validatePath(prefix+".public_path", r.PublicPath)
	case ModeTCP, ModeGRPC:
		return validateHostPort(prefix+".listen", r.Listen)
	default:
		return nil
	}
}

func validatePath(field, value string) error {
	if value == "" {
		return nil
	}
	if !strings.HasPrefix(value, "/") {
		return fmt.Errorf("%s must start with /", field)
	}
	if strings.ContainsAny(value, "?#") {
		return fmt.Errorf("%s must be a path without query or fragment", field)
	}
	return nil
}

func validateHostPort(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be host:port", field)
	}
	if host == "" && !strings.HasPrefix(value, ":") {
		return fmt.Errorf("%s host is required", field)
	}
	if port == "" {
		return fmt.Errorf("%s port is required", field)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("%s port must be a number between 0 and 65535", field)
	}
	return nil
}

func parseBytes(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	for i, r := range s {
		if unicode.IsDigit(r) {
			continue
		}
		if i == 0 {
			return 0, fmt.Errorf("parse bytes %q: missing number", raw)
		}
		n, err := strconv.ParseInt(s[:i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse bytes %q: %w", raw, err)
		}
		multiplier, ok := byteUnits[strings.ToLower(s[i:])]
		if !ok {
			return 0, fmt.Errorf("parse bytes %q: unknown unit %q", raw, s[i:])
		}
		return n * multiplier, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

var byteUnits = map[string]int64{
	"b":   1,
	"kb":  1000,
	"mb":  1000 * 1000,
	"gb":  1000 * 1000 * 1000,
	"kib": 1024,
	"mib": 1024 * 1024,
	"gib": 1024 * 1024 * 1024,
}
