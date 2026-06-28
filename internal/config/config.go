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

	"github.com/terion-name/airpc/internal/headerx"
)

const (
	RouteHTTP      = "http"
	RouteTCP       = "tcp"
	RouteWebSocket = "websocket"
	RouteGRPC      = "grpc"

	defaultRequestTimeout  = 30 * time.Second
	defaultMaxRequestBytes = 4 * 1024 * 1024
	defaultMaxReplyBytes   = 16 * 1024 * 1024
	maxDuration            = 10 * time.Minute
	maxBodyBytes           = 1024 * 1024 * 1024
	defaultTunnelPath      = "/_airpc/v1/tunnel"
)

type Config struct {
	NATS      NATSConfig      `yaml:"nats"`
	Edge      EdgeConfig      `yaml:"edge"`
	Connector ConnectorConfig `yaml:"connector"`
	Defaults  Defaults        `yaml:"defaults"`

	// Routes preserves the initial scaffold's top-level HTTP-only shape. New configs
	// should use edge.routes and connector.routes instead.
	Routes []LegacyRoute `yaml:"routes"`
}

type NATSConfig struct {
	URLs      []string `yaml:"urls"`
	URL       string   `yaml:"url"`
	Name      string   `yaml:"name"`
	CredsFile string   `yaml:"creds_file"`
	NKeyFile  string   `yaml:"nkey_file"`
}

type EdgeConfig struct {
	HTTP   EdgeHTTPConfig `yaml:"http"`
	Routes []EdgeRoute    `yaml:"routes"`
}

type EdgeHTTPConfig struct {
	Listen      string `yaml:"listen"`
	TunnelPath  string `yaml:"tunnel_path"`
	TunnelToken string `yaml:"tunnel_token"`
}

type ConnectorConfig struct {
	ID            string           `yaml:"id"`
	EdgeTunnelURL string           `yaml:"edge_tunnel_url"`
	TunnelToken   string           `yaml:"tunnel_token"`
	Routes        []ConnectorRoute `yaml:"routes"`
}

type Defaults struct {
	RequestTimeout   Duration `yaml:"request_timeout"`
	MaxRequestBytes  Size     `yaml:"max_request_bytes"`
	MaxResponseBytes Size     `yaml:"max_response_bytes"`
}

type RouteLimits struct {
	RequestTimeout        Duration `yaml:"request_timeout"`
	MaxRequestBytes       Size     `yaml:"max_request_bytes"`
	MaxResponseBytes      Size     `yaml:"max_response_bytes"`
	RequestHeaderAllow    []string `yaml:"request_headers"`
	ResponseHeaderAllow   []string `yaml:"response_headers"`
	LegacyHeaderAllowlist []string `yaml:"allow_headers"`
}

type EdgeRoute struct {
	RouteLimits `yaml:",inline"`
	Name        string   `yaml:"name"`
	Type        string   `yaml:"type"`
	Hosts       []string `yaml:"hosts"`
	PathPrefix  string   `yaml:"path_prefix"`
	StripPrefix bool     `yaml:"strip_prefix"`
	Listen      string   `yaml:"listen"`
}

type ConnectorRoute struct {
	RouteLimits `yaml:",inline"`
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Target      string `yaml:"target"`
	PathPrefix  string `yaml:"path_prefix"`
	StripPrefix bool   `yaml:"strip_prefix"`
}

type LegacyRoute struct {
	Name             string   `yaml:"name"`
	Target           string   `yaml:"target"`
	Connectors       []string `yaml:"connectors"`
	AllowHeaders     []string `yaml:"allow_headers"`
	RequestTimeout   Duration `yaml:"request_timeout"`
	MaxRequestBytes  Size     `yaml:"max_request_bytes"`
	MaxResponseBytes Size     `yaml:"max_response_bytes"`
}

type Duration struct {
	time.Duration
	set bool
}

type Size struct {
	Bytes int64
	set   bool
}

func LoadFile(path string) (Config, error) {
	cfg, err := DecodeFile(path)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func LoadFileWithConnectorID(path, connectorID string) (Config, error) {
	cfg, err := DecodeFile(path)
	if err != nil {
		return Config{}, err
	}
	if connectorID != "" {
		cfg.Connector.ID = connectorID
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func DecodeFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	return Decode(data)
}

func Load(data []byte) (Config, error) {
	cfg, err := Decode(data)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Decode(data []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	applyDefaults(c)
	applyLegacyRoutes(c)
	if err := c.NATS.validate(); err != nil {
		return err
	}
	if err := c.Defaults.validate(); err != nil {
		return err
	}
	if err := c.Edge.validate(c.Defaults); err != nil {
		return err
	}
	if err := c.Connector.validate(c.Defaults); err != nil {
		return err
	}
	return validateRoutePairing(c.Edge.Routes, c.Connector.Routes)
}

func (c Config) EdgeRoute(name string) (EdgeRoute, bool) {
	for _, r := range c.Edge.Routes {
		if r.Name == name {
			return r, true
		}
	}
	return EdgeRoute{}, false
}

func (c Config) ConnectorRoute(name string) (ConnectorRoute, bool) {
	for _, r := range c.Connector.Routes {
		if r.Name == name {
			return r, true
		}
	}
	return ConnectorRoute{}, false
}

func (c Config) ConnectorHasRoute(name string) bool {
	_, ok := c.ConnectorRoute(name)
	return ok
}

func (r ConnectorRoute) TargetURL() (*url.URL, error) {
	if r.Type != RouteHTTP && r.Type != RouteWebSocket {
		return nil, fmt.Errorf("route %q type %q does not use a URL target", r.Name, r.Type)
	}
	return parseURLTarget("target", r.Target, schemesForType(r.Type))
}

func (r ConnectorRoute) TargetAddress() (string, error) {
	if r.Type != RouteTCP && r.Type != RouteGRPC {
		return "", fmt.Errorf("route %q type %q does not use host:port target", r.Name, r.Type)
	}
	if err := validateHostPort("target", r.Target); err != nil {
		return "", err
	}
	return r.Target, nil
}

func (n *NATSConfig) validate() error {
	if len(n.URLs) == 0 && n.URL != "" {
		n.URLs = []string{n.URL}
	}
	if len(n.URLs) == 0 {
		return fmt.Errorf("nats.urls must contain at least one URL")
	}
	for i, raw := range n.URLs {
		if err := validateNATSURL(fmt.Sprintf("nats.urls[%d]", i), raw); err != nil {
			return err
		}
	}
	return nil
}

func (d Defaults) validate() error {
	if err := validateDuration("defaults.request_timeout", d.RequestTimeout.Duration); err != nil {
		return err
	}
	if err := validateSize("defaults.max_request_bytes", d.MaxRequestBytes.Bytes); err != nil {
		return err
	}
	return validateSize("defaults.max_response_bytes", d.MaxResponseBytes.Bytes)
}

func (e *EdgeConfig) validate(defaults Defaults) error {
	if e.HTTP.TunnelPath == "" {
		e.HTTP.TunnelPath = defaultTunnelPath
	}
	if err := validateListen("edge.http.listen", e.HTTP.Listen); err != nil {
		return err
	}
	if err := validatePathPrefix("edge.http.tunnel_path", e.HTTP.TunnelPath); err != nil {
		return err
	}
	if len(e.Routes) == 0 {
		return fmt.Errorf("edge.routes must contain at least one route")
	}
	seen := make(map[string]string, len(e.Routes))
	for i := range e.Routes {
		if err := e.Routes[i].validate(i, defaults, seen); err != nil {
			return err
		}
	}
	return nil
}

func (c *ConnectorConfig) validate(defaults Defaults) error {
	if err := validateSafeToken("connector.id", c.ID, 64); err != nil {
		return err
	}
	if err := validateTunnelURL("connector.edge_tunnel_url", c.EdgeTunnelURL); err != nil {
		return err
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("connector.routes must contain at least one route")
	}
	seen := make(map[string]string, len(c.Routes))
	for i := range c.Routes {
		if err := c.Routes[i].validate(i, defaults, seen); err != nil {
			return err
		}
	}
	return nil
}

func (r *EdgeRoute) validate(i int, defaults Defaults, seen map[string]string) error {
	prefix := fmt.Sprintf("edge.routes[%d]", i)
	if err := validateRouteCore(prefix, r.Name, r.Type, seen); err != nil {
		return err
	}
	if err := r.RouteLimits.applyAndValidate(prefix, defaults); err != nil {
		return err
	}
	switch r.Type {
	case RouteHTTP, RouteWebSocket:
		if err := validatePathPrefix(prefix+".path_prefix", r.PathPrefix); err != nil {
			return err
		}
		if err := validateHosts(prefix+".hosts", r.Hosts); err != nil {
			return err
		}
	case RouteTCP, RouteGRPC:
		if err := validateListen(prefix+".listen", r.Listen); err != nil {
			return err
		}
	}
	return nil
}

func (r *ConnectorRoute) validate(i int, defaults Defaults, seen map[string]string) error {
	prefix := fmt.Sprintf("connector.routes[%d]", i)
	if err := validateRouteCore(prefix, r.Name, r.Type, seen); err != nil {
		return err
	}
	if err := r.RouteLimits.applyAndValidate(prefix, defaults); err != nil {
		return err
	}
	if r.PathPrefix != "" {
		if err := validatePathPrefix(prefix+".path_prefix", r.PathPrefix); err != nil {
			return err
		}
	}
	switch r.Type {
	case RouteHTTP, RouteWebSocket:
		_, err := parseURLTarget(prefix+".target", r.Target, schemesForType(r.Type))
		return err
	case RouteTCP, RouteGRPC:
		return validateHostPort(prefix+".target", r.Target)
	default:
		return nil
	}
}

func validateRouteCore(prefix, name, typ string, seen map[string]string) error {
	if err := validateSafeToken(prefix+".name", name, 127); err != nil {
		return err
	}
	if _, ok := seen[name]; ok {
		return fmt.Errorf("%s.name %q is duplicated", prefix, name)
	}
	seen[name] = typ
	if !validRouteType(typ) {
		return fmt.Errorf("%s.type %q is not supported", prefix, typ)
	}
	return nil
}

func (l *RouteLimits) applyAndValidate(prefix string, defaults Defaults) error {
	if len(l.LegacyHeaderAllowlist) > 0 {
		if len(l.RequestHeaderAllow) == 0 {
			l.RequestHeaderAllow = append([]string(nil), l.LegacyHeaderAllowlist...)
		}
		if len(l.ResponseHeaderAllow) == 0 {
			l.ResponseHeaderAllow = append([]string(nil), l.LegacyHeaderAllowlist...)
		}
	}
	if !l.RequestTimeout.set {
		l.RequestTimeout = defaults.RequestTimeout
	}
	if !l.MaxRequestBytes.set {
		l.MaxRequestBytes = defaults.MaxRequestBytes
	}
	if !l.MaxResponseBytes.set {
		l.MaxResponseBytes = defaults.MaxResponseBytes
	}
	if err := validateDuration(prefix+".request_timeout", l.RequestTimeout.Duration); err != nil {
		return err
	}
	if err := validateSize(prefix+".max_request_bytes", l.MaxRequestBytes.Bytes); err != nil {
		return err
	}
	if err := validateSize(prefix+".max_response_bytes", l.MaxResponseBytes.Bytes); err != nil {
		return err
	}
	if _, err := headerx.NormalizeAllowlist(l.RequestHeaderAllow); err != nil {
		return fmt.Errorf("%s.request_headers: %w", prefix, err)
	}
	if _, err := headerx.NormalizeAllowlist(l.ResponseHeaderAllow); err != nil {
		return fmt.Errorf("%s.response_headers: %w", prefix, err)
	}
	return nil
}

func validateRoutePairing(edge []EdgeRoute, connector []ConnectorRoute) error {
	edgeTypes := make(map[string]string, len(edge))
	for _, r := range edge {
		edgeTypes[r.Name] = r.Type
	}
	for i, r := range connector {
		typ, ok := edgeTypes[r.Name]
		if !ok {
			return fmt.Errorf("connector.routes[%d].name %q has no matching edge route", i, r.Name)
		}
		if typ != r.Type {
			return fmt.Errorf("connector.routes[%d].type %q does not match edge route type %q", i, r.Type, typ)
		}
	}
	return nil
}

func applyDefaults(c *Config) {
	if !c.Defaults.RequestTimeout.set {
		c.Defaults.RequestTimeout = Duration{Duration: defaultRequestTimeout, set: true}
	}
	if !c.Defaults.MaxRequestBytes.set {
		c.Defaults.MaxRequestBytes = Size{Bytes: defaultMaxRequestBytes, set: true}
	}
	if !c.Defaults.MaxResponseBytes.set {
		c.Defaults.MaxResponseBytes = Size{Bytes: defaultMaxReplyBytes, set: true}
	}
}

func applyLegacyRoutes(c *Config) {
	if len(c.Routes) == 0 || len(c.Edge.Routes) > 0 || len(c.Connector.Routes) > 0 {
		return
	}
	if c.Edge.HTTP.Listen == "" {
		c.Edge.HTTP.Listen = ":8080"
	}
	if c.Connector.EdgeTunnelURL == "" {
		c.Connector.EdgeTunnelURL = "ws://127.0.0.1:8080" + defaultTunnelPath
	}
	for _, r := range c.Routes {
		limits := RouteLimits{
			RequestTimeout:      r.RequestTimeout,
			MaxRequestBytes:     r.MaxRequestBytes,
			MaxResponseBytes:    r.MaxResponseBytes,
			RequestHeaderAllow:  append([]string(nil), r.AllowHeaders...),
			ResponseHeaderAllow: append([]string(nil), r.AllowHeaders...),
		}
		c.Edge.Routes = append(c.Edge.Routes, EdgeRoute{
			RouteLimits: limits,
			Name:        r.Name,
			Type:        RouteHTTP,
			PathPrefix:  "/",
		})
		c.Connector.Routes = append(c.Connector.Routes, ConnectorRoute{
			RouteLimits: limits,
			Name:        r.Name,
			Type:        RouteHTTP,
			Target:      r.Target,
		})
	}
}

func validateNATSURL(field, raw string) error {
	u, err := parseURLTarget(field, raw, map[string]struct{}{"nats": {}, "tls": {}, "ws": {}, "wss": {}})
	if err != nil {
		return err
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("%s must not include a path", field)
	}
	return nil
}

func validateTunnelURL(field, raw string) error {
	u, err := parseURLTarget(field, raw, map[string]struct{}{"ws": {}, "wss": {}})
	if err != nil {
		return err
	}
	if u.Path == "" || u.Path == "/" {
		return fmt.Errorf("%s must include a tunnel path", field)
	}
	return nil
}

func parseURLTarget(field, raw string, schemes map[string]struct{}) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("%s is required", field)
	}
	if hasControlOrSpace(raw) {
		return nil, fmt.Errorf("%s must not contain whitespace or control characters", field)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("%s must be an absolute URL", field)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%s must not embed credentials", field)
	}
	if _, ok := schemes[u.Scheme]; !ok {
		return nil, fmt.Errorf("%s scheme %q is not supported", field, u.Scheme)
	}
	return u, nil
}

func validateHostPort(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", field)
	}
	if hasControlOrSpace(raw) || strings.Contains(raw, "@") || strings.Contains(raw, "://") {
		return fmt.Errorf("%s must be a host:port without credentials or scheme", field)
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid host:port: %w", field, err)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("%s host is required", field)
	}
	return validatePort(field, port)
}

func validateListen(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", field)
	}
	if hasControlOrSpace(raw) || strings.Contains(raw, "://") || strings.Contains(raw, "@") {
		return fmt.Errorf("%s must be a listen host:port without credentials or scheme", field)
	}
	_, port, err := net.SplitHostPort(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid host:port: %w", field, err)
	}
	return validatePort(field, port)
}

func validatePort(field, port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535", field)
	}
	return nil
}

func validateHosts(field string, hosts []string) error {
	for i, host := range hosts {
		if host == "" || hasControlOrSpace(host) || strings.ContainsAny(host, "/*>") {
			return fmt.Errorf("%s[%d] %q is not a valid host matcher", field, i, host)
		}
	}
	return nil
}

func validatePathPrefix(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !strings.HasPrefix(raw, "/") || hasControl(raw) {
		return fmt.Errorf("%s must start with / and contain no control characters", field)
	}
	return nil
}

func validateSafeToken(field, value string, maxLen int) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > maxLen {
		return fmt.Errorf("%s exceeds %d bytes", field, maxLen)
	}
	if strings.ContainsAny(value, "*>") || hasControlOrSpace(value) {
		return fmt.Errorf("%s %q must not contain NATS wildcards, whitespace, or control characters", field, value)
	}
	if strings.HasPrefix(value, ".") || strings.HasSuffix(value, ".") || strings.Contains(value, "..") {
		return fmt.Errorf("%s %q must not contain empty NATS subject tokens", field, value)
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("%s %q must use letters, digits, '.', '-' or '_'", field, value)
		}
	}
	return nil
}

func validRouteType(typ string) bool {
	switch typ {
	case RouteHTTP, RouteTCP, RouteWebSocket, RouteGRPC:
		return true
	default:
		return false
	}
}

func schemesForType(typ string) map[string]struct{} {
	switch typ {
	case RouteHTTP:
		return map[string]struct{}{"http": {}, "https": {}}
	case RouteWebSocket:
		return map[string]struct{}{"ws": {}, "wss": {}}
	default:
		return nil
	}
}

func validateDuration(field string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	if d > maxDuration {
		return fmt.Errorf("%s must be <= %s", field, maxDuration)
	}
	return nil
}

func validateSize(field string, n int64) error {
	if n <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	if n > maxBodyBytes {
		return fmt.Errorf("%s must be <= %d bytes", field, maxBodyBytes)
	}
	return nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == 0 || value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar")
	}
	if value.Tag == "!!int" {
		n, err := strconv.ParseInt(value.Value, 10, 64)
		if err != nil {
			return err
		}
		d.Duration = time.Duration(n)
		d.set = true
		return nil
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value.Value, err)
	}
	d.Duration = parsed
	d.set = true
	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

func (s *Size) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == 0 || value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("size must be a scalar")
	}
	n, err := ParseSize(value.Value)
	if err != nil {
		return err
	}
	s.Bytes = n
	s.set = true
	return nil
}

func (s Size) MarshalYAML() (any, error) {
	return s.Bytes, nil
}

func ParseSize(raw string) (int64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, fmt.Errorf("size is required")
	}
	unitStart := len(v)
	for i, r := range v {
		if !(r >= '0' && r <= '9') {
			unitStart = i
			break
		}
	}
	if unitStart == 0 {
		return 0, fmt.Errorf("size %q must start with digits", raw)
	}
	n, err := strconv.ParseInt(v[:unitStart], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", raw, err)
	}
	unit := strings.ToLower(strings.TrimSpace(v[unitStart:]))
	switch unit {
	case "", "b":
		return n, nil
	case "kb", "k":
		return n * 1000, nil
	case "mb", "m":
		return n * 1000 * 1000, nil
	case "gb", "g":
		return n * 1000 * 1000 * 1000, nil
	case "kib":
		return n * 1024, nil
	case "mib":
		return n * 1024 * 1024, nil
	case "gib":
		return n * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("size unit %q is not supported", unit)
	}
}

func hasControlOrSpace(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func hasControl(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
