package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"

	"github.com/terion-name/airpc/internal/headerx"
	"github.com/terion-name/airpc/internal/subject"
)

const (
	defaultRequestTimeout  = 30 * time.Second
	defaultMaxRequestBytes = 4 * 1024 * 1024
	defaultMaxReplyBytes   = 16 * 1024 * 1024
	maxDuration            = 10 * time.Minute
	maxBodyBytes           = 1024 * 1024 * 1024
)

type Config struct {
	NATS      NATSConfig      `yaml:"nats"`
	Connector ConnectorConfig `yaml:"connector"`
	Defaults  Defaults        `yaml:"defaults"`
	Routes    []Route         `yaml:"routes"`
}

type NATSConfig struct {
	URL       string `yaml:"url"`
	Name      string `yaml:"name"`
	CredsFile string `yaml:"creds_file"`
	NKeyFile  string `yaml:"nkey_file"`
}

type ConnectorConfig struct {
	ID string `yaml:"id"`
}

type Defaults struct {
	RequestTimeout   Duration `yaml:"request_timeout"`
	MaxRequestBytes  Size     `yaml:"max_request_bytes"`
	MaxResponseBytes Size     `yaml:"max_response_bytes"`
}

type Route struct {
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

func (c *Config) Validate() error {
	applyDefaults(c)
	if err := c.NATS.validate(); err != nil {
		return err
	}
	if err := validateConnectorID("connector.id", c.Connector.ID); err != nil {
		return err
	}
	if err := validateDuration("defaults.request_timeout", c.Defaults.RequestTimeout.Duration); err != nil {
		return err
	}
	if err := validateSize("defaults.max_request_bytes", c.Defaults.MaxRequestBytes.Bytes); err != nil {
		return err
	}
	if err := validateSize("defaults.max_response_bytes", c.Defaults.MaxResponseBytes.Bytes); err != nil {
		return err
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("routes must contain at least one route")
	}
	seen := make(map[string]struct{}, len(c.Routes))
	for i := range c.Routes {
		if err := c.Routes[i].validate(i, c.Defaults, seen); err != nil {
			return err
		}
	}
	return nil
}

func (n NATSConfig) validate() error {
	if n.URL == "" {
		return fmt.Errorf("nats.url is required")
	}
	u, err := url.Parse(n.URL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("nats.url must be a valid NATS URL")
	}
	switch u.Scheme {
	case "nats", "tls", "ws", "wss":
	default:
		return fmt.Errorf("nats.url scheme %q is not supported", u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("nats.url must not embed credentials")
	}
	return nil
}

func (r *Route) validate(i int, defaults Defaults, seen map[string]struct{}) error {
	prefix := fmt.Sprintf("routes[%d]", i)
	if err := subject.ValidateRouteName(r.Name); err != nil {
		return fmt.Errorf("%s.name: %w", prefix, err)
	}
	if _, ok := seen[r.Name]; ok {
		return fmt.Errorf("%s.name %q is duplicated", prefix, r.Name)
	}
	seen[r.Name] = struct{}{}
	if err := validateTarget(prefix+".target", r.Target); err != nil {
		return err
	}
	if len(r.Connectors) == 0 {
		return fmt.Errorf("%s.connectors must contain at least one connector id", prefix)
	}
	for j, id := range r.Connectors {
		if err := validateConnectorID(fmt.Sprintf("%s.connectors[%d]", prefix, j), id); err != nil {
			return err
		}
	}
	if len(r.AllowHeaders) > 0 {
		if _, err := headerx.NormalizeAllowlist(r.AllowHeaders); err != nil {
			return fmt.Errorf("%s.allow_headers: %w", prefix, err)
		}
	}
	if !r.RequestTimeout.set {
		r.RequestTimeout = defaults.RequestTimeout
	}
	if !r.MaxRequestBytes.set {
		r.MaxRequestBytes = defaults.MaxRequestBytes
	}
	if !r.MaxResponseBytes.set {
		r.MaxResponseBytes = defaults.MaxResponseBytes
	}
	if err := validateDuration(prefix+".request_timeout", r.RequestTimeout.Duration); err != nil {
		return err
	}
	if err := validateSize(prefix+".max_request_bytes", r.MaxRequestBytes.Bytes); err != nil {
		return err
	}
	if err := validateSize(prefix+".max_response_bytes", r.MaxResponseBytes.Bytes); err != nil {
		return err
	}
	return nil
}

func applyDefaults(c *Config) {
	if !c.Defaults.RequestTimeout.set {
		c.Defaults.RequestTimeout = Duration{Duration: defaultRequestTimeout}
	}
	if !c.Defaults.MaxRequestBytes.set {
		c.Defaults.MaxRequestBytes = Size{Bytes: defaultMaxRequestBytes}
	}
	if !c.Defaults.MaxResponseBytes.set {
		c.Defaults.MaxResponseBytes = Size{Bytes: defaultMaxReplyBytes}
	}
}

func validateTarget(field, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", field)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s must be an absolute http or https URL", field)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not embed credentials", field)
	}
	switch u.Scheme {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("%s scheme %q is not supported", field, u.Scheme)
	}
}

func validateConnectorID(field, id string) error {
	if id == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(id) > 64 {
		return fmt.Errorf("%s exceeds 64 bytes", field)
	}
	for i, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case (r == '-' || r == '_') && i > 0 && i < len(id)-1:
		default:
			return fmt.Errorf("%s %q must use letters, digits, and interior '-' or '_'", field, id)
		}
	}
	return nil
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
