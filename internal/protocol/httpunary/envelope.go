package httpunary

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const Version = 1

// Request is the MessagePack envelope sent from edge to connector for one HTTP request.
type Request struct {
	Version        int         `msgpack:"version"`
	RequestID      string      `msgpack:"request_id"`
	Route          string      `msgpack:"route"`
	DeadlineUnixMS int64       `msgpack:"deadline_unix_ms"`
	Method         string      `msgpack:"method"`
	Scheme         string      `msgpack:"scheme"`
	Authority      string      `msgpack:"authority"`
	Path           string      `msgpack:"path"`
	Headers        http.Header `msgpack:"headers"`
	Body           []byte      `msgpack:"body"`
}

// Response is the MessagePack envelope sent from connector to edge.
type Response struct {
	Version   int         `msgpack:"version"`
	RequestID string      `msgpack:"request_id"`
	Status    int         `msgpack:"status"`
	Headers   http.Header `msgpack:"headers"`
	Body      []byte      `msgpack:"body"`
	Error     *Error      `msgpack:"error,omitempty"`
}

type Error struct {
	Code    string `msgpack:"code"`
	Message string `msgpack:"message"`
}

func EncodeRequest(req Request) ([]byte, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return msgpack.Marshal(req)
}

func DecodeRequest(data []byte) (Request, error) {
	var req Request
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return Request{}, fmt.Errorf("decode HTTP unary request: %w", err)
	}
	if err := req.Validate(); err != nil {
		return Request{}, err
	}
	return req, nil
}

func EncodeResponse(resp Response) ([]byte, error) {
	if err := resp.Validate(); err != nil {
		return nil, err
	}
	return msgpack.Marshal(resp)
}

func DecodeResponse(data []byte) (Response, error) {
	var resp Response
	if err := msgpack.Unmarshal(data, &resp); err != nil {
		return Response{}, fmt.Errorf("decode HTTP unary response: %w", err)
	}
	if err := resp.Validate(); err != nil {
		return Response{}, err
	}
	return resp, nil
}

func (r Request) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("HTTP unary request version %d is not supported", r.Version)
	}
	if r.RequestID == "" {
		return fmt.Errorf("HTTP unary request_id is required")
	}
	if err := validateRoute(r.Route); err != nil {
		return err
	}
	if r.DeadlineUnixMS <= 0 {
		return fmt.Errorf("HTTP unary deadline_unix_ms is required")
	}
	if time.UnixMilli(r.DeadlineUnixMS).Before(time.Unix(0, 0)) {
		return fmt.Errorf("HTTP unary deadline_unix_ms is invalid")
	}
	if r.Method == "" || !validToken(r.Method) {
		return fmt.Errorf("HTTP unary method %q is invalid", r.Method)
	}
	switch r.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("HTTP unary scheme %q is invalid", r.Scheme)
	}
	if r.Authority == "" || strings.ContainsAny(r.Authority, "\r\n") {
		return fmt.Errorf("HTTP unary authority is invalid")
	}
	if err := validatePath(r.Path); err != nil {
		return err
	}
	return validateHeaders(r.Headers)
}

func (r Response) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("HTTP unary response version %d is not supported", r.Version)
	}
	if r.RequestID == "" {
		return fmt.Errorf("HTTP unary response request_id is required")
	}
	if r.Error != nil {
		if err := r.Error.Validate(); err != nil {
			return err
		}
		return nil
	}
	if r.Status < 100 || r.Status > 999 {
		return fmt.Errorf("HTTP unary response status %d is invalid", r.Status)
	}
	return validateHeaders(r.Headers)
}

func (e Error) Validate() error {
	if e.Code == "" || !validToken(e.Code) {
		return fmt.Errorf("HTTP unary error code %q is invalid", e.Code)
	}
	if strings.ContainsAny(e.Message, "\r\n") {
		return fmt.Errorf("HTTP unary error message is invalid")
	}
	return nil
}

func NewErrorResponse(requestID, code, message string) Response {
	return Response{
		Version:   Version,
		RequestID: requestID,
		Error:     &Error{Code: code, Message: message},
	}
}

func validateRoute(route string) error {
	if route == "" {
		return fmt.Errorf("HTTP unary route is required")
	}
	for _, r := range route {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("HTTP unary route %q is invalid", route)
	}
	return nil
}

func validatePath(path string) error {
	if path == "" || !strings.HasPrefix(path, "/") {
		return fmt.Errorf("HTTP unary path must start with /")
	}
	if strings.ContainsAny(path, "\r\n#") {
		return fmt.Errorf("HTTP unary path is invalid")
	}
	return nil
}

func validateHeaders(headers http.Header) error {
	for name, values := range headers {
		if name == "" || !validToken(name) {
			return fmt.Errorf("HTTP unary header name %q is invalid", name)
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n") {
				return fmt.Errorf("HTTP unary header %q contains an invalid value", name)
			}
		}
	}
	return nil
}

func validToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		default:
			return false
		}
	}
	return s != ""
}
