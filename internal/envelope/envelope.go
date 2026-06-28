package envelope

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strings"
	"unicode"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/terion-name/airpc/internal/ids"
	"github.com/terion-name/airpc/internal/subject"
)

const (
	Version              = 1
	DefaultMaxDecodeSize = 32 * 1024 * 1024
)

var (
	ErrTooLarge = errors.New("envelope too large")
	ErrInvalid  = errors.New("invalid envelope")
)

type UnaryRequest struct {
	RequestID string
	Route     string
	Method    string
	Path      string
	Headers   http.Header
	Body      []byte
}

type UnaryResponse struct {
	RequestID string
	Status    int
	Headers   http.Header
	Body      []byte
	Error     string
}

type OpenRequest struct {
	RequestID string
	SessionID string
	Route     string
	Headers   http.Header
}

type OpenResponse struct {
	RequestID string
	SessionID string
	Accepted  bool
	Code      string
	Message   string
	Headers   http.Header
}

func MarshalUnaryRequest(req UnaryRequest) ([]byte, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayLen(7); err != nil {
		return nil, err
	}
	if err := enc.EncodeUint8(Version); err != nil {
		return nil, err
	}
	for _, s := range []string{req.RequestID, req.Route, req.Method, req.Path} {
		if err := enc.EncodeString(s); err != nil {
			return nil, err
		}
	}
	if err := encodeHeader(enc, req.Headers); err != nil {
		return nil, err
	}
	if err := enc.EncodeBytes(req.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalUnaryRequest(data []byte, maxSize int) (UnaryRequest, error) {
	var req UnaryRequest
	if err := decode(data, maxSize, 7, func(dec *msgpack.Decoder) error {
		if err := decodeVersion(dec); err != nil {
			return err
		}
		var err error
		if req.RequestID, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Route, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Method, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Path, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Headers, err = decodeHeader(dec); err != nil {
			return err
		}
		if req.Body, err = dec.DecodeBytes(); err != nil {
			return err
		}
		return req.validate()
	}); err != nil {
		return UnaryRequest{}, err
	}
	return req, nil
}

func MarshalUnaryResponse(resp UnaryResponse) ([]byte, error) {
	if err := resp.validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayLen(6); err != nil {
		return nil, err
	}
	if err := enc.EncodeUint8(Version); err != nil {
		return nil, err
	}
	if err := enc.EncodeString(resp.RequestID); err != nil {
		return nil, err
	}
	if err := enc.EncodeInt(int64(resp.Status)); err != nil {
		return nil, err
	}
	if err := encodeHeader(enc, resp.Headers); err != nil {
		return nil, err
	}
	if err := enc.EncodeBytes(resp.Body); err != nil {
		return nil, err
	}
	if err := enc.EncodeString(resp.Error); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalUnaryResponse(data []byte, maxSize int) (UnaryResponse, error) {
	var resp UnaryResponse
	if err := decode(data, maxSize, 6, func(dec *msgpack.Decoder) error {
		if err := decodeVersion(dec); err != nil {
			return err
		}
		var err error
		if resp.RequestID, err = dec.DecodeString(); err != nil {
			return err
		}
		status, err := dec.DecodeInt()
		if err != nil {
			return err
		}
		resp.Status = int(status)
		if resp.Headers, err = decodeHeader(dec); err != nil {
			return err
		}
		if resp.Body, err = dec.DecodeBytes(); err != nil {
			return err
		}
		if resp.Error, err = dec.DecodeString(); err != nil {
			return err
		}
		return resp.validate()
	}); err != nil {
		return UnaryResponse{}, err
	}
	return resp, nil
}

func MarshalOpenRequest(req OpenRequest) ([]byte, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayLen(5); err != nil {
		return nil, err
	}
	if err := enc.EncodeUint8(Version); err != nil {
		return nil, err
	}
	for _, s := range []string{req.RequestID, req.SessionID, req.Route} {
		if err := enc.EncodeString(s); err != nil {
			return nil, err
		}
	}
	if err := encodeHeader(enc, req.Headers); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalOpenRequest(data []byte, maxSize int) (OpenRequest, error) {
	var req OpenRequest
	if err := decode(data, maxSize, 5, func(dec *msgpack.Decoder) error {
		if err := decodeVersion(dec); err != nil {
			return err
		}
		var err error
		if req.RequestID, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.SessionID, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Route, err = dec.DecodeString(); err != nil {
			return err
		}
		if req.Headers, err = decodeHeader(dec); err != nil {
			return err
		}
		return req.validate()
	}); err != nil {
		return OpenRequest{}, err
	}
	return req, nil
}

func MarshalOpenResponse(resp OpenResponse) ([]byte, error) {
	if err := resp.validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeArrayLen(7); err != nil {
		return nil, err
	}
	if err := enc.EncodeUint8(Version); err != nil {
		return nil, err
	}
	for _, s := range []string{resp.RequestID, resp.SessionID} {
		if err := enc.EncodeString(s); err != nil {
			return nil, err
		}
	}
	if err := enc.EncodeBool(resp.Accepted); err != nil {
		return nil, err
	}
	for _, s := range []string{resp.Code, resp.Message} {
		if err := enc.EncodeString(s); err != nil {
			return nil, err
		}
	}
	if err := encodeHeader(enc, resp.Headers); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalOpenResponse(data []byte, maxSize int) (OpenResponse, error) {
	var resp OpenResponse
	if err := decode(data, maxSize, 7, func(dec *msgpack.Decoder) error {
		if err := decodeVersion(dec); err != nil {
			return err
		}
		var err error
		if resp.RequestID, err = dec.DecodeString(); err != nil {
			return err
		}
		if resp.SessionID, err = dec.DecodeString(); err != nil {
			return err
		}
		if resp.Accepted, err = dec.DecodeBool(); err != nil {
			return err
		}
		if resp.Code, err = dec.DecodeString(); err != nil {
			return err
		}
		if resp.Message, err = dec.DecodeString(); err != nil {
			return err
		}
		if resp.Headers, err = decodeHeader(dec); err != nil {
			return err
		}
		return resp.validate()
	}); err != nil {
		return OpenResponse{}, err
	}
	return resp, nil
}

func decode(data []byte, maxSize, wantLen int, fill func(*msgpack.Decoder) error) error {
	if maxSize <= 0 {
		maxSize = DefaultMaxDecodeSize
	}
	if len(data) > maxSize {
		return fmt.Errorf("%w: %d > %d", ErrTooLarge, len(data), maxSize)
	}
	r := bytes.NewReader(data)
	dec := msgpack.NewDecoder(r)
	gotLen, err := dec.DecodeArrayLen()
	if err != nil {
		return fmt.Errorf("%w: decode array: %v", ErrInvalid, err)
	}
	if gotLen != wantLen {
		return fmt.Errorf("%w: expected array length %d, got %d", ErrInvalid, wantLen, gotLen)
	}
	if err := fill(dec); err != nil {
		if errors.Is(err, ErrInvalid) || errors.Is(err, ErrTooLarge) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if r.Len() != 0 {
		return fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	return nil
}

func decodeVersion(dec *msgpack.Decoder) error {
	version, err := dec.DecodeUint8()
	if err != nil {
		return err
	}
	if version != Version {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalid, version)
	}
	return nil
}

func encodeHeader(enc *msgpack.Encoder, h http.Header) error {
	if h == nil {
		return enc.EncodeMapLen(0)
	}
	if err := enc.EncodeMapLen(len(h)); err != nil {
		return err
	}
	for key, values := range h {
		canonical := textproto.CanonicalMIMEHeaderKey(key)
		if err := validateHeaderName(canonical); err != nil {
			return err
		}
		if err := enc.EncodeString(canonical); err != nil {
			return err
		}
		if err := enc.EncodeArrayLen(len(values)); err != nil {
			return err
		}
		for _, value := range values {
			if err := enc.EncodeString(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeHeader(dec *msgpack.Decoder) (http.Header, error) {
	length, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	h := make(http.Header, length)
	for i := 0; i < length; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		canonical := textproto.CanonicalMIMEHeaderKey(key)
		if err := validateHeaderName(canonical); err != nil {
			return nil, err
		}
		valueLen, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		for j := 0; j < valueLen; j++ {
			value, err := dec.DecodeString()
			if err != nil {
				return nil, err
			}
			h.Add(canonical, value)
		}
	}
	return h, nil
}

func (r UnaryRequest) validate() error {
	if err := ids.Validate("request id", r.RequestID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := subject.ValidateRouteName(r.Route); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := validateMethod(r.Method); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if r.Path == "" || hasControl(r.Path) || !strings.HasPrefix(r.Path, "/") {
		return fmt.Errorf("%w: path must start with / and contain no control characters", ErrInvalid)
	}
	return validateHeaders(r.Headers)
}

func (r UnaryResponse) validate() error {
	if err := ids.Validate("request id", r.RequestID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if r.Status < 100 || r.Status > 599 {
		return fmt.Errorf("%w: invalid status %d", ErrInvalid, r.Status)
	}
	return validateHeaders(r.Headers)
}

func (r OpenRequest) validate() error {
	if err := ids.Validate("request id", r.RequestID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := ids.Validate("session id", r.SessionID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := subject.ValidateRouteName(r.Route); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return validateHeaders(r.Headers)
}

func (r OpenResponse) validate() error {
	if err := ids.Validate("request id", r.RequestID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := ids.Validate("session id", r.SessionID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !r.Accepted && r.Code == "" {
		return fmt.Errorf("%w: rejected open response requires a code", ErrInvalid)
	}
	return validateHeaders(r.Headers)
}

func validateHeaders(h http.Header) error {
	for key := range h {
		if err := validateHeaderName(key); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	return nil
}

func validateHeaderName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("header name is required")
	}
	for _, r := range name {
		if r > unicode.MaxASCII || unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("invalid header name %q", name)
		}
	}
	return nil
}

func validateMethod(method string) error {
	if method == "" {
		return fmt.Errorf("method is required")
	}
	for _, r := range method {
		if r < 'A' || r > 'Z' {
			return fmt.Errorf("method %q must be uppercase ASCII", method)
		}
	}
	return nil
}

func hasControl(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
