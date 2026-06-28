package logx

import "net/url"

func SafeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	if u.User != nil {
		u.User = url.User("xxxxx")
	}
	return u.String()
}

func SafeHeaderValue(name, value string) string {
	switch name {
	case "Authorization", "Cookie", "Proxy-Authorization", "Set-Cookie", "X-Airpc-Token":
		return "xxxxx"
	default:
		return value
	}
}
