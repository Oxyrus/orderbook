package auth

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// ClientCredentials configures outbound service-to-service calls using the
// OAuth 2.0 client credentials grant (RFC 6749 §4.4).
type ClientCredentials struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
	// EndpointParams adds extra token request parameters, e.g. {"audience": {"other-api"}}.
	EndpointParams map[string][]string
	Timeout        time.Duration
}

// NewClient returns an *http.Client that obtains, caches and refreshes access
// tokens automatically and attaches them to every request. Outgoing requests
// are traced with OpenTelemetry so traces continue across services.
//
//	client := auth.NewClient(ctx, auth.ClientCredentials{...})
//	resp, err := client.Get("https://other-service/v1/things")
func NewClient(ctx context.Context, cc ClientCredentials) *http.Client {
	cfg := clientcredentials.Config{
		ClientID:       cc.ClientID,
		ClientSecret:   cc.ClientSecret,
		TokenURL:       cc.TokenURL,
		Scopes:         cc.Scopes,
		EndpointParams: cc.EndpointParams,
		AuthStyle:      oauth2.AuthStyleAutoDetect,
	}
	timeout := cc.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	base := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: timeout}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, base)
	return &http.Client{
		Transport: &oauth2.Transport{
			Source: oauth2.ReuseTokenSource(nil, cfg.TokenSource(ctx)),
			Base:   base.Transport,
		},
		Timeout: timeout,
	}
}
