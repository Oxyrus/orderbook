# Authentication and authorization (OAuth 2.0)

## Role of this service

In OAuth 2.0 terms this API is a **resource server**. It does not log users
in, store passwords or issue tokens. A dedicated **authorization server**
(Keycloak, Auth0, Okta, Entra ID, Cognito, Zitadel, Ory Hydra, …) does that.
This service only **validates the access tokens** clients present:

```
┌────────┐ 1. get token (auth code + PKCE, client credentials, …) ┌──────────────────┐
│ client │──────────────────────────────────────────────────────▶│ authorization    │
│        │◀──────────────── access token (JWT) ──────────────────│ server (IdP)     │
│        │                                                       └────────┬─────────┘
│        │ 2. Authorization: Bearer <JWT>  ┌──────────────┐               │ JWKS (public keys,
│        │────────────────────────────────▶│  this API    │◀──────────────┘ fetched + cached)
└────────┘                                 └──────────────┘
```

This is the standard design for APIs. Keeping identity out of every
service is safer, and any client type works: SPAs, mobile apps, CLIs and
other services.

## What is validated

`internal/platform/auth` uses [`coreos/go-oidc`](https://github.com/coreos/go-oidc)
to check, for every request:

| Check | Source |
|---|---|
| Signature | Key from the issuer's JWKS matching the token's `kid`. The JWKS is fetched on the first token and cached. It is refetched when a token names an unknown `kid` (so key rotation needs no restart) and after one hour (so removed keys stop being trusted), but **at most once every 30s**. A token with a known `kid` and a bad signature is rejected without contacting the issuer, so forged tokens can't be used to flood it. |
| Algorithm | Must be in `AUTH_SUPPORTED_ALGS` (default `RS256,ES256`). `none`/HMAC are never accepted. |
| `iss` | Must equal `AUTH_ISSUER` exactly. |
| `aud` | Must contain `AUTH_AUDIENCE`. |
| `exp` | Must be in the future. |
| `sub` | Must be present. It becomes the `Principal.Subject`. |
| `typ` header | Only with `AUTH_REQUIRE_AT_JWT=true`: must be `at+jwt` (RFC 9068). Without it, an ID token whose `aud` happens to equal `AUTH_AUDIENCE` would be accepted. Most providers use a different audience for ID tokens (the client ID), but enable this if yours issues RFC 9068 access tokens. |

When validation succeeds, an `auth.Principal` is stored in the request context:

```go
p, ok := auth.PrincipalFrom(r.Context())
p.Subject   // "sub": user ID (or client ID for machine tokens)
p.ClientID  // "client_id" or "azp"
p.Scopes    // parsed from "scope", "scp" or "permissions"
p.Claims    // every raw claim, for custom needs (tenant, roles, email…)
```

The request logger is also enriched with `sub` and `client_id`, so every log
line of the request records who made it.

## Responses

| Situation | Status | `WWW-Authenticate` (RFC 6750) |
|---|---|---|
| No / non-Bearer `Authorization` header | 401 | `Bearer realm="api"` |
| Invalid, expired, wrong issuer/audience/signature | 401 | `Bearer realm="api", error="invalid_token"` |
| Valid token missing a required scope | 403 | `Bearer realm="api", error="insufficient_scope", scope="orders:write"` |

Why validation failed (e.g. "token expired") is logged at debug level but
never returned to the client.

## Scopes

Scopes are enforced per route, right where the route is declared:

```go
r.With(auth.RequireScopes(ScopeRead)).Get("/", httpx.Handle(h.list))
r.With(auth.RequireScopes(ScopeWrite)).Post("/", httpx.Handle(h.create))
```

`RequireScopes("a", "b")` requires **all** listed scopes. Scopes are read
from any of these claim formats, so every major IdP works without code
changes:

| Claim | Format | Used by |
|---|---|---|
| `scope` | space-separated string | RFC 9068, Keycloak, Auth0, Zitadel, Hydra |
| `scp` | array or string | Okta, Entra ID |
| `permissions` | array | Auth0 RBAC |

Name scopes `resource:action` (`orders:read`, `orders:write`,
`platform:read`). Define them as constants in the feature package, as
`order.ScopeRead` does.

For checks finer than a scope (e.g. "only the owner may edit"), do them in
the service using `Principal.Subject` or custom claims.

## The tenant claim

Every tenant-scoped route needs to know which tenant the caller acts for.
`tenant.Middleware` reads it from the claim named by `TENANT_CLAIM`
(default `tenant_id`) of the **verified** token. A valid token without a
tenant gets `403 tenant_required`, and a malformed value gets `403
tenant_invalid` (ids must match `^[a-z0-9][a-z0-9_-]{0,62}$`). The tenant id
is also the database shard key ([sharding.md](sharding.md)), which is why it
must never come from a header, path or body the caller controls.

Adding the claim at common providers:

| Provider | How |
|---|---|
| Keycloak | User attribute or organisation + a "User Attribute" / "Hardcoded claim" protocol mapper on the client scope |
| Auth0 | Organizations, plus a post-login Action that sets `api.accessToken.setCustomClaim("tenant_id", event.organization.name)` |
| Okta | Custom authorization server, claim `tenant_id` from a user profile attribute or group |
| Entra ID | Multi-tenant apps already have `tid`; set `TENANT_CLAIM=tid` (map GUIDs to your tenant ids if needed), or use a claims-mapping policy |
| Cognito | A pre-token-generation Lambda that adds `tenant_id` |
| Zitadel | Organisations: map `urn:zitadel:iam:user:resourceowner:id` (`TENANT_CLAIM` with that name) |

Machine-to-machine clients of one tenant get the claim through
client-specific mappers. Operator tools get **no tenant** and the
`platform:read` scope, which only `/v1/platform/*` accepts.

Locally, `deploy/mock-oauth2/config.json` sets `tenant_id` to the client
id: `make token client=acme`.

## Configuring your identity provider

Two settings are required: the **issuer** (exactly as it appears in the
token's `iss`) and the **audience** your API is registered with.

| Provider | `AUTH_ISSUER` | `AUTH_AUDIENCE` / notes |
|---|---|---|
| **Keycloak** | `https://kc.example.com/realms/<realm>` | Add an *audience mapper* to the client scope so tokens carry your API's ID in `aud`. |
| **Auth0** | `https://<tenant>.auth0.com/` (keep the trailing slash) | The API *Identifier* (e.g. `https://api.example.com`). Turn on RBAC + "Add Permissions in the Access Token" to get `permissions`. |
| **Okta** | `https://<org>.okta.com/oauth2/<authServerId>` (e.g. `default`) | The authorization server's *Audience* (default `api://default`). Scopes arrive in `scp`. |
| **Microsoft Entra ID** | `https://login.microsoftonline.com/<tenant-id>/v2.0` | `api://<app-id>` or the app's client ID. Set `accessTokenAcceptedVersion: 2` in the manifest so `iss` matches v2.0. Scopes arrive in `scp`. |
| **AWS Cognito** | `https://cognito-idp.<region>.amazonaws.com/<userPoolId>` | Cognito **access** tokens have no `aud` (only `client_id`). Use a Pre-Token-Generation Lambda (V2) to add `aud`, or adapt the verifier: set `SkipClientIDCheck` and check `client_id` yourself. |
| **Google** | `https://accounts.google.com` | Google access tokens are opaque, not JWTs. Use ID tokens or token introspection instead (see below). |
| **Zitadel / Hydra / Authentik** | Your instance URL | Standard. Make sure access tokens are issued as **JWTs**, not opaque. |

If the service can't reach the issuer's public URL (e.g. inside Docker,
where tokens say `localhost`), set `AUTH_JWKS_URL` to a reachable JWKS
endpoint. The issuer is still checked against `AUTH_ISSUER`. The compose
file does exactly this.

### Opaque tokens

If your IdP issues opaque (non-JWT) access tokens, implement the one-method
`auth.Verifier` interface with [token introspection (RFC 7662)](https://datatracker.ietf.org/doc/html/rfc7662).
POST the token to the introspection endpoint and cache positive results
until `exp`. Then pass it to `auth.Authenticate`. Nothing else changes.

## Local development

`docker compose up` starts
[mock-oauth2-server](https://github.com/navikt/mock-oauth2-server), a real
OIDC server that signs real JWTs. It's configured in
`deploy/mock-oauth2/config.json`:

- The issuer is `http://localhost:8081/default`.
- Any `client_id`/`client_secret` is accepted; `sub` is set to the `client_id`.
- `aud` is `orderbook-api`.
- Requesting `scope=orders:read` gives a read-only token; anything else gives
  `orders:read orders:write`.

```bash
make token                              # sub=dev-client, read+write
client=alice make token                 # sub=alice
scope=orders:read client=bob make token  # read-only

# Equivalent raw request:
curl -s -X POST localhost:8081/default/token \
  -d grant_type=client_credentials -d client_id=alice -d client_secret=x \
  -d scope="orders:read orders:write"
```

The mock server also supports the interactive **authorization code** flow,
for testing a frontend: `http://localhost:8081/default/authorize?...`.

To skip auth entirely while prototyping, set `AUTH_ENABLED=false`. Every
request then runs as `dev-user` with the wildcard scope. This is refused
when `APP_ENV=production`, which is also the default when `APP_ENV` is
unset.

## Service-to-service calls (client credentials)

To call another OAuth-protected API, use the helper in
`internal/platform/auth/client.go`. It gets tokens with the client
credentials grant, caches them until they expire, attaches them to every
request, and propagates the trace context:

```go
billing := auth.NewClient(ctx, auth.ClientCredentials{
    TokenURL:       "https://idp.example.com/oauth2/token",
    ClientID:       os.Getenv("BILLING_CLIENT_ID"),
    ClientSecret:   os.Getenv("BILLING_CLIENT_SECRET"),
    Scopes:         []string{"invoices:write"},
    EndpointParams: map[string][]string{"audience": {"billing-api"}}, // Auth0-style
})
resp, err := billing.Post("https://billing.internal/v1/invoices", "application/json", body)
```

Add the client settings to `config.Config`. Build one client at startup
(in `internal/app`) and inject it into the feature that needs it.

## Testing auth

`internal/testutil/oauthtest` runs an **in-process OIDC issuer**
(`httptest.Server` with discovery + JWKS). Tests therefore use the real
production code path (discovery → JWKS fetch → signature check):

```go
iss := oauthtest.NewIssuer(t)
verifier, _ := auth.NewJWTVerifier(ctx, iss.AuthConfig())

tok := iss.Token(t, oauthtest.Claims{Subject: "alice", Scope: "orders:read", Extra: map[string]any{"tenant_id": "acme"}})
expired := iss.Token(t, oauthtest.Claims{TTL: -time.Minute})
wrongAud := iss.Token(t, oauthtest.Claims{Audience: []string{"other"}})
forged := iss.ForeignToken(t) // signed with an unknown key
```

For handler tests that don't care about tokens, use
`auth.Static(&auth.Principal{...})` to inject a principal directly. See
`internal/order/handler_test.go`.

## Security notes

- Always serve the API over TLS (terminated at the load balancer is fine).
  Bearer tokens are credentials.
- Keep access tokens short-lived (5–60 min). This service can't revoke JWTs
  early. If you need instant revocation, use introspection or a denylist on
  `jti`.
- Never log the `Authorization` header. The access logger does not.
- `aud` validation is what stops a token minted for *another* API at the same
  IdP from working here. Don't disable it.
