// Package apidocs serves the OpenAPI spec and a Swagger UI page.
// The spec file is built into the binary with go:embed, so nothing extra ships with the container.
package apidocs

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var spec []byte

// The page loads Swagger UI's JavaScript/CSS from a CDN, then points it at /openapi.yaml.
// Same server as the API, so "Try it out" works without any CORS setup.
const page = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>FairRelay API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui", persistAuthorization: true });
  </script>
</body>
</html>`

// Register adds GET /docs (the UI) and GET /openapi.yaml (the spec) to the mux.
func Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(spec)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
}
