// Integration tests run against a real Echo HTTP server.
//
// This is a separate module on purpose: the httpcache library itself depends
// only on golang.org/x/sync, and a test dependency in its go.mod would show up
// in every consumer's dependency graph. Nested modules are excluded from the
// parent's ./... patterns, so `go test ./...` at the repo root never builds
// any of this.
module github.com/ferocious-space/httpcache/integration

go 1.26

replace github.com/ferocious-space/httpcache => ../

require (
	github.com/ferocious-space/httpcache v0.0.0-00010101000000-000000000000
	github.com/labstack/echo/v4 v4.15.4
)

require (
	github.com/labstack/gommon v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)
