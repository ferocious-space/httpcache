package httpcache

// https://github.com/gregjones/httpcache + minor modifications
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	stale = iota
	fresh
	transparent
)

// XFromCache is the header added to responses that are returned from the cache.
// Its value is the Date header of the cached response.
const XFromCache = "X-Client-Cache"

// A Cache is used by the Transport to store and retrieve responses. Implement
// it to back the transport with any store: a file system, an embedded
// key/value database, Redis, S3, and so on.
//
// Implementations must satisfy the following contract:
//
//   - All three methods must be safe for concurrent use by multiple
//     goroutines. The Transport calls them from whichever goroutine issued
//     the request.
//   - Set takes ownership of responseBytes; the Transport does not modify the
//     slice afterwards, and the implementation may retain it.
//   - The slice returned by Get must remain valid and unmodified after Get
//     returns. Implementations backed by memory-mapped storage must copy
//     before returning.
//   - Get must return ok=false rather than an error for a miss; there is no
//     error channel. Report backend failures as a miss.
//   - Delete on an absent key must succeed silently.
//
// A cache may evict entries at any time; the Transport treats an eviction as
// a cache miss and revalidates or refetches.
type Cache interface {
	// Get returns the []byte representation of a cached response and a bool
	// set to true if the value was present.
	Get(key string) (responseBytes []byte, ok bool)
	// Set stores the []byte representation of a response against a key.
	Set(key string, responseBytes []byte)
	// Delete removes the value associated with the key.
	Delete(key string)
}

// cacheKey returns the cache key for req.
func cacheKey(req *http.Request) string {
	if req.Method == http.MethodGet {
		return req.URL.String()
	}
	return req.Method + " " + req.URL.String()
}

// flightKey scopes request deduplication to requests that are identical in
// both target and headers.
//
// Sharing one upstream response between requests whose headers differ would
// serve one caller another caller's response: two concurrent requests to the
// same URL with different Authorization headers must never be collapsed. Only
// cacheable GET/HEAD requests are ever deduplicated, so the request body is
// not part of the key.
func flightKey(key string, req *http.Request) string {
	names := make([]string, 0, len(req.Header))
	for name := range req.Header {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	var n [8]byte
	// Every component is length-prefixed. Separator bytes alone would be
	// ambiguous: a header value containing the separator is indistinguishable
	// from a structural boundary, so two different header sets could hash to
	// the same digest, share one flight, and hand one caller another caller's
	// response — exactly what scoping the key on headers is meant to prevent.
	write := func(s string) {
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	writeCount := func(c int) {
		binary.BigEndian.PutUint64(n[:], uint64(c))
		h.Write(n[:])
	}

	write(key)
	writeCount(len(names))
	for _, name := range names {
		write(name)
		values := req.Header[name]
		writeCount(len(values))
		for _, value := range values {
			write(value)
		}
	}
	return string(h.Sum(nil))
}

// CachedResponse returns the cached http.Response for req if present, and nil
// otherwise.
func CachedResponse(c Cache, req *http.Request) (resp *http.Response, err error) {
	cachedVal, ok := c.Get(cacheKey(req))
	if !ok {
		return
	}
	b := bytes.NewBuffer(cachedVal)
	return http.ReadResponse(bufio.NewReaderSize(b, b.Len()), req)
}

// Transport is an implementation of http.RoundTripper that will return values from a cache
// where possible (avoiding a network request) and will additionally add validators (etag/if-modified-since)
// to repeated requests allowing servers to return 304 / Not Modified
type Transport struct {
	// The RoundTripper interface actually used to make requests
	// If nil, http.DefaultTransport is used
	Transport    http.RoundTripper
	Cache        Cache
	singleflight singleflight.Group
	// If true, responses returned from the cache will be given an extra header, X-From-Cache
	MarkCachedResponses bool
	// MaxCacheableBytes is the largest response body that will be cached.
	// A larger response is still delivered to the caller in full, streamed
	// rather than buffered, but is not stored.
	//
	// Zero selects DefaultMaxCacheableBytes. A negative value removes the
	// ceiling, which lets a single response consume memory proportional to
	// its size — only sensible when every origin is trusted and bounded.
	MaxCacheableBytes int64
}

// DefaultMaxCacheableBytes is the ceiling applied when a Transport leaves
// MaxCacheableBytes at zero.
const DefaultMaxCacheableBytes = 10 << 20 // 10 MiB

// maxCacheableBytes resolves the configured ceiling. It returns a negative
// value to mean "no ceiling".
func (t *Transport) maxCacheableBytes() int64 {
	if t.MaxCacheableBytes == 0 {
		return DefaultMaxCacheableBytes
	}
	return t.MaxCacheableBytes
}

// defaultTransport is built at most once and shared, so that connections are
// pooled across requests. Building a transport per request leaks its idle
// connection pool and defeats keep-alive entirely.
var defaultTransport = sync.OnceValue(func() http.RoundTripper {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
})

// roundTripper returns the configured RoundTripper, or the shared default.
func (t *Transport) roundTripper() http.RoundTripper {
	if t.Transport != nil {
		return t.Transport
	}
	return defaultTransport()
}

// do performs req, optionally deduplicating it against identical in-flight
// requests.
//
// A deduplicated upstream response is buffered once and each caller parses its
// own copy, so every caller gets an independent body it can read and close.
// Handing the same *http.Response to several callers lets only the first read
// it.
//
// That buffering is why dedup is reserved for revalidating an entry already in
// the cache. On that path the response is either a 304 with no body, or a
// representation that is about to be stored — and storing it buffers the body
// anyway, so nothing is spent that would not have been spent regardless. A
// request with nothing cached behind it has no such bound: it could be a
// multi-gigabyte download or an endless event stream, and buffering it would
// hold the whole thing in memory and withhold every byte from the caller until
// the origin finished. Those stream instead, at the cost of letting concurrent
// first-time requests for the same URL each reach the origin.
// errTooLargeToShare reports that a response exceeded MaxCacheableBytes and so
// was not buffered for deduplication. It never reaches the caller.
var errTooLargeToShare = errors.New("httpcache: response too large to share")

func (t *Transport) do(key string, req *http.Request, dedup bool) (*http.Response, error) {
	if !dedup {
		return t.roundTripper().RoundTrip(req)
	}

	v, err, _ := t.singleflight.Do(flightKey(key, req), func() (interface{}, error) {
		resp, err := t.roundTripper().RoundTrip(req)
		if err != nil {
			return nil, err
		}
		// A RoundTripper is not guaranteed to set Body (e.g. a hand-built
		// mock response); DumpResponse tolerates a nil Body, but closing one
		// unconditionally would panic.
		if resp.Body != nil {
			defer resp.Body.Close()

			if limit := t.maxCacheableBytes(); limit >= 0 {
				// Sharing means holding the whole body in memory, so apply the
				// same ceiling the cache uses. Read one byte past it to tell
				// "at the limit" from "over" it.
				body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
				if err != nil {
					return nil, err
				}
				if int64(len(body)) > limit {
					return nil, errTooLargeToShare
				}
				resp.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
		return httputil.DumpResponse(resp, true)
	})
	if err != nil {
		// Too large to hold for the group: nothing was shared, so every caller
		// fetches for itself and streams the result.
		if errors.Is(err, errTooLargeToShare) {
			return t.roundTripper().RoundTrip(req)
		}
		// The leader may have been cancelled by its own caller. If this
		// caller's context is still live, make its own attempt instead of
		// inheriting an unrelated cancellation.
		if errors.Is(err, context.Canceled) && req.Context().Err() == nil {
			return t.roundTripper().RoundTrip(req)
		}
		return nil, err
	}

	dump := v.([]byte)
	return http.ReadResponse(bufio.NewReader(bytes.NewReader(dump)), req)
}

type CacheOption func(*cacheParams)
type cacheParams struct {
	markResponse      bool
	maxCacheableBytes int64
}

func WithMarkedResponses(mark bool) CacheOption {
	return func(params *cacheParams) {
		params.markResponse = mark
	}
}

// WithMaxCacheableBytes sets the largest response body that will be cached.
// Responses above the ceiling are still delivered in full, just not stored.
// A negative value removes the ceiling; zero selects DefaultMaxCacheableBytes.
func WithMaxCacheableBytes(n int64) CacheOption {
	return func(params *cacheParams) {
		params.maxCacheableBytes = n
	}
}

// NewTransport returns a new Transport with the
// provided Cache implementation and MarkCachedResponses set to true
func NewTransport(c Cache, opt ...CacheOption) *Transport {
	params := &cacheParams{
		markResponse:      true,
		maxCacheableBytes: DefaultMaxCacheableBytes,
	}
	for _, o := range opt {
		o(params)
	}
	return &Transport{
		Cache:               c,
		MarkCachedResponses: params.markResponse,
		MaxCacheableBytes:   params.maxCacheableBytes,
	}
}

// Client returns an *http.Client that caches responses.
func (t *Transport) Client() *http.Client {
	return &http.Client{Transport: t}
}

// varyMatches will return false unless all of the cached values for the headers listed in Vary
// match the new request
func varyMatches(cachedResp *http.Response, req *http.Request) bool {
	for _, header := range headerAllCommaSepValues(cachedResp.Header, "vary") {
		header = http.CanonicalHeaderKey(header)
		if header != "" && req.Header.Get(header) != cachedResp.Header.Get("X-Varied-"+header) {
			return false
		}
	}
	return true
}

// RoundTrip takes a Request and returns a Response
//
// If there is a fresh Response already in cache, then it will be returned without connecting to
// the server.
//
// If there is a stale Response, then any validators it contains will be set on the new request
// to give the server a chance to respond with NotModified. If this happens, then the cached Response
// will be returned.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	var err error
	var resp *http.Response
	cacheKey := cacheKey(req)
	cacheable := (req.Method == "GET" || req.Method == "HEAD") && req.Header.Get("range") == ""

	var cachedResp *http.Response
	if cacheable {
		cachedResp, err = CachedResponse(t.Cache, req)
	} else {
		// Need to invalidate an existing value
		t.Cache.Delete(cacheKey)
	}

	if cacheable && cachedResp != nil && err == nil { // mark the cached response
		if t.MarkCachedResponses {
			cachedResp.Header.Set(XFromCache, cachedResp.Header.Get("Date"))
		}

		// check vary-match
		if varyMatches(cachedResp, req) {
			// Can only use cached value if the new request doesn't Vary significantly
			switch getFreshness(cachedResp.Header, req.Header) {
			case fresh:
				return cachedResp, nil
			case stale:
				var clone *http.Request
				// Add validators if caller hasn't already done so
				etag := cachedResp.Header.Get("etag")
				if etag != "" && req.Header.Get("etag") == "" {
					clone = req.Clone(req.Context())
					clone.Header.Set("if-none-match", etag)
				}
				lastModified := cachedResp.Header.Get("last-modified")
				if lastModified != "" && req.Header.Get("last-modified") == "" {
					if clone == nil {
						clone = req.Clone(req.Context())
					}
					clone.Header.Set("if-modified-since", lastModified)
				}
				if clone != nil {
					req = clone
				}
			}
		}
		resp, err = t.do(cacheKey, req, true)
		if err == nil {
			// handle 5xx family errors if can stale
			if resp.StatusCode >= 500 && resp.StatusCode != 501 {
				if req.Method == "GET" && canStaleOnError(cachedResp.Header, req.Header) {
					cachedResp.Header.Add(
						textproto.CanonicalMIMEHeaderKey("Warning"),
						fmt.Sprintf(
							"110 httpCache \"Response is stale\" %s",
							time.Now().UTC().Format(time.RFC1123),
						),
					)
					_, _ = io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					return cachedResp, nil
				}
			}
			switch resp.StatusCode {
			case http.StatusNotModified:
				// our cached content is unmodified, swap it
				endToEndHeaders := getEndToEndHeaders(resp.Header)

				for _, header := range endToEndHeaders {
					cachedResp.Header[header] = resp.Header[header]
				}

				// we are not using the response so drain it and close the body
				_, _ = io.ReadAll(resp.Body)
				_ = resp.Body.Close()

				// we set the response to the cached response because they are the same
				resp = cachedResp
			case http.StatusNotImplemented:
				// wat ?
				t.Cache.Delete(cacheKey)
				return resp, nil
			case http.StatusGatewayTimeout, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusInternalServerError:
				// if we are here we cant stale on error , but dont delete the cache also as this is recoverable state
				// just proxy the request
				return resp, err
			case http.StatusTooManyRequests:
				// we are getting rate limited , dont delete cache
				// just proxy the request
				return resp, err
			default:
				// delete the cache if we received something new
				t.Cache.Delete(cacheKey)
			}
		} else {
			// If the caller's own context was cancelled or timed out, that
			// takes priority over stale-if-error: report the cancellation
			// instead of silently returning a stale cached response. A
			// deadline/timeout surfacing from a lower layer while the
			// request's own context is still live is the origin-unreachable
			// case stale-if-error exists for, so that case falls through
			// to the check below.
			if req.Context().Err() != nil {
				return nil, err
			}
			// RFC 5861 stale-if-error applies to any error that prevents the
			// request from being satisfied, not only a 5xx response - the round
			// trip failing outright (e.g. the origin is unreachable) is exactly
			// the case stale-if-error exists for. Mirrors the resp.StatusCode
			// >= 500 branch above.
			if req.Method == "GET" && canStaleOnError(cachedResp.Header, req.Header) {
				cachedResp.Header.Add(
					textproto.CanonicalMIMEHeaderKey("Warning"),
					fmt.Sprintf(
						"110 httpCache \"Response is stale\" %s",
						time.Now().UTC().Format(time.RFC1123),
					),
				)
				return cachedResp, nil
			}
			// delete the cache on error
			var urlError *url.Error
			if errors.As(err, &urlError) {
				if urlError.Temporary() || urlError.Timeout() {
					return nil, err
				}
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// skip deletion on context errors
				return nil, err
			}

			t.Cache.Delete(cacheKey)
			return nil, err
			// rErr := err.(*url.Error)
			// if rErr.Temporary() || rErr.Timeout() {
			// 	// dont delete cache on temporary errors or timeouts
			// 	return nil, err
			// }
			// t.Cache.Delete(cacheKey)
			// return nil, err
		}
	} else {
		// no cached response or request not cachable
		reqCacheControl := parseCacheControl(req.Header)
		if reqCacheControl.Have("only-if-cached") {
			// Synthesized locally: never store it, or the cache serves a 504
			// back as though the origin had sent it.
			return newGatewayTimeoutResponse(req), nil
		}
		{
			// Nothing is cached for this request, so the response size is
			// unbounded and unknown: stream it rather than buffering it to
			// share. See do for why dedup is limited to revalidation.
			resp, err = t.do(cacheKey, req, false)
			if err != nil {
				return nil, err
			}
		}
	}

	if cacheable && canStore(parseCacheControl(req.Header), parseCacheControl(resp.Header)) {
		// Record the request values for any headers the response varies on,
		// so varyMatches can reject a mismatched request on the way back in.
		for _, varyKey := range headerAllCommaSepValues(resp.Header, "vary") {
			varyKey = http.CanonicalHeaderKey(varyKey)
			if reqValue := req.Header.Get(varyKey); reqValue != "" {
				resp.Header.Set("X-Varied-"+varyKey, reqValue)
			}
		}
		if req.Method == http.MethodGet {
			// Store the body as the caller reads it, not before. Draining it
			// here to build the cache entry would withhold every byte until
			// the origin finished and hold the whole response in memory
			// first, which makes large downloads and streaming endpoints
			// unusable. A caller that stops reading early just leaves
			// nothing cached.
			toCache := *resp
			resp.Body = &cachingReadCloser{
				body:  resp.Body,
				limit: t.maxCacheableBytes(),
				onEOF: func(body io.Reader) {
					toCache.Body = io.NopCloser(body)
					if respBytes, err := httputil.DumpResponse(&toCache, true); err == nil {
						t.Cache.Set(cacheKey, respBytes)
					}
				},
			}
		} else {
			// HEAD and other cacheable methods carry no body worth streaming.
			// DumpResponse drains resp.Body and restores it with an
			// equivalent reader, so on success the caller still receives a
			// readable body.
			respBytes, err := httputil.DumpResponse(resp, true)
			if err != nil {
				// On a failed drain DumpResponse returns before restoring the
				// body, so resp carries a partially consumed one. Reporting
				// success here would hand the caller a silently truncated
				// response — the read error is the caller's answer.
				resp.Body.Close()
				return nil, err
			}
			if limit := t.maxCacheableBytes(); limit < 0 || int64(len(respBytes)) <= limit {
				t.Cache.Set(cacheKey, respBytes)
			}
		}
	} else {
		t.Cache.Delete(cacheKey)
	}
	return resp, nil
}

// cachingReadCloser wraps a response body and hands the bytes that were read
// to onEOF once, when the underlying body reaches EOF. It lets the transport
// cache a response without draining it first: the caller reads at its own
// pace and the cache is written only if the whole body was actually consumed.
//
// A caller that abandons the body early leaves nothing cached, which is the
// correct outcome — a partial body must never be stored and later served as
// though it were complete.
// A body that grows past limit stops being buffered and is never cached; the
// caller still receives every byte. A negative limit means no ceiling.
type cachingReadCloser struct {
	body     io.ReadCloser
	buf      bytes.Buffer
	onEOF    func(io.Reader)
	limit    int64
	cached   bool
	oversize bool
}

// The order here is load-bearing. A reader may return n > 0 together with
// io.EOF on its final call, and net/http's Content-Length body reader does
// exactly that — so the bytes must be buffered before EOF is acted on. Firing
// onEOF first would store a body missing its last read, which for small
// responses can be most of it.
func (c *cachingReadCloser) Read(p []byte) (int, error) {
	n, err := c.body.Read(p)
	if n > 0 && !c.oversize {
		c.buf.Write(p[:n]) // bytes.Buffer.Write never returns an error
		if c.limit >= 0 && int64(c.buf.Len()) > c.limit {
			// Too large to cache. Drop what was buffered rather than carry it
			// for the rest of the read, and hand the remaining bytes straight
			// through.
			c.oversize = true
			c.buf = bytes.Buffer{}
		}
	}
	if err == io.EOF && !c.cached && !c.oversize {
		c.cached = true
		c.onEOF(bytes.NewReader(c.buf.Bytes()))
	}
	return n, err
}

func (c *cachingReadCloser) Close() error {
	return c.body.Close()
}

// ErrNoDateHeader indicates that the HTTP headers contained no Date header.
var ErrNoDateHeader = errors.New("no Date header")

// Date parses and returns the value of the Date header.
func Date(respHeaders http.Header) (date time.Time, err error) {
	dateHeader := respHeaders.Get("date")
	if dateHeader == "" {
		err = ErrNoDateHeader
		return
	}

	return time.Parse(time.RFC1123, dateHeader)
}

type realClock struct{}

func (c *realClock) since(d time.Time) time.Duration {
	return time.Since(d)
}

// timer abstracts elapsed-time measurement so tests can inject a fake clock.
type timer interface {
	since(d time.Time) time.Duration
}

var clock timer = &realClock{}

// getFreshness will return one of fresh/stale/transparent based on the Cache-Control
// values of the request and the response
//
// fresh indicates the response can be returned
// stale indicates that the response needs validating before it is returned
// transparent indicates the response should not be used to fulfil the request
//
// Because this is only a private cache, 'public' and 'private' in Cache-Control aren't
// signficant. Similarly, smax-age isn't used.
func getFreshness(respHeaders, reqHeaders http.Header) (freshness int) {
	respCacheControl := parseCacheControl(respHeaders)
	reqCacheControl := parseCacheControl(reqHeaders)
	if reqCacheControl.Have("only-if-cached") {
		return fresh
	}
	if reqCacheControl.Have("no-cache") {
		return transparent
	}
	if respCacheControl.Have("no-cache") {
		// The origin requires revalidation before this response is reused.
		return stale
	}
	if respCacheControl.Have("immutable") {
		return fresh
	}

	date, err := Date(respHeaders)
	if err != nil {
		return stale
	}
	currentAge := clock.since(date)

	var lifetime time.Duration
	var zeroDuration time.Duration

	// If a response includes both an Expires header and a max-age directive,
	// the max-age directive overrides the Expires header, even if the Expires header is more restrictive.
	if maxAge, ok := respCacheControl["max-age"]; ok {
		lifetime, err = time.ParseDuration(maxAge + "s")
		if err != nil {
			lifetime = zeroDuration
		}
	} else {
		expiresHeader := respHeaders.Get("Expires")
		if expiresHeader != "" {
			expires, err := time.Parse(time.RFC1123, expiresHeader)
			if err != nil {
				lifetime = zeroDuration
			} else {
				lifetime = expires.Sub(date)
			}
		}
	}

	if maxAge, ok := reqCacheControl["max-age"]; ok {
		// The client will accept a response whose age is no greater than the
		// given number of seconds. This can only shorten the usable lifetime,
		// never extend it past what the origin allowed (RFC 7234 5.2.1.1).
		reqLifetime, err := time.ParseDuration(maxAge + "s")
		if err != nil {
			reqLifetime = zeroDuration
		}
		lifetime = min(lifetime, reqLifetime)
	}

	if minfresh, ok := reqCacheControl["min-fresh"]; ok {
		//  the client wants a response that will still be fresh for at least the specified number of seconds.
		minfreshDuration, err := time.ParseDuration(minfresh + "s")
		if err == nil {
			currentAge = currentAge + minfreshDuration
		}
	}

	if maxstale, ok := reqCacheControl["max-stale"]; ok {
		// Indicates that the client is willing to accept a response that has exceeded its expiration time.
		// If max-stale is assigned a value, then the client is willing to accept a response that has exceeded
		// its expiration time by no more than the specified number of seconds.
		// If no value is assigned to max-stale, then the client is willing to accept a stale response of any age.
		//
		// Responses served only because of a max-stale value are supposed to have a Warning header added to them,
		// but that seems like a  hassle, and is it actually useful? If so, then there needs to be a different
		// return-value available here.
		if maxstale == "" {
			return fresh
		}
		maxstaleDuration, err := time.ParseDuration(maxstale + "s")
		if err == nil {
			currentAge = currentAge - maxstaleDuration
		}
	}

	if lifetime > currentAge {
		return fresh
	}

	return stale
}

// Returns true if either the request or the response includes the stale-if-error
func canStaleOnError(respHeaders, reqHeaders http.Header) bool {
	respCacheControl := parseCacheControl(respHeaders)
	reqCacheControl := parseCacheControl(reqHeaders)

	var err error
	lifetime := time.Duration(-1)

	if staleMaxAge, ok := respCacheControl["stale-if-error"]; ok {
		if staleMaxAge != "" {
			lifetime, err = time.ParseDuration(staleMaxAge + "s")
			if err != nil {
				return false
			}
		} else {
			return true
		}
	}
	if staleMaxAge, ok := reqCacheControl["stale-if-error"]; ok {
		if staleMaxAge != "" {
			lifetime, err = time.ParseDuration(staleMaxAge + "s")
			if err != nil {
				return false
			}
		} else {
			return true
		}
	}

	if lifetime >= 0 {
		date, err := Date(respHeaders)
		if err != nil {
			return false
		}
		currentAge := clock.since(date)
		if lifetime > currentAge {
			return true
		}
	}

	return false
}

func getEndToEndHeaders(respHeaders http.Header) []string {
	// These headers are always hop-by-hop
	hopByHopHeaders := map[string]struct{}{
		"Connection":          {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {},
		"Trailers":            {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	}

	for _, extra := range strings.Split(respHeaders.Get("connection"), ",") {
		// any header listed in connection, if present, is also considered hop-by-hop
		if strings.Trim(extra, " ") != "" {
			hopByHopHeaders[http.CanonicalHeaderKey(extra)] = struct{}{}
		}
	}
	var endToEndHeaders []string
	for respHeader := range respHeaders {
		if _, ok := hopByHopHeaders[respHeader]; !ok {
			endToEndHeaders = append(endToEndHeaders, respHeader)
		}
	}
	return endToEndHeaders
}

func canStore(reqCacheControl, respCacheControl cacheControl) (canStore bool) {
	if _, ok := respCacheControl["no-store"]; ok {
		return false
	}
	if _, ok := reqCacheControl["no-store"]; ok {
		return false
	}
	return true
}

func newGatewayTimeoutResponse(req *http.Request) *http.Response {
	var braw bytes.Buffer
	braw.WriteString("HTTP/1.1 504 Gateway Timeout\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(&braw), req)
	if err != nil {
		panic(err)
	}
	return resp
}

type cacheControl map[string]string

func parseCacheControl(headers http.Header) cacheControl {
	cc := cacheControl{}
	ccHeader := headers.Get("Cache-Control")
	for _, part := range strings.Split(ccHeader, ",") {
		part = strings.Trim(part, " ")
		if part == "" {
			continue
		}
		if strings.ContainsRune(part, '=') {
			keyval := strings.Split(part, "=")
			cc[strings.Trim(keyval[0], " ")] = strings.Trim(keyval[1], ",")
		} else {
			cc[part] = ""
		}
	}
	return cc
}

func (c cacheControl) Have(key string) bool {
	_, ok := c[key]
	return ok
}

// headerAllCommaSepValues returns all comma-separated values (each
// with whitespace trimmed) for header name in headers. According to
// Section 4.2 of the HTTP/1.1 spec
// (http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2),
// values from multiple occurrences of a header should be concatenated, if
// the header's value is a comma-separated list.
func headerAllCommaSepValues(headers http.Header, name string) []string {
	vals := make([]string, 0)
	for _, val := range headers[http.CanonicalHeaderKey(name)] {
		fields := strings.Split(val, ",")
		for i, f := range fields {
			fields[i] = strings.TrimSpace(f)
		}
		vals = append(vals, fields...)
	}
	return vals
}
