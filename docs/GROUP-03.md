# Group 3 — HTTP probing

`download-http` exposes `HttpClient::probe` and `RemoteMetadata`.

- Uses a bounded Reqwest client with Rustls TLS, connection and request timeouts.
- Follows up to ten redirects and refuses non-HTTP(S) redirect targets.
- First attempts HEAD. If the server does not advertise byte ranges, probes with GET `Range: bytes=0-0`.
- Accepts range support only after a 206 with a matching `Content-Range`.
- Captures final URL, length when known, ETag and Last-Modified.

The response body is not buffered into memory. Request probing may still cause a server to begin a transfer; the response is promptly dropped.

Group 4 implements streaming transfers and cancellation. Future hardening must validate redirect destinations against configured network policy, enforce consistency of HTTP validators across range requests and use a bounded streaming buffer.
