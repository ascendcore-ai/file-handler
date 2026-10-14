---
trigger: always_on
---

---
trigger: glob
globs: "*.go"
description: "Rules for high-performance file streaming in Go"
---
# Go Streaming Constraints
1. **Zero Memory Buffering:** Never load whole files into RAM. Do not use `io.ReadAll` or `ioutil.ReadAll` under any circumstances.
2. **Uploads:** Never use `r.ParseMultipartForm()`. It buffers to memory/disk. Strictly use `r.MultipartReader()` and iterate through `NextPart()`.
3. **Downloads:** Use `http.ServeContent` or `http.ServeFile` for local files to get automatic Range-request support. For remote streams, pipe directly to `http.ResponseWriter` using `io.Copy`.
4. **Buffer Pooling:** When using `io.CopyBuffer`, allocate the byte slices using a `sync.Pool` to prevent garbage collection spikes during high concurrency.