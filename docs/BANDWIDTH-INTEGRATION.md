# HTTP transfer bandwidth integration

The HTTP managed-download and resumed-range write paths now use a per-transfer token bucket. Set `RUST_IDM_BYTES_PER_SECOND` before launching the desktop to apply the same bytes/second limit separately to each active transfer; unset it for unlimited writing. A transfer is paced in sub-chunks, with cancellable async waits when its budget is exhausted. The implementation reuses `download-scheduler::bandwidth::TokenBucket`.

**Limitations:** this is a per-transfer write-rate limit, not a shared global network bandwidth cap. Network reads can buffer data ahead of pacing, and the UI does not yet expose speed settings. A process-wide shared arbiter, adaptive scheduling and byte-accurate network ingress accounting remain future work.
