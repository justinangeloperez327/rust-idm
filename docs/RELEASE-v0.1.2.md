# Rust IDM v0.1.2 Preview

Windows x64 pre-release. Native Slint desktop interface, HTTP(S) transfers, SQLite history, queue, bandwidth controls, and strong-ETag partial recovery.

Assets: RustIDM-v0.1.2-win64-setup.exe (per-user installer); RustIDM-v0.1.2-win64-portable.zip (extract and launch RustIDM.exe); SHA256SUMS.txt.

Unsigned preview; Windows may warn about an unknown publisher. Only install assets from this official GitHub release.

Limitations: Download servers must provide known Content-Length and strong ETag for the managed transfer path. Browser integration, auto-update, system tray, code signing and comprehensive desktop acceptance tests are not included. User downloads use generated filenames in the Downloads directory. This is not production-ready software.

Fix in v0.1.2: Ordinary HTTP downloads no longer fail solely because the server lacks a strong ETag or known content length. Such downloads cannot resume and restart from zero when interrupted.

Additional v0.1.2 fix: the desktop displays connection, completion, and download failure messages above the downloads list so HTTP and filesystem errors are visible.
