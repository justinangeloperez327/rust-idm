use download_core::{Download, DownloadId};

fn main() {
    let command = std::env::args().nth(1);
    match command.as_deref() {
        None | Some("--help" | "-h") => {
            println!("rust-idm-cli - download manager developer tools");
            println!("Usage: rust-idm-cli [--help|--version|status]");
            println!("Note: HTTP downloads are introduced in later implementation groups.");
        }
        Some("--version" | "-V") => println!("{}", env!("CARGO_PKG_VERSION")),
        Some("status") => {
            let download = Download::new(DownloadId::new(1).expect("valid constant ID"));
            println!("Core ready. Example download state: {:?}", download.state());
        }
        Some(other) => {
            eprintln!("Unknown command: {other}");
            std::process::exit(2);
        }
    }
}
