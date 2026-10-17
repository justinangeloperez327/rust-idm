slint::include_modules!();

fn main() -> Result<(), slint::PlatformError> {
    let app = MainWindow::new()?;
    app.on_add_download(|url| {
        if !url.trim().is_empty() {
            eprintln!("Download UI is not connected yet: {url}");
        }
    });
    app.on_theme_changed(|_| {});
    app.run()
}
