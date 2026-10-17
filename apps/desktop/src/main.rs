use slint::{Model, ModelRc, VecModel};
use std::rc::Rc;

slint::include_modules!();

fn main() -> Result<(), slint::PlatformError> {
    let app = MainWindow::new()?;
    let rows = Rc::new(VecModel::<DownloadRow>::from(Vec::new()));
    app.set_downloads(ModelRc::from(rows.clone()));
    let model = rows.clone();
    app.on_add_download(move |url| {
        let url = url.trim().to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            eprintln!("Please provide an HTTP or HTTPS URL");
            return;
        }
        let id = model.row_count() as i32 + 1;
        let name = url.rsplit('/').next().filter(|name| !name.is_empty()).unwrap_or("download");
        model.push(DownloadRow {
            id,
            name: name.into(),
            status: "queued".into(),
            progress: 0,
            url: url.into(),
        });
    });
    let model = rows;
    app.on_download_action(move |id, action| {
        if let Some(index) = (0..model.row_count()).find(|&index| {
            model.row_data(index).is_some_and(|row| row.id == id)
        }) {
            if let Some(mut row) = model.row_data(index) {
                row.status = match action.as_str() {
                    "start" => "downloading",
                    "pause" => "paused",
                    "cancel" => "cancelled",
                    _ => return,
                }.into();
                model.set_row_data(index, row);
            }
        }
    });
    app.run()
}
