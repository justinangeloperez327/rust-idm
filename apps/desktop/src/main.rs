use download_http::transfer::{TransferEngine, TransferEvent};
use slint::{Model, ModelRc, VecModel};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use tokio_util::sync::CancellationToken;

slint::include_modules!();

fn update_row(model: &VecModel<DownloadRow>, id: i32, change: impl FnOnce(&mut DownloadRow)) {
    if let Some(index) = (0..model.row_count())
        .find(|&index| model.row_data(index).is_some_and(|row| row.id == id))
    {
        if let Some(mut row) = model.row_data(index) {
            change(&mut row);
            model.set_row_data(index, row);
        }
    }
}

fn main() -> Result<(), slint::PlatformError> {
    let app = MainWindow::new()?;
    let rows = Rc::new(VecModel::<DownloadRow>::from(Vec::new()));
    let tokens = Rc::new(RefCell::new(HashMap::<i32, CancellationToken>::new()));
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
    let weak = app.as_weak();
    app.on_download_action(move |id, action| {
        let Some(item) = (0..model.row_count())
            .filter_map(|index| model.row_data(index))
            .find(|row| row.id == id)
        else { return };
        match action.as_str() {
            "pause" | "cancel" => {
                if let Some(token) = tokens.borrow_mut().remove(&id) {
                    token.cancel();
                }
                if item.status == "downloading" || item.status == "queued" {
                    let state = if action == "pause" { "paused" } else { "cancelled" };
                    update_row(&model, id, |row| row.status = state.into());
                }
            }
            "start" => {
                if item.status == "downloading" || item.status == "completed" { return; }
                let token = CancellationToken::new();
                tokens.borrow_mut().insert(id, token.clone());
                update_row(&model, id, |row| {
                    row.status = "downloading".into();
                    row.progress = 0;
                });
                let url = item.url.to_string();
                let weak_window = weak.clone();
                std::thread::spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all().build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            eprintln!("Could not launch download runtime: {error}");
                            return;
                        }
                    };
                    runtime.block_on(async move {
                        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                        let filename = format!("rust-idm-{id}.download");
                        let destination = std::env::var_os("USERPROFILE")
                            .map(PathBuf::from)
                            .unwrap_or_else(std::env::temp_dir)
                            .join("Downloads").join(filename);
                        if let Some(parent) = destination.parent() {
                            if let Err(error) = tokio::fs::create_dir_all(parent).await {
                                eprintln!("Cannot create downloads folder: {error}");
                                return;
                            }
                        }
                        let worker_token = token.clone();
                        let worker = async {
                            let engine = TransferEngine::new()?;
                            engine.download(&url, &destination, &worker_token, Some(&tx)).await
                        };
                        tokio::pin!(worker);
                        let result = loop {
                            tokio::select! {
                                event = rx.recv() => {
                                    if let Some(TransferEvent::Progress { received, total }) = event {
                                        let percent = total.filter(|n| *n > 0)
                                            .map_or(0, |n| (received.saturating_mul(100) / n).min(100) as i32);
                                        let weak = weak_window.clone();
                                        let _ = slint::invoke_from_event_loop(move || {
                                            if let Some(window) = weak.upgrade() {
                                                let downloads = window.get_downloads();
                                                if let Some(index) = (0..downloads.row_count())
                                                    .find(|&index| downloads.row_data(index).is_some_and(|row| row.id == id))
                                                {
                                                    if let Some(mut row) = downloads.row_data(index) {
                                                        if row.status == "downloading" {
                                                            row.progress = percent;
                                                            downloads.set_row_data(index, row);
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
                                result = &mut worker => break result,
                            }
                        };
                        let weak = weak_window;
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak.upgrade() {
                                let downloads = window.get_downloads();
                                if let Some(index) = (0..downloads.row_count())
                                    .find(|&index| downloads.row_data(index).is_some_and(|row| row.id == id))
                                {
                                    if let Some(mut row) = downloads.row_data(index) {
                                        if row.status == "downloading" {
                                            if result.is_ok() { row.status = "completed".into(); row.progress = 100; }
                                            else { row.status = "failed".into(); }
                                            downloads.set_row_data(index, row);
                                        }
                                    }
                                }
                            }
                        });
                    });
                });
            }
            _ => {}
        }
    });
    app.run()
}
