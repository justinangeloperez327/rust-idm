mod settings;
use settings::TransferSettings;
use download_http::managed::ManagedDownloader;
use download_http::throttle::configure_global_limit;
use download_http::transfer::TransferEvent;
use download_storage::database::{DownloadDatabase, DownloadRecord};
use download_scheduler::{JobId, Priority, Scheduler, JobState};
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

fn persist_status(db: &DownloadDatabase, id: i32, status: &str, received: Option<u64>, total: Option<u64>) {
    match db.find(i64::from(id)) {
        Ok(Some(mut record)) => {
            record.status = status.to_owned();
            if let Some(bytes) = received {
                record.received_bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            }
            if let Some(bytes) = total {
                record.total_bytes = i64::try_from(bytes).ok();
            }
            // Restarted single-stream transfers always restart at zero.
            if record.total_bytes.is_some_and(|bytes| record.received_bytes > bytes) {
                record.total_bytes = None;
            }
            if let Err(error) = db.update(&record) {
                eprintln!("Could not persist download {id}: {error}");
            }
        }
        Ok(None) => eprintln!("Download {id} not found in database"),
        Err(error) => eprintln!("Could not read download {id}: {error}"),
    }
}

fn main() -> Result<(), slint::PlatformError> {
    let app = MainWindow::new()?;
    let rows = Rc::new(VecModel::<DownloadRow>::from(Vec::new()));
    let tokens = Rc::new(RefCell::new(HashMap::<i32, CancellationToken>::new()));
    app.set_downloads(ModelRc::from(rows.clone()));
    let data_dir = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("RustIDM");
    std::fs::create_dir_all(&data_dir).expect("create Rust IDM data directory");
    let db = Rc::new(DownloadDatabase::open(&data_dir.join("downloads.sqlite"))
        .expect("open Rust IDM database"));
    db.recover_interrupted().expect("recover interrupted download records");
    let existing = db.list().expect("load download history");
    let next_id = Rc::new(RefCell::new(
        existing.iter().filter_map(|record| i32::try_from(record.id).ok())
            .max().unwrap_or(0)
    ));
    for record in existing.into_iter().rev() {
        let Ok(id) = i32::try_from(record.id) else { continue };
        let percent = match record.total_bytes {
            Some(total) if total > 0 => (record.received_bytes.saturating_mul(100) / total)
                .clamp(0, 100) as i32,
            _ => 0,
        };
        rows.push(DownloadRow {
            id,
            name: format!("rust-idm-{id}.download").into(),
            status: record.status.into(),
            progress: percent,
            url: record.url.into(),
        });
    }
    let downloads_dir = std::env::var_os("USERPROFILE")
        .map(PathBuf::from).unwrap_or_else(std::env::temp_dir).join("Downloads");

    let model = rows.clone();
    let add_db = db.clone();
    let add_id = next_id;
    let add_dir = downloads_dir.clone();
    app.on_add_download(move |url| {
        let url = url.trim().to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            eprintln!("Please provide an HTTP or HTTPS URL");
            return;
        }
        let id = match add_id.borrow().checked_add(1) {
            Some(id) => id,
            None => { eprintln!("Download identifier exhausted"); return; }
        };
        let record = DownloadRecord {
            id: i64::from(id),
            url: url.clone(),
            destination: add_dir.join(format!("rust-idm-{id}.download"))
                .to_string_lossy().into_owned(),
            status: "queued".into(),
            received_bytes: 0,
            total_bytes: None,
            etag: None,
            last_modified: None,
            checkpoint_json: None,
        };
        if let Err(error) = add_db.insert(&record) {
            eprintln!("Could not save new download: {error}");
            return;
        }
        *add_id.borrow_mut() = id;
        let name = url.rsplit('/').next().filter(|name| !name.is_empty()).unwrap_or("download");
        model.push(DownloadRow {
            id,
            name: name.into(),
            status: "queued".into(),
            progress: 0,
            url: url.into(),
        });
    });

    let settings_path = data_dir.join("settings.json");
    let saved = TransferSettings::load(&settings_path).unwrap_or_else(|error| {
        eprintln!("Cannot read settings: {error}; using defaults");
        TransferSettings::default()
    });
    app.set_max_concurrent(saved.concurrency);
    app.set_global_limit_kib(saved.global_limit_kib);
    let _ = configure_global_limit((saved.global_limit_kib > 0)
        .then_some(saved.global_limit_kib as u64 * 1024));
    let scheduler = Rc::new(RefCell::new(Scheduler::new(saved.concurrency as usize).expect("valid worker limit")));
    let settings_scheduler = scheduler.clone();
    let settings_window = app.as_weak();
    app.on_apply_settings(move |concurrency, kib| {
        if !(1..=16).contains(&concurrency) || !(0..=1_048_576).contains(&kib) {
            eprintln!("Invalid settings");
            return;
        }
        let rate = if kib == 0 { None } else { Some(kib as u64 * 1024) };
        if let Err(error) = configure_global_limit(rate) {
            eprintln!("Could not apply bandwidth setting: {error}");
            return;
        }
        if let Err(error) = (TransferSettings { concurrency, global_limit_kib: kib }).save(&settings_path) {
            eprintln!("Could not save transfer settings: {error}");
        }
        let ready = {
            let mut scheduler = settings_scheduler.borrow_mut();
            if scheduler.set_limit(concurrency as usize).is_err() { return; }
            scheduler.dispatch_ready()
        };
        if let Some(window) = settings_window.upgrade() {
            for job in ready {
                window.invoke_download_action(job.0 as i32, "launch".into());
            }
        }
    });
    let cleanup_tokens = tokens.clone();
    let completion_scheduler = scheduler.clone();
    let completion_window = app.as_weak();
    app.on_transfer_finished(move |id| {
        cleanup_tokens.borrow_mut().remove(&id);
        let ready = {
            let mut scheduler = completion_scheduler.borrow_mut();
            let _ = scheduler.complete(JobId(id as u64));
            scheduler.dispatch_ready()
        };
        if let Some(window) = completion_window.upgrade() {
            for job in ready {
                window.invoke_download_action(job.0 as i32, "launch".into());
            }
        }
    });
    let model = rows;
    let action_db = db.clone();
    let action_scheduler = scheduler;
    let weak = app.as_weak();
    let data_dir_for_workers = data_dir.join("downloads.sqlite");
    app.on_download_action(move |id, action| {
        let Some(item) = (0..model.row_count())
            .filter_map(|index| model.row_data(index))
            .find(|row| row.id == id)
        else {
            if let Some(window) = weak.upgrade() { window.set_transfer_message("Select an item using Details before starting.".into()); }
            return;
        };
        match action.as_str() {
            "pause" | "cancel" => {
                if action_scheduler.borrow().state(JobId(id as u64)) == Some(JobState::Queued) {
                    let _ = action_scheduler.borrow_mut().cancel_queued(JobId(id as u64));
                }
                if let Some(token) = tokens.borrow().get(&id) {
                    token.cancel();
                }
                if item.status == "downloading" || item.status == "queued" {
                    let state = if action == "pause" { "paused" } else { "cancelled" };
                    update_row(&model, id, |row| row.status = state.into());
                    persist_status(&action_db, id, state, None, None);
                }
            }
            "start" => {
                if item.status == "downloading" || item.status == "completed" || tokens.borrow().contains_key(&id) { return; }
                if action_scheduler.borrow().state(JobId(id as u64)).is_some() { return; }
                if let Err(error) = action_scheduler.borrow_mut().enqueue(JobId(id as u64), Priority::Normal) {
                    eprintln!("Could not enqueue download: {error:?}");
                    return;
                }
                if let Some(window) = weak.upgrade() { window.set_transfer_message(format!("Download {id} added to queue").into()); }
                persist_status(&action_db, id, "queued", None, None);
                update_row(&model, id, |row| row.status = "queued".into());
                let ready = action_scheduler.borrow_mut().dispatch_ready();
                if let Some(window) = weak.upgrade() {
                    for job in ready { window.invoke_download_action(job.0 as i32, "launch".into()); }
                }
            }
            "launch" => {
                if action_scheduler.borrow().state(JobId(id as u64)) != Some(JobState::Running) { return; }
                if tokens.borrow().contains_key(&id) { return; }
                if let Some(window) = weak.upgrade() { window.set_transfer_message(format!("Download {id}: connecting to server…").into()); }
                persist_status(&action_db, id, "downloading", None, None);
                let token = CancellationToken::new();
                tokens.borrow_mut().insert(id, token.clone());
                update_row(&model, id, |row| {
                    row.status = "downloading".into();
                });
                let url = item.url.to_string();
                let weak_window = weak.clone();
                let progress_database_path = data_dir_for_workers.clone();
                std::thread::spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all().build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            eprintln!("Could not launch download runtime: {error}");
                            let weak = weak_window.clone();
                            let _ = slint::invoke_from_event_loop(move || {
                                if let Some(window) = weak.upgrade() { window.set_transfer_message(format!("Download {id}: worker startup failed").into()); window.invoke_transfer_finished(id); }
                            });
                            return;
                        }
                    };
                    let worker_db = match DownloadDatabase::open(&progress_database_path) {
                        Ok(db) => db,
                        Err(error) => {
                            eprintln!("Cannot open transfer database: {error}");
                            let weak = weak_window.clone();
                            let _ = slint::invoke_from_event_loop(move || {
                                if let Some(window) = weak.upgrade() { window.invoke_transfer_finished(id); }
                            });
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
                                let weak = weak_window.clone();
                                let _ = slint::invoke_from_event_loop(move || {
                                    if let Some(window) = weak.upgrade() { window.invoke_transfer_finished(id); }
                                });
                                return;
                            }
                        }
                        let worker_token = token.clone();
                        let worker = async {
                            let engine = ManagedDownloader::new()?;
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
                                        // Database updates occur on the worker, never the Slint UI thread.
                                        if let Ok(Some(record)) = worker_db.find(i64::from(id)) {
                                            if record.status == "downloading" {
                                                persist_status(&worker_db, id, "downloading", Some(received), total);
                                            }
                                        }
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
                        let error_message = result.as_ref().err().map(|error| format!("Download {id} failed: {error}"));
                        if let Some(message) = &error_message { eprintln!("{message}"); }
                        let weak = weak_window;
                        let completed_bytes = result.as_ref().ok().map(|outcome| outcome.bytes);
                        if let Ok(Some(record)) = worker_db.find(i64::from(id)) {
                            if record.status == "downloading" {
                                let state = if result.is_ok() { "completed" } else { "failed" };
                                persist_status(&worker_db, id, state, completed_bytes, completed_bytes);
                            }
                        }
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak.upgrade() {
                                if let Some(message) = error_message {
                                    window.set_transfer_message(message.into());
                                } else if result.is_ok() {
                                    window.set_transfer_message(format!("Download {id} completed; check Downloads folder").into());
                                }
                                window.invoke_transfer_finished(id);
                                let downloads = window.get_downloads();
                                if let Some(index) = (0..downloads.row_count())
                                    .find(|&index| downloads.row_data(index).is_some_and(|row| row.id == id))
                                {
                                    if let Some(mut row) = downloads.row_data(index) {
                                        if row.status == "downloading" {
                                            if result.is_ok() { row.status = "completed".into(); row.progress = 100; }
                                            else { row.status = "failed".into(); }
                                            let new_status = row.status.to_string();
                                            downloads.set_row_data(index, row);
                                            let _ = new_status;
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
