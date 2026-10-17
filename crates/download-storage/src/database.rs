//! SQLite persistence for download metadata, status and durable checkpoints.
//! The synchronous connection is not shared across Tokio executor threads.

use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS downloads (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  destination TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('queued','downloading','paused','completed','failed','cancelled')),
  received_bytes INTEGER NOT NULL DEFAULT 0 CHECK(received_bytes >= 0),
  total_bytes INTEGER CHECK(total_bytes IS NULL OR total_bytes >= 0),
  etag TEXT,
  last_modified TEXT,
  checkpoint_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK(total_bytes IS NULL OR received_bytes <= total_bytes)
);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadRecord {
    pub id: i64,
    pub url: String,
    pub destination: String,
    pub status: String,
    pub received_bytes: i64,
    pub total_bytes: Option<i64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub checkpoint_json: Option<String>,
}

pub struct DownloadDatabase {
    conn: Connection,
}

impl DownloadDatabase {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        Self::initialize(conn)
    }

    pub fn in_memory() -> rusqlite::Result<Self> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(conn: Connection) -> rusqlite::Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version > 1 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        if version == 0 {
            conn.execute_batch("BEGIN IMMEDIATE;")?;
            let migrated = (|| {
                conn.execute_batch(SCHEMA)?;
                conn.pragma_update(None, "user_version", 1)?;
                Ok::<(), rusqlite::Error>(())
            })();
            if let Err(error) = migrated {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(error);
            }
            conn.execute_batch("COMMIT;")?;
        }
        Ok(Self { conn })
    }

    pub fn insert(&self, record: &DownloadRecord) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO downloads (id,url,destination,status,received_bytes,total_bytes,etag,last_modified,checkpoint_json)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                record.id, record.url, record.destination, record.status,
                record.received_bytes, record.total_bytes, record.etag,
                record.last_modified, record.checkpoint_json,
            ],
        )?;
        Ok(())
    }

    pub fn update(&self, record: &DownloadRecord) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE downloads SET url=?2,destination=?3,status=?4,received_bytes=?5,
             total_bytes=?6,etag=?7,last_modified=?8,checkpoint_json=?9,
             updated_at=CURRENT_TIMESTAMP WHERE id=?1",
            params![
                record.id, record.url, record.destination, record.status,
                record.received_bytes, record.total_bytes, record.etag,
                record.last_modified, record.checkpoint_json,
            ],
        )?;
        Ok(changed != 0)
    }

    pub fn find(&self, id: i64) -> rusqlite::Result<Option<DownloadRecord>> {
        self.conn.query_row(
            "SELECT id,url,destination,status,received_bytes,total_bytes,etag,last_modified,checkpoint_json
             FROM downloads WHERE id=?1",
            [id],
            decode_record,
        ).optional()
    }

    pub fn list(&self) -> rusqlite::Result<Vec<DownloadRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,url,destination,status,received_bytes,total_bytes,etag,last_modified,checkpoint_json
             FROM downloads ORDER BY id DESC"
        )?;
        let records = stmt.query_map([], decode_record)?.collect();
        records
    }

    /// Mark interrupted active downloads as paused when reopening the application.
    /// File and remote identity checks must follow before any segment is reused.
    pub fn recover_interrupted(&self) -> rusqlite::Result<usize> {
        self.conn.execute(
            "UPDATE downloads SET status='paused', updated_at=CURRENT_TIMESTAMP
             WHERE status='downloading'",
            [],
        )
    }

    pub fn delete(&self, id: i64) -> rusqlite::Result<bool> {
        Ok(self.conn.execute("DELETE FROM downloads WHERE id=?1", [id])? != 0)
    }
}

fn decode_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DownloadRecord> {
    Ok(DownloadRecord {
        id: row.get(0)?,
        url: row.get(1)?,
        destination: row.get(2)?,
        status: row.get(3)?,
        received_bytes: row.get(4)?,
        total_bytes: row.get(5)?,
        etag: row.get(6)?,
        last_modified: row.get(7)?,
        checkpoint_json: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> DownloadRecord {
        DownloadRecord {
            id: 1,
            url: "https://example.com/file".into(),
            destination: "file.zip".into(),
            status: "downloading".into(),
            received_bytes: 5,
            total_bytes: Some(10),
            etag: Some("\"abc\"".into()),
            last_modified: None,
            checkpoint_json: None,
        }
    }

    #[test]
    fn persistence_crud_and_recovery() {
        let db = DownloadDatabase::in_memory().unwrap();
        let mut item = record();
        db.insert(&item).unwrap();
        assert_eq!(db.find(1).unwrap(), Some(item.clone()));
        assert_eq!(db.recover_interrupted().unwrap(), 1);
        assert_eq!(db.find(1).unwrap().unwrap().status, "paused");
        item.status = "completed".into();
        item.received_bytes = 10;
        assert!(db.update(&item).unwrap());
        assert_eq!(db.list().unwrap(), vec![item]);
        assert!(db.delete(1).unwrap());
        assert!(db.list().unwrap().is_empty());
    }

    #[test]
    fn invalid_state_and_progress_rejected() {
        let db = DownloadDatabase::in_memory().unwrap();
        let mut item = record();
        item.status = "unknown".into();
        assert!(db.insert(&item).is_err());
        item.status = "queued".into();
        item.received_bytes = 20;
        assert!(db.insert(&item).is_err());
    }
}
