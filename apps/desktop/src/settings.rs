//! Durable desktop transfer settings, validated before use.
use serde::{Deserialize, Serialize};
use std::{fs, io, path::Path};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferSettings {
    pub concurrency: i32,
    pub global_limit_kib: i32,
}
impl Default for TransferSettings {
    fn default() -> Self { Self { concurrency: 3, global_limit_kib: 0 } }
}
impl TransferSettings {
    pub fn validate(self) -> io::Result<Self> {
        if !(1..=16).contains(&self.concurrency) || !(0..=1_048_576).contains(&self.global_limit_kib) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid transfer settings"));
        }
        Ok(self)
    }
    pub fn load(path: &Path) -> io::Result<Self> {
        match fs::read(path) {
            Ok(data) => serde_json::from_slice::<Self>(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?.validate(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error),
        }
    }
    pub fn save(self, path: &Path) -> io::Result<()> {
        self.validate()?;
        let temp = path.with_extension("json.tmp");
        let data = serde_json::to_vec_pretty(&self).map_err(io::Error::other)?;
        {
            use std::io::Write;
            let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&temp)?;
            if let Err(error) = file.write_all(&data).and_then(|_| file.sync_all()) {
                let _ = fs::remove_file(&temp);
                return Err(error);
            }
        }
        if let Err(error) = fs::rename(&temp, path) {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn corrupted_settings_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings.json");
        fs::write(&path, b"{invalid").unwrap();
        assert_eq!(TransferSettings::load(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
        fs::write(&path, br#"{"concurrency":64,"global_limit_kib":0}"#).unwrap();
        assert_eq!(TransferSettings::load(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn settings_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("settings.json");
        assert_eq!(TransferSettings::load(&path).unwrap(), TransferSettings::default());
        let value = TransferSettings { concurrency: 5, global_limit_kib: 2048 };
        value.save(&path).unwrap();
        assert_eq!(TransferSettings::load(&path).unwrap(), value);
        assert!(TransferSettings { concurrency: 0, global_limit_kib: 0 }.save(&path).is_err());
    }
}
