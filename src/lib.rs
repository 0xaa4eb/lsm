//! An LSM-Tree (Log-Structured Merge Tree) implementation in Rust.
//! Provides a persistent key-value store with efficient write operations
//! by batching writes in memory before flushing to disk.

pub mod memtable;
pub mod sstable;

use thiserror::Error;
use crate::memtable::MemTable;
use crate::sstable::SSTable;

#[derive(Error, Debug)]
pub enum LSMError {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Key not found")]
    KeyNotFound,
}

pub type Result<T> = std::result::Result<T, LSMError>;

/// Configuration options for LSMTree
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum size of memtable in bytes before flushing to disk
    pub memtable_size_threshold: usize,
    /// Directory where SSTable files will be stored
    pub data_dir: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            memtable_size_threshold: 1024 * 1024, // 1MB default
            data_dir: "data".to_string(),
        }
    }
}

/// LSMTree is the main structure that coordinates MemTable and SSTables
pub struct LSMTree<K, V> {
    memtable: MemTable<K, V>,
    sstables: Vec<SSTable<K, V>>,
    sstable_id: u64,
    config: Config,
}

impl<K, V> LSMTree<K, V> 
where 
    K: Ord + serde::Serialize + serde::de::DeserializeOwned + Clone,
    V: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    /// Creates a new LSM Tree instance with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(Config::default())
    }

    /// Creates a new LSM Tree instance with custom configuration
    pub fn with_config(config: Config) -> Result<Self> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;
        
        Ok(LSMTree {
            memtable: MemTable::new(),
            sstables: Vec::new(),
            sstable_id: 0,
            config,
        })
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.memtable.put(key, value)?;
        
        if self.memtable.size() >= self.config.memtable_size_threshold {
            self.flush_memtable()?;
        }
        
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        // First check memtable
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.clone()));
        }

        // Then check SSTables from newest to oldest
        for sstable in self.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let old_memtable = std::mem::replace(&mut self.memtable, MemTable::new());
        let sstable_path = format!("{}/sstable_{:06}.db", self.config.data_dir, self.sstable_id);
        let new_sstable = SSTable::from_memtable(&old_memtable, sstable_path)?;

        self.sstables.push(new_sstable);
        self.sstable_id += 1;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;  // Add tempfile to your Cargo.toml

    fn setup() -> (LSMTree<String, String>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            memtable_size_threshold: 1024, // Small size for testing
            data_dir: temp_dir.path().to_str().unwrap().to_string(),
        };
        let lsm = LSMTree::with_config(config).unwrap();
        (lsm, temp_dir)
    }

    #[test]
    fn test_basic_operations() -> Result<()> {
        let (mut lsm, _temp_dir) = setup();

        // Test insert and get
        lsm.insert("key1".to_string(), "value1".to_string())?;
        assert_eq!(
            lsm.get(&"key1".to_string())?,
            Some("value1".to_string())
        );

        // Test non-existent key
        assert_eq!(lsm.get(&"nonexistent".to_string())?, None);

        // Test overwrite
        lsm.insert("key1".to_string(), "value2".to_string())?;
        assert_eq!(
            lsm.get(&"key1".to_string())?,
            Some("value2".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_memtable_flush() -> Result<()> {
        let (mut lsm, temp_dir) = setup();

        // Insert enough data to trigger a flush
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            lsm.insert(key, value)?;
        }

        // Verify that at least one SSTable was created
        let sstable_count = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter(|entry| {
                entry.as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .starts_with("sstable_")
            })
            .count();
        assert!(sstable_count > 0, "No SSTable files were created");

        // Verify we can still read all values
        for i in 0..100 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(
                lsm.get(&key)?,
                Some(expected_value),
                "Failed to read key {}",
                key
            );
        }

        Ok(())
    }

/*     #[test]
    fn test_persistence() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = Config {
            memtable_size_threshold: 1024,
            data_dir: temp_dir.path().to_str().unwrap().to_string(),
        };

        // Insert data and flush
        {
            let mut lsm = LSMTree::with_config(config.clone())?;
            lsm.insert("key1".to_string(), "value1".to_string())?;
            lsm.flush_memtable()?;
        }

        // Create new LSM tree instance and verify data
        {
            let lsm = LSMTree::with_config(config)?;
            assert_eq!(
                lsm.get(&"key1".to_string())?,
                Some("value1".to_string()),
                "Failed to recover data from SSTable"
            );
        }

        Ok(())
    } */

    #[test]
    fn test_large_dataset() -> Result<()> {
        let (mut lsm, _temp_dir) = setup();
        let num_entries = 10_000;

        // Insert a large number of entries
        for i in 0..num_entries {
            let key = format!("key{:05}", i);
            let value = format!("value{:05}", i);
            lsm.insert(key, value)?;
        }

        // Verify random access
        for i in (0..num_entries).step_by(100) {
            let key = format!("key{:05}", i);
            let expected_value = format!("value{:05}", i);
            assert_eq!(
                lsm.get(&key)?,
                Some(expected_value),
                "Failed to read key {}",
                key
            );
        }

        Ok(())
    }
}