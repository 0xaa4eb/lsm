//! SSTable (Sorted String Table) implementation - the persistent storage component.
//! Provides immutable on-disk storage of sorted key-value pairs with a sparse index
//! for efficient lookups. Created when MemTable is flushed to disk.

use crate::memtable::MemTable;
use std::io::{Write, Seek};
use crate::Result;

#[derive(Debug)]
struct IndexEntry<K> {
    key: K,
    position: u64,
}

pub struct SSTable<K, V> {
    path: String,
    index: Vec<IndexEntry<K>>,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> SSTable<K, V>
where
    K: Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + Clone,
    V: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone,
{
    pub fn from_memtable(memtable: &MemTable<K, V>, path: String) -> Result<Self> {
        let file = std::fs::File::create(&path)?;
        let mut writer = std::io::BufWriter::new(file);
        let mut index = Vec::new();
        
        let entry_count = memtable.data.len() as u64;
        bincode::serialize_into(&mut writer, &entry_count)?;
        
        const INDEX_INTERVAL: u64 = 10;
        
        for (i, (key, value)) in memtable.iter().enumerate() {
            let position = writer.stream_position()?;
            
            if (i as u64) % INDEX_INTERVAL == 0 {
                index.push(IndexEntry {
                    key: key.clone(),
                    position,
                });
            }
            
            bincode::serialize_into(&mut writer, &key)?;
            bincode::serialize_into(&mut writer, &value)?;
        }
        
        writer.flush()?;
        
        Ok(Self {
            path,
            index,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn get(&self, search_key: &K) -> Result<Option<V>> {
        let file = std::fs::File::open(&self.path)?;
        let mut reader = std::io::BufReader::new(file);
        
        let entry_count: u64 = bincode::deserialize_from(&mut reader)?;
        
        let index_pos = match self.index.binary_search_by(|entry| entry.key.cmp(search_key)) {
            Ok(pos) => {
                reader.seek(std::io::SeekFrom::Start(self.index[pos].position))?;
                reader.seek(std::io::SeekFrom::Start(self.index[pos].position))?;
                let key: K = bincode::deserialize_from(&mut reader)?;
                let value: V = bincode::deserialize_from(&mut reader)?;
                return Ok(Some(value));
            }
            Err(pos) => {
                if pos == 0 {
                    // Key is before first index entry, we're already at the right position
                    // after reading entry_count
                    pos
                } else {
                    // Seek to the previous index entry
                    reader.seek(std::io::SeekFrom::Start(self.index[pos - 1].position))?;
                    pos - 1
                }
            }
        };
        
        loop {
            let position = match reader.stream_position() {
                Ok(pos) => pos,
                Err(_) => return Ok(None), // Handle IO error gracefully
            };
            
            if index_pos + 1 < self.index.len() && position >= self.index[index_pos + 1].position {
                return Ok(None);
            }
            
            let key: K = match bincode::deserialize_from(&mut reader) {
                Ok(k) => k,
                Err(_) => return Ok(None),
            };
            
            match key.cmp(search_key) {
                std::cmp::Ordering::Equal => {
                    // Also handle potential EOF or corruption when reading value
                    let value: V = match bincode::deserialize_from(&mut reader) {
                        Ok(v) => v,
                        Err(_) => return Ok(None),
                    };
                    return Ok(Some(value));
                }
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {
                    // Skip the value and continue searching
                    // Handle potential EOF or corruption when skipping value
                    match bincode::deserialize_from::<_, V>(&mut reader) {
                        Ok(_) => (),
                        Err(_) => return Ok(None),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sstable_basic_operations() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.sst").to_str().unwrap().to_string();
        
        let mut memtable = MemTable::new();
        memtable.put(5, "five".to_string())?;
        memtable.put(3, "three".to_string())?;
        memtable.put(7, "seven".to_string())?;
        memtable.put(1, "one".to_string())?;
        memtable.put(9, "nine".to_string())?;
        
        let sstable = SSTable::from_memtable(&memtable, path)?;
        
        assert_eq!(sstable.get(&1)?, Some("one".to_string()));
        assert_eq!(sstable.get(&3)?, Some("three".to_string()));
        assert_eq!(sstable.get(&5)?, Some("five".to_string()));
        assert_eq!(sstable.get(&7)?, Some("seven".to_string()));
        assert_eq!(sstable.get(&9)?, Some("nine".to_string()));
        
        assert_eq!(sstable.get(&0)?, None);
        assert_eq!(sstable.get(&2)?, None);
        assert_eq!(sstable.get(&6)?, None);
        assert_eq!(sstable.get(&8)?, None);
        assert_eq!(sstable.get(&10)?, None);
        
        Ok(())
    }

    #[test]
    fn test_sstable_large_dataset() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_large.sst").to_str().unwrap().to_string();
        
        let mut memtable = MemTable::new();
        for i in 0..1000 {
            memtable.put(i, format!("value_{}", i))?;
        }
        
        let sstable = SSTable::from_memtable(&memtable, path)?;
        
        assert_eq!(sstable.get(&42)?, Some("value_42".to_string()));
        assert_eq!(sstable.get(&999)?, Some("value_999".to_string()));
        assert_eq!(sstable.get(&500)?, Some("value_500".to_string()));
        
        assert_eq!(sstable.get(&0)?, Some("value_0".to_string()));
        assert_eq!(sstable.get(&999)?, Some("value_999".to_string()));
        assert_eq!(sstable.get(&1000)?, None);
        
        Ok(())
    }

    #[test]
    fn test_sstable_empty() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_empty.sst").to_str().unwrap().to_string();
        
        let memtable = MemTable::<i32, i32>::new();
        let sstable = SSTable::from_memtable(&memtable, path)?;
        
        assert_eq!(sstable.get(&1)?, None);
        Ok(())
    }

    #[test]
    fn test_sstable_string_keys() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_strings.sst").to_str().unwrap().to_string();
        
        let mut memtable = MemTable::<String, i32>::new();
        memtable.put("apple".to_string(), 1)?;
        memtable.put("banana".to_string(), 2)?;
        memtable.put("cherry".to_string(), 3)?;
        
        let sstable = SSTable::from_memtable(&memtable, path)?;
        
        assert_eq!(sstable.get(&"aaa".to_string())?, None);
        
        assert_eq!(sstable.get(&"apple".to_string())?, Some(1));
        assert_eq!(sstable.get(&"banana".to_string())?, Some(2));
        assert_eq!(sstable.get(&"cherry".to_string())?, Some(3));
        assert_eq!(sstable.get(&"date".to_string())?, None);
        
        Ok(())
    }
} 