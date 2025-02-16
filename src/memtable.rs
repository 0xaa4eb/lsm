//! MemTable (Memory Table) implementation.
//!
//! A MemTable is the in-memory component of the LSM tree that stores key-value pairs in sorted order
//! using a BTreeMap. It accumulates writes until it reaches a size threshold, at which point it is
//! flushed to disk as an SSTable. The size tracking is done by estimating the serialized size of
//! entries using bincode.

use std::collections::BTreeMap;
use crate::Result;

pub struct MemTable<K, V> {
    pub(crate) data: BTreeMap<K, V>,
    size_bytes: usize,
}

impl<K, V> MemTable<K, V>
where
    K: Ord + serde::Serialize + Clone,
    V: serde::Serialize + Clone,
{
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<usize> {
        let key_size = bincode::serialized_size(&key)? as usize;
        let value_size = bincode::serialized_size(&value)? as usize;
        
        self.data.insert(key, value);
        self.size_bytes += key_size + value_size;
        
        Ok(key_size + value_size)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    pub fn size(&self) -> usize {
        self.size_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.data.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic_operations() -> Result<()> {
        let mut table = MemTable::<i32, String>::new();
        
        let mut total_size = 0;
        total_size += table.put(1, "one".to_string())?;
        total_size += table.put(2, "two".to_string())?;
        total_size += table.put(3, "three".to_string())?;
        
        assert_eq!(table.get(&1), Some(&"one".to_string()));
        assert_eq!(table.get(&2), Some(&"two".to_string()));
        assert_eq!(table.get(&3), Some(&"three".to_string()));
        assert_eq!(table.get(&4), None);
        
        total_size += table.put(2, "TWO".to_string())?;
        assert_eq!(table.get(&2), Some(&"TWO".to_string()));
        
        assert_eq!(table.size(), total_size);
        Ok(())
    }

    #[test]
    fn test_memtable_ordering() -> Result<()> {
        let mut table = MemTable::new();
        
        table.put(5, "five".to_string())?;
        table.put(3, "three".to_string())?;
        table.put(1, "one".to_string())?;
        table.put(4, "four".to_string())?;
        table.put(2, "two".to_string())?;
        
        let mut iter = table.iter();
        assert_eq!(iter.next(), Some((&1, &"one".to_string())));
        assert_eq!(iter.next(), Some((&2, &"two".to_string())));
        assert_eq!(iter.next(), Some((&3, &"three".to_string())));
        assert_eq!(iter.next(), Some((&4, &"four".to_string())));
        assert_eq!(iter.next(), Some((&5, &"five".to_string())));
        assert_eq!(iter.next(), None);
        
        Ok(())
    }

    #[test]
    fn test_memtable_empty() {
        let table: MemTable<i32, String> = MemTable::new();
        
        assert_eq!(table.get(&1), None);
        assert_eq!(table.size(), 0);
        assert!(table.iter().next().is_none());
    }

    #[test]
    fn test_memtable_string_keys() -> Result<()> {
        let mut table = MemTable::new();
        
        table.put("apple".to_string(), 1)?;
        table.put("banana".to_string(), 2)?;
        table.put("cherry".to_string(), 3)?;
        
        assert_eq!(table.get(&"apple".to_string()), Some(&1));
        assert_eq!(table.get(&"banana".to_string()), Some(&2));
        assert_eq!(table.get(&"cherry".to_string()), Some(&3));
        assert_eq!(table.get(&"date".to_string()), None);
        
        let mut iter = table.iter();
        assert_eq!(iter.next(), Some((&"apple".to_string(), &1)));
        assert_eq!(iter.next(), Some((&"banana".to_string(), &2)));
        assert_eq!(iter.next(), Some((&"cherry".to_string(), &3)));
        assert_eq!(iter.next(), None);
        
        Ok(())
    }

    #[test]
    fn test_memtable_large_dataset() -> Result<()> {
        let mut table = MemTable::new();
        
        for i in 0..1000 {
            table.put(i, format!("value_{}", i))?;
        }
        
        assert_eq!(table.size(), 20890);
        
        assert_eq!(table.get(&42), Some(&"value_42".to_string()));
        assert_eq!(table.get(&999), Some(&"value_999".to_string()));
        assert_eq!(table.get(&500), Some(&"value_500".to_string()));
        
        assert_eq!(table.get(&0), Some(&"value_0".to_string()));
        assert_eq!(table.get(&999), Some(&"value_999".to_string()));
        assert_eq!(table.get(&1000), None);
        
        let mut prev = -1;
        for (k, _) in table.iter() {
            assert!(*k > prev);
            prev = *k;
        }
        
        Ok(())
    }

    #[test]
    fn test_memtable_custom_type() -> Result<()> {
        #[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, serde::Serialize)]
        struct CustomKey {
            primary: i32,
            secondary: String,
        }
        
        let mut table = MemTable::<CustomKey, String>::new();
        
        let key1 = CustomKey {
            primary: 1,
            secondary: "a".to_string(),
        };
        let key2 = CustomKey {
            primary: 1,
            secondary: "b".to_string(),
        };
        let key3 = CustomKey {
            primary: 2,
            secondary: "a".to_string(),
        };
        
        table.put(key1.clone(), "value1".to_string())?;
        table.put(key2.clone(), "value2".to_string())?;
        table.put(key3.clone(), "value3".to_string())?;
        
        assert_eq!(table.get(&key1), Some(&"value1".to_string()));
        assert_eq!(table.get(&key2), Some(&"value2".to_string()));
        assert_eq!(table.get(&key3), Some(&"value3".to_string()));
        
        let mut iter = table.iter();
        assert_eq!(iter.next(), Some((&key1, &"value1".to_string())));
        assert_eq!(iter.next(), Some((&key2, &"value2".to_string())));
        assert_eq!(iter.next(), Some((&key3, &"value3".to_string())));
        assert_eq!(iter.next(), None);
        
        Ok(())
    }
} 