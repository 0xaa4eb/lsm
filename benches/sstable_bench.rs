//! Benchmarks for SSTable operations including creation and reads.
//! Tests performance with different dataset sizes and access patterns.

#![feature(test)]

extern crate test;
use test::Bencher;

use lsm_tree::{memtable::MemTable, sstable::SSTable, Result};
use tempfile::tempdir;

#[bench]
fn bench_sstable_creation_10k(b: &mut Bencher) -> Result<()> {
    let dir = tempdir()?;

    let mut memtable = MemTable::new();
    for i in 0..10_000 {
        memtable.put(i, format!("value_{}", i))?;
    }
    
    b.iter(|| {
        let path = dir.path().join("bench_create.sst").to_str().unwrap().to_string();
        SSTable::from_memtable(&memtable, path).unwrap()
    });
    
    Ok(())
}

#[bench]
fn bench_sstable_random_reads(b: &mut Bencher) -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bench_read.sst").to_str().unwrap().to_string();

    let mut memtable = MemTable::new();
    for i in 0..10_000 {
        memtable.put(i, format!("value_{}", i))?;
    }
    let sstable = SSTable::from_memtable(&memtable, path)?;

    use rand::Rng;
    let mut rng = rand::thread_rng();
    
    b.iter(|| {
        let key = rng.gen_range(0..10_000);
        sstable.get(&key).unwrap()
    });
    
    Ok(())
}

#[bench]
fn bench_sstable_sequential_reads(b: &mut Bencher) -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("bench_seq_read.sst").to_str().unwrap().to_string();

    let mut memtable = MemTable::new();
    for i in 0..10_000 {
        memtable.put(i, format!("value_{}", i))?;
    }
    let sstable = SSTable::from_memtable(&memtable, path)?;
    
    let mut i = 0;
    b.iter(|| {
        let key = i % 10_000;
        i += 1;
        sstable.get(&key).unwrap()
    });
    
    Ok(())
}

#[bench]
fn bench_sstable_creation_100k(b: &mut Bencher) -> Result<()> {
    let dir = tempdir()?;

    let mut memtable = MemTable::new();
    for i in 0..100_000 {
        memtable.put(i, format!("value_{}", i))?;
    }
    
    b.iter(|| {
        let path = dir.path().join("bench_create_large.sst").to_str().unwrap().to_string();
        SSTable::from_memtable(&memtable, path).unwrap()
    });
    
    Ok(())
} 