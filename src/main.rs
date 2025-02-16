//! A demo application showcasing basic LSM-Tree operations including inserts, 
//! retrievals, updates, and memtable flushing.

use lsm_tree::{Config, LSMTree};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        memtable_size_threshold: 4096,  // 4KB threshold
        data_dir: "demo_db".to_string(),
    };
    
    let mut lsm_tree = LSMTree::with_config(config)?;

    println!("Demonstrating LSM-Tree operations...\n");
    println!("Inserting data...");
    lsm_tree.insert("user:1".to_string(), "Alice".to_string())?;
    lsm_tree.insert("user:2".to_string(), "Bob".to_string())?;
    lsm_tree.insert("user:3".to_string(), "Charlie".to_string())?;

    if let Some(value) = lsm_tree.get(&"user:2".to_string())? {
        println!("Retrieved user:2 -> {}", value);
    }

    println!("\nUpdating user:2's value...");
    lsm_tree.insert("user:2".to_string(), "Bobby".to_string())?;
    
    if let Some(value) = lsm_tree.get(&"user:2".to_string())? {
        println!("Retrieved updated user:2 -> {}", value);
    }

    println!("\nInserting many entries to trigger memtable flush...");
    for i in 0..1000 {
        let key = format!("bulk:key:{}", i);
        let value = format!("value:{}", i);
        lsm_tree.insert(key, value)?;
    }

    if let Some(value) = lsm_tree.get(&"user:1".to_string())? {
        println!("Can still read old data - user:1 -> {}", value);
    }
    
    if let Some(value) = lsm_tree.get(&"bulk:key:500".to_string())? {
        println!("Can read new data - bulk:key:500 -> {}", value);
    }

    Ok(())
}
