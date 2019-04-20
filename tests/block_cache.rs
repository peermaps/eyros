extern crate random_access_storage;
extern crate random_access_disk;
extern crate tempfile;
extern crate failure;
extern crate lru;
extern crate random;

#[path="../src/block_cache.rs"]
mod block_cache;

use block_cache::BlockCache;
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;
use tempfile::Builder as Tmpfile;
use failure::Error;
use random::{Source, default as rand};

#[test]
fn block_cache_full_write () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  {
    let mut store = BlockCache::new(
      RandomAccessDisk::open(dir.path().join("0"))?, 10, 20);
    store.write(0, &vec![97,98,5])?;
    store.write(2, &vec![99,100,101,102,103,104,105,106,107,108,109])?;
    assert_eq![store.read(8, 4)?, vec![105,106,107,108]];
    assert_eq![store.read(10, 3)?, vec![107,108,109]];
    assert_eq![store.read(0, 4)?, vec![97,98,99,100]];
  }
  {
    for i in 1..10 {
      let mut store = BlockCache::new(
        RandomAccessDisk::open(dir.path().join(i.to_string()))?, 50, 40);
      let size = 1_000;
      let mut data = vec![];
      for _ in 0..size {
        data.push(r.read::<u8>());
      }
      let mut mask = vec![false;size];
      let mut covered = 0;
      while covered < size {
        let i = (r.read::<f64>()*(size as f64)) as usize;
        let j = i + ((r.read::<f64>()*((size-i+1) as f64)) as usize);
        store.write(i,&data[i..j])?;
        for k in i..j {
          if !mask[k] { covered += 1 }
          mask[k] = true;
        }
      }
      for _ in 0..500 {
        let i = (r.read::<f64>()*(size as f64)) as usize;
        let len = (r.read::<f64>()*((size-i) as f64)) as usize;
        assert_eq![store.read(i,len)?, data[i..i+len].to_vec()];
      }
      assert_eq![store.read(8, 4)?, data[8..8+4].to_vec()];
      assert_eq![store.read(10, 3)?, data[10..10+3].to_vec()];
      assert_eq![store.read(0, 4)?, data[0..4].to_vec()];
    }
  }
  Ok(())
}

#[test]
fn block_cache_read_write_commit () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  for i in 10..20 {
    let mut store = BlockCache::new(
      RandomAccessDisk::open(dir.path().join(i.to_string()))?, 50, 40);
    let size = 1_000;
    let mut data = vec![];
    for _ in 0..size {
      data.push(r.read::<u8>());
    }
    let mut mask = vec![false;size];
    let mut covered = 0;
    while covered < size {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let j = i + ((r.read::<f64>()*((size-i+1) as f64)) as usize);
      store.write(i,&data[i..j])?;
      for k in i..j {
        if !mask[k] { covered += 1 }
        mask[k] = true;
      }
    }
    store.commit()?;
    for _ in 0..500 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![store.read(i,len)?, data[i..i+len].to_vec()];
    }
    assert_eq![store.read(8, 4)?, data[8..8+4].to_vec()];
    assert_eq![store.read(10, 3)?, data[10..10+3].to_vec()];
    assert_eq![store.read(0, 4)?, data[0..4].to_vec()];
  }
  Ok(())
}

#[test]
fn block_cache_cold_read () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let mut store = RandomAccessDisk::open(dir.path().join("20"))?;
  let size = 1_000;
  let mut data = vec![];
  for _ in 0..size {
    data.push(r.read::<u8>());
  }
  store.write(0, &data)?;
  let mut bstore = BlockCache::new(store, 50, 20);
  for _ in 0..500 {
    let i = (r.read::<f64>()*(size as f64)) as usize;
    let len = (r.read::<f64>()*((size-i) as f64)) as usize;
    assert_eq![bstore.read(i,len)?, data[i..i+len].to_vec()];
  }
  Ok(())
}

#[test]
fn block_cache_cold_read_write_read () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let size = 1_000;
  let mut data = vec![];
  {
    let mut store = RandomAccessDisk::open(dir.path().join("20"))?;
    for _ in 0..size {
      data.push(r.read::<u8>());
    }
    store.write(0, &data)?;
    let mut bstore = BlockCache::new(store, 50, 20);
    for _ in 0..500 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![bstore.read(i,len)?, data[i..i+len].to_vec()];
    }
    for _ in 0..20 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = ((r.read::<f64>()*((size-i) as f64)) as usize).min(30);
      let mut chunk = vec![];
      for _ in 0..len {
        chunk.push(r.read::<u8>());
      }
      data[i..i+len].copy_from_slice(&chunk);
      bstore.write(i, &data)?;
    }
    for _ in 0..500 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![bstore.read(i,len)?, data[i..i+len].to_vec()];
    }
    bstore.commit()?;
    for _ in 0..500 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![bstore.read(i,len)?, data[i..i+len].to_vec()];
    }
  }
  {
    let mut store = RandomAccessDisk::open(dir.path().join("20"))?;
    assert_eq![store.read(0,size)?, data];
  }
  Ok(())
}
