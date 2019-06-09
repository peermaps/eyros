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
      RandomAccessDisk::builder(dir.path().join("0"))
        .auto_sync(false)
        .build()?, 10, 20);
    store.write(0, &vec![97,98,5])?;
    store.write(2, &vec![99,100,101,102,103,104,105,106,107,108,109])?;
    assert_eq![store.read(8, 4)?, vec![105,106,107,108]];
    assert_eq![store.read(10, 3)?, vec![107,108,109]];
    assert_eq![store.read(0, 4)?, vec![97,98,99,100]];
  }
  {
    for i in 1..10 {
      let mut store = BlockCache::new(
        RandomAccessDisk::builder(dir.path().join(i.to_string()))
          .auto_sync(false)
          .build()?, 50, 40);
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
      assert_eq![store.len()?, size, "expected length"];
      for _ in 0..500 {
        let i = (r.read::<f64>()*(size as f64)) as usize;
        let len = (r.read::<f64>()*((size-i) as f64)) as usize;
        assert_eq![store.read(i,len)?, data[i..i+len].to_vec()];
      }
      assert_eq![store.read(8, 4)?, data[8..8+4].to_vec()];
      assert_eq![store.read(10, 3)?, data[10..10+3].to_vec()];
      assert_eq![store.read(0, 4)?, data[0..4].to_vec()];
      assert_eq![store.len()?, size, "expected length"];
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
      RandomAccessDisk::builder(dir.path().join(i.to_string()))
        .auto_sync(false)
        .build()?, 50, 40);
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
    assert_eq![store.len()?, size, "expected length"];
    store.commit()?;
    assert_eq![store.len()?, size, "expected length"];
    for _ in 0..500 {
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![store.read(i,len)?, data[i..i+len].to_vec()];
    }
    assert_eq![store.read(8, 4)?, data[8..8+4].to_vec()];
    assert_eq![store.read(10, 3)?, data[10..10+3].to_vec()];
    assert_eq![store.read(0, 4)?, data[0..4].to_vec()];
    assert_eq![store.len()?, size, "expected length"];
  }
  Ok(())
}

#[test]
fn block_cache_cold_read () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let mut store = RandomAccessDisk::builder(dir.path().join("20"))
    .auto_sync(false)
    .build()?;
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
    let mut store = RandomAccessDisk::builder(dir.path().join("20"))
      .auto_sync(false)
      .build()?;
    for _ in 0..size {
      data.push(r.read::<u8>());
    }
    store.write(0, &data)?;
    assert_eq![store.len()?, size, "expected length"];
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
      bstore.write(i, &chunk)?;
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
    assert_eq![bstore.read(0,size)?, data];
    assert_eq![bstore.len()?, size, "expected length"];
  }
  {
    let mut store = RandomAccessDisk::builder(dir.path().join("20"))
      .auto_sync(false)
      .builder()?;
    assert_eq![store.read(0,size)?, data];
    assert_eq![store.len()?, size, "expected length"];
  }
  Ok(())
}

#[test]
fn block_cache_append_low_count () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let file = dir.path().join("21");
  let mut store = BlockCache::new(RandomAccessDisk::builder(file)
    .auto_sync(false)
    .build()?, 50, 40);
  let mut expected: Vec<u8> = vec![];
  for _level in 0..50 {
    for _ in 0..100 {
      let mut chunk = vec![];
      let size = 1 + ((r.read::<f64>()*(4096 as f64)) as usize);
      for _ in 0..size {
        chunk.push(r.read::<u8>());
      }
      let offset = store.len()?;
      store.write(offset, &chunk)?;
      expected.extend(chunk);
      assert_eq![store.len()?, expected.len(), "post-write length check"];
    }
    assert_eq![store.len()?, expected.len(), "pre-commit length check"];
    store.commit()?;
    assert_eq![store.len()?, expected.len(), "post-commit length check"];
  }
  let len = store.len()?;
  assert_eq![len, expected.len(), "expected vs store length mismatch"];
  assert_eq![store.read(0,len)?, expected];
  Ok(())
}

#[test]
fn block_cache_append_high_count () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let file = dir.path().join("21");
  let mut store = BlockCache::new(RandomAccessDisk::builder(file)
    .auto_sync(false)
    .build()?, 50, 4000);
  let mut expected: Vec<u8> = vec![];
  for _level in 0..50 {
    for _ in 0..100 {
      let mut chunk = vec![];
      let size = 1 + ((r.read::<f64>()*(4096 as f64)) as usize);
      for _ in 0..size {
        chunk.push(r.read::<u8>());
      }
      let offset = store.len()?;
      store.write(offset, &chunk)?;
      expected.extend(chunk);
      assert_eq![store.len()?, expected.len(), "post-write length check"];
    }
    assert_eq![store.len()?, expected.len(), "pre-commit length check"];
    store.commit()?;
    assert_eq![store.len()?, expected.len(), "post-commit length check"];
  }
  let len = store.len()?;
  assert_eq![len, expected.len(), "expected vs store length mismatch"];
  assert_eq![store.read(0,len)?, expected];
  Ok(())
}

#[test]
fn block_cache_read_append_interleaved () -> Result<(),Error> {
  let mut r = rand().seed([13,12]);
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let file = dir.path().join("22");
  let mut store = BlockCache::new(RandomAccessDisk::builder(file)
    .auto_sync(false)
    .build()?, 500, 20);
  let mut data: Vec<u8> = vec![];
  for _ in 0..5_000 {
    let c = r.read::<f64>();
    if c < 0.01 {
      store.commit()?;
    } else if c < 0.4 {
      let i = store.len()?;
      let size = (r.read::<f64>()*(1000 as f64)) as usize;
      let mut chunk = vec![];
      for _ in 0..size {
        chunk.push(r.read::<u8>());
      }
      store.write(i, &chunk)?;
      data.extend_from_slice(&chunk);
    } else {
      let size = store.len()?;
      let i = (r.read::<f64>()*(size as f64)) as usize;
      let len = (r.read::<f64>()*((size-i) as f64)) as usize;
      assert_eq![store.read(i,len)?.as_slice(), &data[i..i+len],
        "read data {}..{}", i, i+len];
    }
    assert_eq![store.len()?, data.len(), "length"];
  }
  store.commit()?;
  let len = store.len()?;
  assert_eq![len, data.len(), "full length"];
  assert_eq![store.read(0,len)?, data, "full content"];
  Ok(())
}
