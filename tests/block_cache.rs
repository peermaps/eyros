extern crate random_access_storage;
extern crate random_access_disk;
extern crate tempfile;
extern crate failure;
extern crate lru;

#[path="../src/block_cache.rs"]
mod block_cache;

use block_cache::BlockCache;
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;
use tempfile::Builder as Tmpfile;
use failure::Error;

#[test]
fn block_cache () -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros-block-cache").tempdir()?;
  let mut store = BlockCache::new(
    RandomAccessDisk::open(dir.path().join("0"))?, 10, 20);
  store.write(0, &vec![97,98,5])?;
  store.write(2, &vec![99,100,101,102,103,104,105,106,107,108,109])?;
  assert_eq![store.read(8, 4)?, vec![105,106,107,108]];
  assert_eq![store.read(10, 3)?, vec![107,108,109]];
  assert_eq![store.read(0, 4)?, vec![97,98,99,100]];
  Ok(())
}
