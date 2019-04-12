use random_access_storage::RandomAccess;
use std::io::Write;

#[derive(Debug,Clone)]
pub struct BlockCache<S> where S: RandomAccess {
  store: S,
  size: usize,
  count: usize
}

impl<S> BlockCache<S> where S: RandomAccess {
  pub fn new (store: S, size: usize, count: usize) -> Self {
    Self {
      store,
      size,
      count
    }
  }
}

impl<S> RandomAccess for BlockCache<S> where S: RandomAccess {
  type Error = S::Error;
  fn write (&mut self, offset: usize, data: &[u8]) -> Result<(),Self::Error> {
    self.store.write(offset, data)
  }
  fn read (&mut self, offset: usize, length: usize) ->
  Result<Vec<u8>,Self::Error> {
    self.store.read(offset, length)
  }
  fn read_to_writer (&mut self, _offset: usize, _length: usize,
  _buf: &mut impl Write) -> Result<(),Self::Error> {
    unimplemented![]
  }
  fn del (&mut self, offset: usize, length: usize) -> Result<(),Self::Error> {
    self.store.del(offset, length)
  }
  fn truncate (&mut self, length: usize) -> Result<(),Self::Error> {
    self.store.truncate(length)
  }
  fn len (&mut self) -> Result<usize,Self::Error> {
    self.store.len()
  }
  fn is_empty (&mut self) -> Result<bool,Self::Error> {
    self.store.is_empty()
  }
}
