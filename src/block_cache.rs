use random_access_storage::RandomAccess;
use std::io::Write;
use lru::LruCache;

struct Block {
  pub data: Vec<u8>,
  pub mask: Vec<u8>
}

impl Block {
  pub fn new (size: usize) -> Self {
    let n = (size+7)/8;
    Self { data: vec![0;size], mask: vec![0;n] }
  }
  pub fn from_data (data: Vec<u8>) -> Self {
    let n = (data.len()+7)/8;
    Self { data, mask: vec![0;n] }
  }
  pub fn write (&mut self, offset: usize, data: &[u8]) -> () {
    eprintln!["WRITE {} {}", offset, data.len()];
    self.data[offset..offset+data.len()].copy_from_slice(data);
    for i in offset..offset+data.len() {
      self.mask[i/8] |= 1<<(i%8);
    }
  }
  pub fn commit<S> (&mut self, store: &mut S, offset: u64)
  -> Result<(),S::Error> where S: RandomAccess {
    store.write(offset as usize, &self.data)?;
    Ok(())
  }
}

//#[derive(Debug,Clone)]
pub struct BlockCache<S> where S: RandomAccess {
  store: S,
  size: usize,
  count: usize,
  cache: LruCache<u64,Block>,
  write_queue: Vec<u64>
}

impl<S> BlockCache<S> where S: RandomAccess {
  pub fn new (store: S, size: usize, count: usize) -> Self {
    Self {
      store,
      size,
      count,
      cache: LruCache::new(count),
      write_queue: vec![]
    }
  }
}

impl<S> RandomAccess for BlockCache<S> where S: RandomAccess {
  type Error = S::Error;
  fn write (&mut self, offset: usize, data: &[u8]) -> Result<(),Self::Error> {
    let start = (offset/self.size) as u64;
    let end = ((offset+data.len()+self.size-1)/self.size) as u64;
    let mut d_start = 0;
    for (j,i) in (start..end).enumerate() {
      let b = i * (self.size as u64);
      let b_start = ((offset as u64).max(b)-b) as usize;
      let b_len = (((offset+data.len()) as u64 - b) as usize)
        .min(self.size - b_start);
      let b_end = b_start + b_len;
      let d_end = d_start + b_len;
      let slice = &data[d_start..d_end];
      d_start += b_len;
      let none = match self.cache.get_mut(&b) {
        Some(block) => {
          block.write(b_start, slice);
          false
        },
        None => { true }
      };
      if none {
        let mut block = Block::new(self.size);
        block.write(((offset as u64).max(b)-b) as usize, slice);
        self.cache.put(b, block);
      }
    }
    Ok(())
  }
  fn read (&mut self, offset: usize, length: usize) ->
  Result<Vec<u8>,Self::Error> {
    let start = (offset/self.size) as u64;
    let end = ((offset+length+self.size-1)/self.size) as u64;
    let mut result: Vec<u8> = vec![];
    for i in start..end {
      let b = i * (self.size as u64);
      let b_start = ((offset as u64).max(b)-b) as usize;
      let b_len = (((offset+length) as u64 - b) as usize)
        .min(self.size - b_start);
      let b_end = b_start + b_len;
      let none = match self.cache.get(&b) {
        Some(block) => {
          // TODO: check write mask
          eprintln!["block.data={:?}", block.data];
          result.extend_from_slice(&block.data[b_start..b_end]);
          false
        },
        None => { true }
      };
      if none {
        let data = self.store.read(b as usize, length)?;
        result.extend_from_slice(&data[b_start..b_end]);
        let block = Block::from_data(data);
        self.cache.put(b, block);
      }
    }
    Ok(result)
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
