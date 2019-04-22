use random_access_storage::RandomAccess;
use std::io::Write;
use lru::LruCache;
use std::collections::HashMap;

#[derive(Debug,Clone)]
struct Block {
  pub data: Vec<u8>,
  pub mask: Vec<u8>,
  pub missing: usize
}

impl Block {
  pub fn new (size: usize) -> Self {
    let n = (size+7)/8;
    Self {
      data: vec![0;size],
      mask: vec![0;n],
      missing: size
    }
  }
  pub fn from_data (data: Vec<u8>) -> Self {
    let n = (data.len()+7)/8;
    Self {
      data,
      mask: vec![0;n],
      missing: 0
    }
  }
  pub fn write (&mut self, offset: usize, data: &[u8]) -> () {
    self.data[offset..offset+data.len()].copy_from_slice(data);
    for i in offset..offset+data.len() {
      let m = (self.mask[i/8] >> (i%8)) & 1 == 1;
      if !m && self.missing > 0 { self.missing -= 1 }
      self.mask[i/8] |= 1<<(i%8);
    }
  }
  pub fn merge (&mut self, data: &[u8]) -> () {
    for i in 0..data.len() {
      let m = (self.mask[i/8] >> (i%8)) & 1 == 1;
      if !m {
        self.data[i] = data[i];
        self.missing -= 1;
      }
    }
  }
  pub fn has_range (&self, i: usize, j: usize) -> bool {
    if self.missing == 0 { return true }
    for k in i..j {
      if (self.mask[k/8] >> (k%8)) & 1 == 0 { return false }
    }
    true
  }
  pub fn writes (&self) -> Vec<(usize,&[u8])> {
    if self.missing == 0 {
      vec![(0,self.data.as_slice())]
    } else {
      let mut result = vec![];
      let mut offset = 0;
      let mut prev = false;
      for i in 0..self.data.len() {
        let m = (self.mask[i/8] >> (i%8)) & 1 == 1;
        if m && !prev {
          offset = i;
        } else if !m && prev {
          result.push((offset,&self.data[offset..i]));
        }
        prev = m;
      }
      if prev && offset < self.data.len() {
        result.push((offset,&self.data[offset..]));
      }
      result
    }
  }
}

//#[derive(Debug,Clone)]
pub struct BlockCache<S> where S: RandomAccess {
  store: S,
  size: usize,
  length: Option<u64>,
  reads: LruCache<u64,Block>,
  writes: HashMap<u64,Block>
}

impl<S> BlockCache<S> where S: RandomAccess {
  pub fn new (store: S, size: usize, count: usize) -> Self {
    Self {
      store,
      size,
      length: None,
      reads: LruCache::new(count),
      writes: HashMap::new()
    }
  }
}

impl<S> BlockCache<S> where S: RandomAccess {
  pub fn commit (&mut self) -> Result<(),S::Error> {
    let mut writes: Vec<(u64,Vec<u8>)> = vec![];
    let mut keys: Vec<u64> = self.writes.keys().map(|b| *b).collect();
    keys.sort_unstable();
    let len = self.len()? as u64;
    for b in keys {
      let mut block = self.writes.remove(&b).unwrap();
      for (i,slice) in block.writes() {
        if i == 0 && !writes.is_empty() {
          let should_push = {
            let last = writes.last_mut().unwrap();
            if last.0 + (last.1.len() as u64) == b {
              last.1.extend_from_slice(slice);
              false
            } else { true }
          };
          if should_push {
            writes.push(((i as u64)+b,slice.to_vec()));
          }
        } else {
          writes.push(((i as u64)+b,slice.to_vec()));
        }
      }
      if block.has_range(0, self.size) {
        self.reads.put(b, block);
      } else if block.has_range(0, ((len-b) as usize).min(self.size)) {
        block.merge(&vec![0;self.size]);
        self.reads.put(b, block);
      }
    }
    // TODO: analyze gaps and state of read cache in order to 
    // merge some nearby writes with the gap filled from the read cache
    for (offset,data) in writes {
      self.store.write(offset as usize, &data)?;
    }
    Ok(())
  }
}

impl<S> RandomAccess for BlockCache<S> where S: RandomAccess {
  type Error = S::Error;
  fn write (&mut self, offset: usize, data: &[u8]) -> Result<(),Self::Error> {
    let start = (offset/self.size) as u64;
    let end = ((offset+data.len()+self.size-1)/self.size) as u64;
    let mut d_start = 0;
    for i in start..end {
      let b = i * (self.size as u64);
      let b_start = ((offset as u64).max(b)-b) as usize;
      let b_len = (((offset+data.len()) as u64 - b) as usize)
        .min(self.size - b_start)
        .min(data.len());
      let b_end = b_start + b_len;
      let d_end = d_start + b_len;
      let slice = &data[d_start..d_end];
      d_start += b_len;
      let check_read = match self.writes.get_mut(&b) {
        Some(block) => {
          block.write(b_start, slice);
          false
        },
        None => true
      };
      if check_read {
        match self.reads.pop(&b) {
          Some(mut block) => {
            if !block.has_range(b_start, b_end) {
              panic!["read block does not have sufficient data"];
            }
            block.data[b_start..b_end].copy_from_slice(slice);
            self.writes.insert(b, block);
          },
          None => {
            let mut block = Block::new(self.size);
            block.write(b_start, slice);
            self.writes.insert(b, block);
          }
        }
      }
    }
    self.length = Some(match self.length {
      Some(len) => len.max((offset as u64) + (data.len() as u64)),
      None => (self.store.len()? as u64)
        .max((offset as u64) + (data.len() as u64))
    });
    Ok(())
  }
  fn read (&mut self, offset: usize, length: usize) ->
  Result<Vec<u8>,Self::Error> {
    let start = (offset/self.size) as u64;
    let end = ((offset+length+self.size-1)/self.size) as u64;
    let mut result: Vec<u8> = vec![0;length];
    let mut result_i = 0;
    let mut reads: Vec<(u64,(usize,usize),bool)> = vec![];
    for i in start..end {
      let b = i * (self.size as u64);
      let b_start = ((offset as u64).max(b)-b) as usize;
      let b_len = (((offset+length) as u64 - b) as usize)
        .min(self.size - b_start)
        .min(length);
      let b_end = b_start + b_len;
      let range = (result_i, result_i + b_len);
      result_i += b_len;
      match self.writes.get(&b) {
        Some(block) => {
          if block.has_range(b_start, b_end) {
            let slice = &block.data[b_start..b_end];
            result[range.0..range.1].copy_from_slice(slice);
          } else {
            reads.push((b,range,true));
          }
        },
        None => {
          match self.reads.get(&b) {
            Some(rblock) => {
              if !rblock.has_range(b_start, b_end) {
                panic!["read block does not have sufficient data"];
              }
              let slice = &rblock.data[b_start..b_end];
              result[range.0..range.1].copy_from_slice(slice);
            },
            None => { reads.push((b,range,false)) }
          }
        }
      };
    }
    if !reads.is_empty() {
      let len = self.store.len()? as u64;
      let i = reads[0].0.min(len);
      let j = (reads.last().unwrap().0 + (self.size as u64)).min(len);
      let data = if j > i {
        self.store.read(i as usize, (j-i) as usize)?
      } else { vec![] };

      let len = data.len();
      for (b,range,write) in reads {
        let d_start = ((b-i) as usize).min(len);
        let d_end = (d_start + self.size).min(len);
        let slice = &data[d_start..d_end];

        let b_start = ((offset as u64).max(b)-b) as usize;
        let b_len = (((offset+length) as u64 - b) as usize)
          .min(self.size - b_start)
          .min(length);
        let b_end = b_start + b_len;

        if write {
          match self.writes.get_mut(&b) {
            Some(block) => {
              block.merge(&slice);
              if !block.has_range(b_start, b_end) {
                panic!["write block {} does not have the necessary bytes: \
                  {}..{}", b, b_start, b_end
                ];
              }
              let bslice = &block.data[b_start..b_end];
              result[range.0..range.1].copy_from_slice(bslice);
            },
            None => {
              panic!["expected block in write cache at offset {}", b]
            }
          }
        } else {
          let block = if slice.len() < self.size {
            let mut b = Block::new(self.size);
            b.write(b_start, slice);
            b
          } else {
            Block::from_data(slice.to_vec())
          };
          if !block.has_range(b_start, b_end) {
            panic!["read block {} does not have the necessary bytes: {}..{}",
              b, b_start, b_end];
          }
          {
            let bslice = &block.data[b_start..b_end];
            result[range.0..range.1].copy_from_slice(bslice);
          }
          if block.has_range(0, self.size) {
            self.reads.put(b, block);
          }
        }
      }
    }
    assert_eq![result.len(), length, "correct result length"];
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
    if length == 0 {
      self.reads.clear();
      self.writes.clear();
    } else {
      let rkeys: Vec<u64> = self.reads.iter()
        .map(|(k,_)| *k)
        .filter(|b| *b >= length as u64)
        .collect();
      for b in rkeys {
        self.reads.pop(&b);
      }
      let wkeys: Vec<u64> = self.writes.keys()
        .map(|b| *b)
        .filter(|b| *b >= length as u64)
        .collect();
      for b in wkeys {
        self.writes.remove(&b);
      }
    }
    self.length = Some(length as u64);
    self.store.truncate(length)
  }
  fn len (&mut self) -> Result<usize,Self::Error> {
    Ok(match self.length {
      None => {
        let len = self.store.len()? as u64;
        self.length = Some(len);
        len
      },
      Some(len) => len
    } as usize)
  }
  fn is_empty (&mut self) -> Result<bool,Self::Error> {
    self.store.is_empty()
  }
}
