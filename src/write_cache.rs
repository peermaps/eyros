use random_access_storage::RandomAccess;
use std::io::{Write};

#[derive(Debug,Clone)]
pub struct WriteCache<S> where S: RandomAccess {
  store: S,
  queue: Vec<(usize,Vec<u8>)>,
  length: usize
}

impl<S> WriteCache<S> where S: RandomAccess {
  pub fn new (mut store: S) -> Result<Self,S::Error> {
    let length = store.len()?;
    Ok(Self {
      store,
      queue: vec![],
      length
    })
  }
  pub fn flush (&mut self) -> Result<(),S::Error> {
    for q in self.queue.iter() {
      self.store.write(q.0, &q.1)?;
    }
    self.queue.clear();
    Ok(())
  }
}

impl<S> RandomAccess for WriteCache<S> where S: RandomAccess {
  type Error = S::Error;
  fn write (&mut self, offset: usize, data: &[u8]) -> Result<(),Self::Error> {
    let new_range = (offset,offset+data.len());
    let overlapping: Vec<usize> = (0..self.queue.len()).filter(|i| {
      let q = &self.queue[*i];
      overlaps(new_range, (q.0,q.0+(q.1).len()))
    }).collect();

    let mut start = new_range.0;
    let mut end = new_range.1;
    for i in overlapping.iter() {
      let q = &self.queue[*i];
      start = start.min(q.0);
      end = end.max(q.0 + (q.1).len());
    }
    let mut merged = (start,vec![0;end-start]);
    for i in overlapping.iter() {
      let q = &self.queue[*i];
      merged.1[q.0-start..q.0-start+q.1.len()]
        .copy_from_slice(&q.1.as_slice());
    }
    merged.1[new_range.0-start..new_range.0-start+data.len()]
      .copy_from_slice(data);
 
    for i in overlapping.iter() {
      self.queue.remove(overlapping[*i]-*i);
    }
    if overlapping.is_empty() {
      let mut j = 0;
      for i in 0..self.queue.len() {
        let q = &self.queue[i];
        if merged.0 < q.0 { break }
        j = i+1;
      }
      self.queue.insert(j, merged);
    } else {
      self.queue.insert(overlapping[0], merged);
    }
    self.length = self.length.max(end);
    Ok(())
  }
  fn read (&mut self, offset: usize, length: usize)
  -> Result<Vec<u8>,Self::Error> {
    self.store.read(offset, length)
  }
  fn read_to_writer (&mut self, _offset: usize, _length: usize,
  _buf: &mut impl Write) -> Result<(),Self::Error> {
    unimplemented![];
  }
  fn del (&mut self, offset: usize, length: usize) -> Result<(),Self::Error> {
    self.store.del(offset, length)
  }
  fn truncate (&mut self, length: usize) -> Result<(),Self::Error> {
    let mut i = 0;
    while i < self.queue.len() {
      let q0 = self.queue[i].0;
      let qlen = self.queue[i].1.len();
      if q0 < length {
        self.queue.remove(i);
      } else if q0 + qlen < length {
        self.queue[i].1.truncate(length - q0);
        i += 1;
      } else {
        i += 1;
      }
    }
    self.store.truncate(length)?;
    self.length = length;
    Ok(())
  }
  fn len (&mut self) -> Result<usize,Self::Error> {
    Ok(self.length)
  }
  fn is_empty (&mut self) -> Result<bool,Self::Error> {
    self.store.is_empty()
  }
}

fn overlaps<T> (a: (T,T), b: (T,T)) -> bool where T: PartialOrd {
  a.0 <= b.1 && b.0 <= a.1
}
