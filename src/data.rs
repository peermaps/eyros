use crate::{Point,Value,read_block::read_block};
use random_access_storage::RandomAccess;
use failure::{Error,ensure,bail};
use std::rc::Rc;
use std::cell::RefCell;
use lru::LruCache;

pub trait DataBatch<P,V> where P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error>;
}

pub struct DataMerge<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  data_store: Rc<RefCell<DataStore<S,P,V>>>
}

impl<S,P,V> DataMerge<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (data_store: Rc<RefCell<DataStore<S,P,V>>>) -> Self {
    Self { data_store }
  }
}

impl<S,P,V> DataBatch<P::Range,u64> for DataMerge<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P::Range,u64)>) -> Result<u64,Error> {
    if rows.len() == 1 { // use existing address
      Ok(rows[0].1)
    } else { // combine addresses into a new block
      let mut dstore = self.data_store.try_borrow_mut()?;
      let max = dstore.max_data_size;
      let mut combined = vec![];
      for row in rows {
        combined.extend(dstore.list(row.1)?);
      }
      ensure![combined.len() <= max, "data size limit exceeded in data merge"];
      dstore.batch(&combined.iter().map(|c| c).collect())
    }
  }
}

//#[derive(Debug,Clone)]
pub struct DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  bounds: DataBounds<S,P>,
  list_cache: LruCache<u64,Vec<(P,V)>>,
  pub max_data_size: usize,
  pub bincode: Rc<bincode::Config>
}

impl<S,P,V> DataBatch<P,V> for DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    ensure![rows.len() <= self.max_data_size,
      "data size limit exceeded in data merge"];
    let mut data: Vec<u8> = vec![0;4];
    for row in rows.iter() {
      let buf = self.bincode.serialize(row)?;
      //ensure_eq!(buf.len(), P::size_of() + size_of::<V>(),
      //  "unexpected length in data batch");
      data.extend(buf);
    }
    let len = data.len() as u32;
    data[0..4].copy_from_slice(&len.to_be_bytes());
    let offset = self.store.len()? as u64;
    self.store.write(offset as usize, &data)?;
    let bbox = match P::bounds(&rows.iter().map(|(p,_)| *p).collect()) {
      None => bail!["invalid data at offset {}", offset],
      Some(bbox) => bbox
    };
    self.bounds.write(&(offset,P::bounds_to_range(bbox),rows.len() as u64))?;
    Ok(offset as u64)
  }
}

impl<S,P,V> DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S, bbox_store: S,
  max_data_size: usize, bbox_cache_size: usize,
  list_cache_size: usize, bincode: Rc<bincode::Config>) -> Result<Self,Error> {
    Ok(Self {
      store,
      bounds: DataBounds::new(
        bbox_store, bbox_cache_size, Rc::clone(&bincode)
      ),
      list_cache: LruCache::new(list_cache_size),
      max_data_size,
      bincode
    })
  }
  pub fn commit (&mut self) -> Result<(),Error> {
    //self.store.commit()
    Ok(())
  }
  pub fn query (&mut self, offset: u64, bbox: &P::Bounds)
  -> Result<Vec<(P,V)>,Error> {
    let rows = self.list(offset)?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).map(|row| { row.clone() }).collect())
  }
  pub fn list (&mut self, offset: u64) -> Result<Vec<(P,V)>,Error> {
    match self.list_cache.get(&offset) {
      Some(rows) => return Ok(rows.to_vec()),
      None => {}
    }
    let buf = self.read(offset)?;
    let rows = self.parse(&buf)?;
    self.list_cache.put(offset, rows);
    Ok(self.list_cache.peek(&offset).unwrap().to_vec())
  }
  pub fn parse (&self, buf: &Vec<u8>) -> Result<Vec<(P,V)>,Error> {
    let mut results = vec![];
    let mut offset = 0;
    while offset < buf.len() {
      let psize = P::size_of();
      let vsize = V::take_bytes(offset+psize, &buf);
      let n = psize + vsize;
      results.push(self.bincode.deserialize(&buf[offset..offset+n])?);
      offset += n;
    }
    Ok(results)
  }
  pub fn read (&mut self, offset: u64) -> Result<Vec<u8>,Error> {
    let len = self.store.len()? as u64;
    read_block(&mut self.store, offset, len, 1024)
  }
  pub fn bytes (&mut self) -> Result<u64,Error> {
    Ok(self.store.len()? as u64)
  }
  pub fn bbox (&mut self, offset: u64) -> Result<(P::Bounds,u64),Error> {
    match self.bounds.cache.get(&offset) {
      None => {},
      Some(r) => return Ok(*r)
    };
    let rows = self.list(offset)?;
    if rows.is_empty() {
      bail!["empty data block"]
    }
    let bbox = match P::bounds(&rows.iter().map(|(p,_)| *p).collect()) {
      None => bail!["invalid data at offset {}", offset],
      Some(bbox) => bbox
    };
    let result = (bbox,rows.len() as u64);
    self.bounds.cache.put(offset, result.clone());
    Ok(result)
  }
}

pub struct DataBounds<S,P>
where S: RandomAccess<Error=Error>, P: Point {
  pub store: S,
  pub cache: LruCache<u64,(P::Bounds,u64)>,
  bincode: Rc<bincode::Config>
}

impl<S,P> DataBounds<S,P>
where S: RandomAccess<Error=Error>, P: Point {
  pub fn new (store: S, cache_size: usize, bincode: Rc<bincode::Config>) -> Self {
    Self {
      store,
      bincode,
      cache: LruCache::new(cache_size)
    }
  }
  pub fn write (&mut self, b: &(u64,P::Range,u64)) -> Result<(),Error> {
    let data = self.bincode.serialize(b)?;
    let offset = self.store.len()?;
    self.store.write(offset, &data)
  }
  pub fn list (&mut self) -> Result<Vec<(u64,P,u64)>,Error> {
    let mut results = vec![];
    let bsize = <P::Range as Point>::size_of();
    let n = 8 + bsize + 8;
    let len = self.store.len()?;
    let buf_size = {
      let x = 1024 * 1024;
      x - (x % n) // ensure buf_size is a multiple of n
    };
    for j in 0..(len+buf_size-1)/buf_size {
      let buf = self.store.read(j*buf_size,((j+1)*buf_size).min(len))?;
      for i in 0..buf.len()/n {
        let offset = i * n;
        results.push(self.bincode.deserialize(&buf[offset..offset+n])?);
      }
    }
    Ok(results)
  }
}
