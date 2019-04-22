use ::{Point,Value};
use bincode::{serialize,deserialize};
use std::mem::size_of;
use random_access_storage::RandomAccess;
use failure::{Error,format_err,ensure,bail};
use std::marker::PhantomData;
use read_block::read_block;
use std::rc::Rc;
use std::cell::RefCell;
use lru::LruCache;

pub trait DataBatch<P,V> where P: Point, V: Value {
  fn batch (&mut self, &Vec<&(P,V)>) -> Result<u64,Error>;
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
  bbox_cache: LruCache<u64,(P::Bounds,u64)>,
  list_cache: LruCache<u64,Vec<(P,V)>>,
  pub max_data_size: usize,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> DataBatch<P,V> for DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    ensure![rows.len() <= self.max_data_size,
      "data size limit exceeded in data merge"];
    let mut data: Vec<u8> = vec![0;4];
    for row in rows.iter() {
      let buf = serialize(row)?;
      ensure_eq!(buf.len(), P::size_of() + size_of::<V>(),
        "unexpected length in data batch");
      data.extend(buf);
    }
    let len = data.len() as u32;
    data[0..4].copy_from_slice(&len.to_be_bytes());
    let offset = self.store.len()? as u64;
    self.store.write(offset as usize, &data)?;
    Ok(offset as u64)
  }
}

impl<S,P,V> DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S, max_data_size: usize, bbox_cache_size: usize,
  block_cache_size: usize, block_cache_count: usize) -> Result<Self,Error> {
    Ok(Self {
      store,
      bbox_cache: LruCache::new(bbox_cache_size),
      list_cache: LruCache::new(2_000),
      max_data_size,
      _marker: PhantomData
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
    }).map(|row| { *row }).collect())
  }
  pub fn list (&mut self, offset: u64) -> Result<Vec<(P,V)>,Error> {
    match self.list_cache.get(&offset) {
      Some(rows) => return Ok(rows.to_vec()),
      None => {}
    }
    let rows = Self::parse(&self.read(offset)?)?;
    self.list_cache.put(offset, rows);
    Ok(self.list_cache.peek(&offset).unwrap().to_vec())
  }
  pub fn parse (buf: &Vec<u8>) -> Result<Vec<(P,V)>,Error> {
    let size = P::size_of() + size_of::<V>();
    let mut results = vec![];
    for i in 0..buf.len()/size {
      results.push(deserialize(&buf[i*size..(i+1)*size])?);
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
    match self.bbox_cache.get(&offset) {
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
    self.bbox_cache.put(offset, result.clone());
    Ok(result)
  }
}
