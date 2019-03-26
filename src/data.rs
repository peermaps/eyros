use ::{Point,Value};
use write_cache::WriteCache;
use bincode::{serialize,deserialize};
use std::mem::size_of;
use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use read_block::read_block;
use std::rc::Rc;
use std::cell::RefCell;

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
      // TODO: this function should return a Vec<u64>
      // of blocks with at most max_data_size records
      let mut dstore = self.data_store.try_borrow_mut()?;
      let mut combined = vec![];
      for row in rows {
        combined.extend(dstore.list(row.1)?);
      }
      dstore.batch(&combined.iter().map(|c| c).collect())
    }
  }
}

#[derive(Debug,Clone)]
pub struct DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: WriteCache<S>,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> DataBatch<P,V> for DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    let mut data: Vec<u8> = vec![0;4];
    for row in rows.iter() {
      data.extend(serialize(row)?);
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
  pub fn open (store: S) -> Result<Self,Error> {
    Ok(Self {
      store: WriteCache::open(store)?,
      _marker: PhantomData
    })
  }
  pub fn flush (&mut self) -> Result<(),Error> {
    self.store.flush()
  }
  pub fn query (&mut self, offset: u64, bbox: &P::Bounds)
  -> Result<Vec<(P,V)>,Error> {
    let rows = self.list(offset)?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).map(|row| { *row }).collect())
  }
  pub fn list (&mut self, offset: u64) -> Result<Vec<(P,V)>,Error> {
    Self::parse(&self.read(offset)?)
  }
  pub fn parse (buf: &Vec<u8>) -> Result<Vec<(P,V)>,Error> {
    let size = size_of::<P>() + size_of::<V>();
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
}
