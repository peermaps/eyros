use ::{Row,Point,Value};
use bincode::{serialize,deserialize};
use std::mem::size_of;
use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use read_block::read_block;

#[derive(Debug,Clone,Copy)]
pub struct DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S) -> Result<Self,Error> {
    Ok(Self { store, _marker: PhantomData })
  }
  pub fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    let mut data: Vec<u8> = vec![0;4];
    for row in rows.iter() {
      data.extend(serialize(row)?);
    }
    let len = (data.len()-4) as u32;
    data[0..4].copy_from_slice(&len.to_be_bytes());
    let offset = self.store.len()?;
    self.store.write(offset, &data)?;
    Ok(offset as u64)
  }
  pub fn query (&mut self, offset: u64, bbox: &P::BBox)
  -> Result<Vec<(P,V)>,Error> {
    let rows = Self::parse(&self.read(offset)?)?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).map(|row| { *row }).collect())
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
  pub fn len (&mut self) -> Result<u64,Error> {
    Ok(self.store.len()? as u64)
  }
}
