use ::{Row,Point,Value};
use bincode::{serialize,deserialize};
use std::mem::size_of;

pub struct Data<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S
}

impl<S,P,V> Data<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (store: S) -> Self {
    Self { store }
  }
  pub fn batch (&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    let mut data: Vec<u8> = (rows.len() as u32).to_be_bytes();
    for row in rows.iter() {
      data.extend(serialize(row)?);
    }
    let offset = self.store.len()?;
    self.store.write(offset, &data)?;
    Ok(())
  }
  pub fn query (&mut self, offset: usize, bbox: &P::BBox)
  -> Result<Vec<(P,V)>,Error> {
    let rows = self.parse(self.read(offset)?)?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).collect())
  }
  pub fn parse (buf: &Vec<u8>) -> Result<Vec<(P,V)>,Error> {
    let buf = self.read(offset)?;
    let size = size_of(P) + size_of(V);
    let results = vec![];
    for i in 0..buf.len()/size {
      results.push(deserialize(buf[i*size..(i+1)*size])?);
    }
    results
  }
  pub fn read (&mut self, offset: usize) -> Result<Vec<u8>,Error> {
    let len = self.store.len()?;
    let flen = 1024.min(len-offset);
    let mut data = self.store.read(offset, flen)?;
    if len > 1024 {
      let slen = u32::from_be_bytes([data[0],data[1],data[2],data[3]]);
      data.extend(self.store.read(offset + flen, slen - flen));
    }
    Ok(data)
  }
}
