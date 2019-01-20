use ::{Row,Point,Value};
use failure::{Error,bail};
use random_access_storage::RandomAccess;
use std::mem::size_of;
use bincode::{serialize,deserialize};

pub struct StagingIterator<'a,'b,P,V> where P: Point, V: Value {
  rows: &'a Vec<Row<P,V>>,
  bbox: &'b P::BBox,
  index: usize
}

impl<'a,'b,P,V> StagingIterator<'a,'b,P,V> where P: Point, V: Value {
  pub fn new (rows: &'a Vec<Row<P,V>>, bbox: &'b P::BBox) -> Self {
    Self { index: 0, bbox, rows }
  }
}

impl<'a,'b,P,V> Iterator for StagingIterator<'a,'b,P,V>
where P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    let len = self.rows.len();
    while self.index < len {
      let i = self.index;
      self.index += 1;
      match self.rows[i] {
        Row::Insert(point,value) => {
          if point.overlaps(self.bbox) {
            return Some(Ok((point,value)))
          }
        },
        Row::Delete(point,value) => {}
      }
    }
    None
  }
}

pub struct Staging<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  rows: Vec<Row<P,V>>
}

impl<S,P,V> Staging<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  const INSERT: u8 = 0u8;
  const DELETE: u8 = 1u8;

  pub fn open (mut store: S) -> Result<Self,Error> {
    let is_empty = store.is_empty()?;
    let mut staging = Self { store, rows: vec![] };
    if !is_empty { staging.load()? }
    Ok(staging)
  }
  fn load (&mut self) -> Result<(),Error> {
    let len = self.store.len()?;
    let buf = self.store.read(0, len)?;
    let n = 1+size_of::<P>() + size_of::<V>();
    let m = len/n;
    self.rows.clear();
    self.rows.reserve(m);
    for i in 0..m {
      let offset = i*n;
      let (pt_type,point,value): (u8,P,V)
        = deserialize(&buf[offset..offset+n])?;
      self.rows.push(match pt_type {
        0u8 => Row::Insert(point,value),
        1u8 => Row::Delete(point,value),
        _ => bail!("unexpected point type")
      });
    }
    Ok(())
  }
  pub fn clear (&mut self) -> Result<(),Error> {
    self.store.truncate(0)?;
    Ok(())
  }
  pub fn len (&mut self) -> Result<usize,Error> {
    let len = self.store.len()?;
    let n = 1+size_of::<P>() + size_of::<V>();
    Ok(len/n)
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let offset = self.store.len()?;
    let n = 1+size_of::<P>() + size_of::<V>();
    let mut buf: Vec<u8> = Vec::with_capacity(n*rows.len());
    for row in rows {
      let bytes: Vec<u8> = serialize(&match row {
        Row::Insert(point,value) => (Self::INSERT,point,value),
        Row::Delete(point,value) => (Self::DELETE,point,value)
      })?;
      buf.extend(bytes);
    }
    self.store.write(offset,&buf)?;
    self.rows.extend_from_slice(rows);
    Ok(())
  }
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::BBox)
  -> StagingIterator<'a,'b,P,V> {
    <(StagingIterator<'a,'b,P,V>)>::new(&self.rows, bbox)
  }
}
