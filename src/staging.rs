use crate::{Point,Value,Location,write_cache::WriteCache};
use failure::{Error};
use random_access_storage::RandomAccess;
use std::mem::size_of;
use bincode::{serialize,deserialize};

pub struct StagingIterator<'a,'b,P,V> where P: Point, V: Value {
  inserts: &'a Vec<(P,V)>,
  deletes: &'a Vec<Location>,
  bbox: &'b P::Bounds,
  index: usize
}

impl<'a,'b,P,V> StagingIterator<'a,'b,P,V> where P: Point, V: Value {
  pub fn new (inserts: &'a Vec<(P,V)>, deletes: &'a Vec<Location>,
  bbox: &'b P::Bounds) -> Self {
    Self { index: 0, bbox, inserts, deletes }
  }
}

impl<'a,'b,P,V> Iterator for StagingIterator<'a,'b,P,V>
where P: Point, V: Value {
  type Item = Result<(P,V,Location),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    let len = self.inserts.len();
    while self.index < len {
      let i = self.index;
      self.index += 1;
      let (point,value) = &self.inserts[i];
      if point.overlaps(self.bbox) {
        return Some(Ok((*point,value.clone(),(0, i))));
      }
    }
    None
  }
}

pub struct Staging<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  insert_store: WriteCache<S>,
  delete_store: WriteCache<S>,
  pub inserts: Vec<(P,V)>,
  pub deletes: Vec<Location>,
}

impl<S,P,V> Staging<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (istore: S, dstore: S) -> Result<Self,Error> {
    let mut staging = Self {
      insert_store: WriteCache::open(istore)?,
      delete_store: WriteCache::open(dstore)?,
      inserts: vec![],
      deletes: vec![],
    };
    staging.load()?;
    Ok(staging)
  }
  fn load (&mut self) -> Result<(),Error> {
    if !self.insert_store.is_empty()? {
      self.inserts.clear();
      let len = self.insert_store.len()?;
      let buf = self.insert_store.read(0, len)?;
      let mut offset = 0;
      while offset < len as usize {
        let psize = P::size_of();
        let vsize = V::take_bytes(offset+psize, &buf);
        let n = psize + vsize;
        let pv: (P,V) = deserialize(&buf[offset..offset+n])?;
        self.inserts.push(pv);
        offset += n;
      }
    }
    if !self.delete_store.is_empty()? {
      self.deletes.clear();
      let len = self.delete_store.len()?;
      let buf = self.delete_store.read(0, len)?;
      let mut offset = 0;
      while offset < len as usize {
        let psize = P::size_of();
        let vsize = V::take_bytes(offset+psize, &buf);
        let n = psize + vsize;
        let loc: Location = deserialize(&buf[offset..offset+n])?;
        self.deletes.push(loc);
        offset += n;
      }
    }
    Ok(())
  }
  pub fn clear (&mut self) -> Result<(),Error> {
    self.insert_store.truncate(0)?;
    self.delete_store.truncate(0)?;
    self.inserts.clear();
    self.deletes.clear();
    Ok(())
  }
  pub fn bytes (&mut self) -> Result<u64,Error> {
    Ok(self.insert_store.len()? + self.delete_store.len()?)
  }
  pub fn len (&mut self) -> Result<usize,Error> {
    Ok(self.inserts.len() + self.deletes.len())
  }
  pub fn batch (&mut self, inserts: &Vec<(P,V)>, deletes: &Vec<Location>)
  -> Result<(),Error> {
    let n = size_of::<u8>() + P::size_of() + size_of::<V>();
    let mut ibuf: Vec<u8> = Vec::with_capacity(n*inserts.len());
    let mut dbuf: Vec<u8> = Vec::with_capacity(
      size_of::<Location>()*deletes.len());
    for insert in inserts {
      ibuf.extend(serialize(&insert)?);
    }
    for delete in deletes {
      dbuf.extend(serialize(&delete)?);
    }
    let i_offset = self.insert_store.len()?;
    self.insert_store.write(i_offset,&ibuf)?;
    let d_offset = self.delete_store.len()?;
    self.delete_store.write(d_offset,&dbuf)?;
    self.inserts.extend_from_slice(inserts);
    self.deletes.extend_from_slice(deletes);
    Ok(())
  }
  pub fn commit (&mut self) -> Result<(),Error> {
    self.insert_store.sync_all()?;
    self.delete_store.sync_all()?;
    Ok(())
  }
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::Bounds)
  -> StagingIterator<'a,'b,P,V> {
    <StagingIterator<'a,'b,P,V>>::new(&self.inserts, &self.deletes, bbox)
  }
}
