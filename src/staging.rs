use crate::{Point,Value,Location,Error};
use random_access_storage::RandomAccess;
use std::collections::HashSet;
use async_std::sync::{Arc,Mutex};
use desert::{FromBytes,ToBytes,CountBytes};

pub struct StagingIterator<P,V> where P: Point, V: Value {
  inserts: Arc<Mutex<Vec<(P,V)>>>,
  deletes: Arc<Mutex<HashSet<Location>>>,
  bbox: Arc<P::Bounds>,
  index: u32
}

type Item<P,V> = Result<(P,V,Location),Error>;
impl<P,V> StagingIterator<P,V> where P: Point, V: Value {
  pub fn new (inserts: Arc<Mutex<Vec<(P,V)>>>,
  deletes: Arc<Mutex<HashSet<Location>>>, bbox: Arc<P::Bounds>) -> Self {
    Self { index: 0, bbox, inserts, deletes }
  }
  pub async fn next (&mut self) -> Option<Item<P,V>> {
    let len = self.inserts.lock().await.len();
    while (self.index as usize) < len {
      let i = self.index;
      self.index += 1;
      if self.deletes.lock().await.contains(&(0,i)) {
        continue;
      }
      let (point,value) = &self.inserts.lock().await[i as usize];
      if point.overlaps(&self.bbox) {
        return Some(Ok((*point,value.clone(),(0, i))));
      }
    }
    None
  }
}

pub struct Staging<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  insert_store: S,
  delete_store: S,
  pub inserts: Arc<Mutex<Vec<(P,V)>>>,
  pub deletes: Arc<Mutex<Vec<Location>>>,
  pub delete_set: Arc<Mutex<HashSet<Location>>>
}

impl<S,P,V> Staging<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  pub async fn open (istore: S, dstore: S) -> Result<Self,Error> {
    let mut staging = Self {
      insert_store: istore,
      delete_store: dstore,
      inserts: Arc::new(Mutex::new(vec![])),
      deletes: Arc::new(Mutex::new(vec![])),
      delete_set: Arc::new(Mutex::new(HashSet::new()))
    };
    staging.load().await?;
    Ok(staging)
  }
  async fn load (&mut self) -> Result<(),Error> {
    if !self.insert_store.is_empty().await? {
      let mut inserts = self.inserts.lock().await;
      inserts.clear();
      let len = self.insert_store.len().await?;
      let buf = self.insert_store.read(0, len).await?;
      let mut offset = 0;
      while offset < len as usize {
        let (size,pv) = <(P,V)>::from_bytes(&buf[offset..])?;
        inserts.push(pv);
        offset += size;
      }
    }
    if !self.delete_store.is_empty().await? {
      let mut deletes = self.deletes.lock().await;
      let mut delete_set = self.delete_set.lock().await;
      deletes.clear();
      delete_set.clear();
      let len = self.delete_store.len().await?;
      let buf = self.delete_store.read(0, len).await?;
      let mut offset = 0;
      while offset < len as usize {
        let (size,loc) = Location::from_bytes(&buf[offset..])?;
        deletes.push(loc);
        delete_set.insert(loc);
        offset += size;
      }
    }
    Ok(())
  }
  pub async fn clear (&mut self) -> Result<(),Error> {
    self.clear_inserts().await?;
    self.clear_deletes().await?;
    Ok(())
  }
  pub async fn clear_inserts (&mut self) -> Result<(),Error> {
    self.insert_store.truncate(0).await?;
    self.inserts.lock().await.clear();
    Ok(())
  }
  pub async fn clear_deletes (&mut self) -> Result<(),Error> {
    self.delete_store.truncate(0).await?;
    self.deletes.lock().await.clear();
    self.delete_set.lock().await.clear();
    Ok(())
  }
  pub async fn delete (&mut self, deletes: &Vec<Location>) -> Result<(),Error> {
    let mut del_set: HashSet<u32> = HashSet::new();
    for delete in deletes {
      if delete.0 == 0 { del_set.insert(delete.1); }
    }
    let mut i = 0;
    self.inserts.lock().await.retain(|_row| {
      let j = i;
      i += 1;
      !del_set.contains(&j)
    });
    Ok(())
  }
  pub async fn bytes (&mut self) -> Result<u64,Error> {
    Ok(self.insert_store.len().await? + self.delete_store.len().await?)
  }
  pub async fn len (&mut self) -> Result<usize,Error> {
    Ok(
      self.inserts.lock().await.len()
      + self.deletes.lock().await.len()
    )
  }
  pub async fn batch (&mut self, inserts: &Vec<(P,V)>, deletes: &Vec<Location>)
  -> Result<(),Error> {
    // todo: calculate the necessary size before allocating
    let mut i_size = 0;
    for insert in inserts.iter() {
      i_size += insert.count_bytes();
    }
    let mut ibuf = vec![0u8;i_size];
    {
      let mut i_offset = 0;
      for insert in inserts.iter() {
        i_offset += insert.write_bytes(&mut ibuf[i_offset..])?;
      }
    }

    let mut d_size = 0;
    for delete in deletes.iter() {
      d_size += delete.count_bytes();
    }
    let mut dbuf = vec![0u8;d_size];
    {
      let mut d_offset = 0;
      for delete in deletes.iter() {
        d_offset += delete.write_bytes(&mut dbuf[d_offset..])?;
      }
    }

    let i_offset = self.insert_store.len().await?;
    self.insert_store.write(i_offset,&ibuf).await?;
    let d_offset = self.delete_store.len().await?;
    self.delete_store.write(d_offset,&dbuf).await?;
    self.inserts.lock().await.extend_from_slice(inserts);
    self.deletes.lock().await.extend_from_slice(deletes);
    {
      let mut delete_set = self.delete_set.lock().await;
      for delete in deletes {
        delete_set.insert(*delete);
      }
    }
    Ok(())
  }
  pub async fn commit (&mut self) -> Result<(),Error> {
    self.insert_store.sync_all().await?;
    self.delete_store.sync_all().await?;
    Ok(())
  }
  pub fn query (&mut self, bbox: Arc<P::Bounds>)
  -> StagingIterator<P,V> {
    <StagingIterator<P,V>>::new(
      Arc::clone(&self.inserts),
      Arc::clone(&self.delete_set),
      bbox
    )
  }
}
