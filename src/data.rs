use crate::{Point,Value,Location,read_block::read_block};
use random_access_storage::RandomAccess;
use failure::{Error,ensure,bail};
use async_std::{sync::{Arc,Mutex}};

use lru::LruCache;
use std::collections::HashMap;
use desert::{FromBytes,ToBytes,CountBytes};

#[async_trait::async_trait]
pub trait DataBatch<P,V>: Send+Sync where P: Point, V: Value {
  async fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error>;
}

pub struct DataMerge<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  data_store: Arc<Mutex<DataStore<S,P,V>>>
}

impl<S,P,V> DataMerge<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  pub fn new (data_store: Arc<Mutex<DataStore<S,P,V>>>) -> Self {
    Self { data_store }
  }
}

#[async_trait::async_trait]
impl<S,P,V> DataBatch<P::Range,u64> for DataMerge<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  async fn batch (&mut self, rows: &Vec<&(P::Range,u64)>) -> Result<u64,Error> {
    if rows.len() == 1 { // use existing address
      Ok(rows[0].1)
    } else { // combine addresses into a new block
      let mut combined: Vec<(P,V)> = vec![];
      let mut dstore = self.data_store.lock().await;
      let max = dstore.max_data_size;
      for row in rows {
        let pvs: Vec<(P,V)> = dstore.list(row.1).await?.iter().map(|c| {
          (c.0, c.1.clone())
        }).collect();
        combined.extend(pvs);
      }
      ensure![combined.len() <= max, "data size limit exceeded in data merge"];
      Ok(dstore.batch(&combined.iter().collect()).await?)
      /*
      let dstore = &mut self.data_store.lock().unwrap();
      let mut combined: Vec<(P,V)> = vec![];
      let max = dstore.max_data_size;
      for row in rows {
        let pvs: Vec<(P,V)> = dstore.list(row.1).await?.iter().map(|c| {
          (c.0, c.1.clone())
        }).collect();
        combined.extend(pvs);
      }
      ensure![combined.len() <= max, "data size limit exceeded in data merge"];
      Ok(dstore.batch(&combined.iter().collect()).await?)
      */
    }
  }
}

//#[derive(Debug,Clone)]
pub struct DataStore<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  store: S,
  range: DataRange<S,P>,
  list_cache: LruCache<u64,Vec<(P,V,Location)>>,
  pub max_data_size: usize
}

#[async_trait::async_trait]
impl<S,P,V> DataBatch<P,V> for DataStore<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  async fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    ensure![rows.len() <= self.max_data_size,
      "data size limit exceeded in data merge"];
    let bitfield_len = (rows.len()+7)/8;
    let mut len = 6 + bitfield_len;
    for row in rows.iter() {
      len += row.count_bytes();
    }
    let mut data = vec![0u8;len];
    let mut offset = 0;
    offset += (len as u32).write_bytes(&mut data[offset..])?;
    offset += (bitfield_len as u16).write_bytes(&mut data[offset..])?;
    for (i,_row) in rows.iter().enumerate() {
      data[6+i/8] |= 1<<(i%8);
    }
    offset += bitfield_len;
    for row in rows.iter() {
      offset += row.write_bytes(&mut data[offset..])?;
    }
    let store_offset = self.store.len().await?;
    self.store.write(store_offset, &data).await?;
    let bbox = match P::bounds(&rows.iter().map(|(p,_)| *p).collect()) {
      None => bail!["failed to calculate bounds"],
      Some(bbox) => bbox
    };
    self.range.write(
      &(store_offset,P::bounds_to_range(bbox),rows.len() as u64)
    ).await?;
    Ok(store_offset)
  }
}

impl<S,P,V> DataStore<S,P,V>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point, V: Value {
  pub fn open (store: S, range_store: S, max_data_size: usize,
  bbox_cache_size: usize, list_cache_size: usize) -> Result<Self,Error> {
    Ok(Self {
      store,
      range: DataRange::new(range_store, bbox_cache_size),
      list_cache: LruCache::new(list_cache_size),
      max_data_size
    })
  }
  pub async fn commit (&mut self) -> Result<(),Error> {
    self.store.sync_all().await?;
    Ok(())
  }
  pub async fn query (&mut self, offset: u64, bbox: &P::Bounds)
  -> Result<Vec<(P,V,Location)>,Error> {
    let rows = self.list(offset).await?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).map(|row| { row.clone() }).collect())
  }
  pub async fn list (&mut self, offset: u64) -> Result<Vec<(P,V,Location)>,Error> {
    match self.list_cache.get(&offset) {
      Some(rows) => return Ok(rows.to_vec()),
      None => {}
    }
    let buf = self.read(offset).await?;
    let rows = self.parse(&buf)?.iter().map(|row| {
      (row.0,row.1.clone(),(offset+1,row.2))
    }).collect();
    self.list_cache.put(offset, rows);
    Ok(self.list_cache.peek(&offset).unwrap().to_vec())
  }
  pub fn parse (&self, buf: &Vec<u8>) -> Result<Vec<(P,V,u32)>,Error> {
    let mut results = vec![];
    let mut offset = 0;
    let bitfield_len = u16::from_be_bytes([buf[0],buf[1]]) as usize;
    offset += 2;
    let bitfield: &[u8] = &buf[offset..offset+bitfield_len];
    offset += bitfield_len;
    let mut index = 0;
    while offset < buf.len() {
      if ((bitfield[index/8]>>(index%8))&1) == 1 {
        let (size,pv) = <(P,V)>::from_bytes(&buf[offset..])?;
        results.push((pv.0,pv.1,index as u32));
        offset += size;
      } else {
        offset += <(P,V)>::count_from_bytes(&buf[offset..])?;
      }
      index += 1;
    }
    Ok(results)
  }
  pub async fn read (&mut self, offset: u64) -> Result<Vec<u8>,Error> {
    let len = self.store.len().await? as u64;
    read_block(&mut self.store, offset, len, 1024).await
  }
  // todo: replace() similar to delete but with an additional array of
  // replacement candidates
  pub async fn delete (&mut self, locations: &Vec<Location>) -> Result<(),Error> {
    let mut by_block: HashMap<u64,Vec<u32>> = HashMap::new();
    for (block,index) in locations {
      if *block == 0 { continue } // staging block
      match by_block.get_mut(&(*block-1)) {
        Some(indexes) => {
          indexes.push(*index);
        },
        None => {
          by_block.insert(*block-1, vec![*index]);
        },
      }
    }
    for (block,indexes) in by_block.iter() {
      let max_i = match indexes.iter().max() {
        Some(i) => *i as u64,
        None => bail!["indexes is an empty array"],
      };
      let len = 7 + max_i/8; // indexes start at 0, unlike lengths
      ensure![len <= self.store.len().await?-block,
        "index length past the end of the block"];
      let mut header = self.store.read(*block, len).await?;
      let block_size = u32::from_bytes(&header[0..])?.1 as u64;
      let bitfield_len = u16::from_bytes(&header[4..])?.1;
      ensure![len <= (bitfield_len as u64) + 6,
        "read length {} from index {} past expected bitfield length {} \
        for block size {} at offset {}",
        len, max_i, bitfield_len, block_size, *block
      ];
      ensure![len <= block_size, "data block is too small"];
      for index in indexes.iter() {
        let i = *index as usize;
        header[6+i/8] &= 0xff - (1<<(i%8));
      }
      self.store.write(block+6, &header[6..]).await?;
      match self.list_cache.get_mut(block) {
        Some(rows) => {
          rows.retain(|row| !indexes.contains(&((row.2).1)));
        },
        None => {},
      }
    }
    Ok(())
  }
  pub async fn bytes (&mut self) -> Result<u64,Error> {
    Ok(self.store.len().await? as u64)
  }
  pub async fn bbox (&mut self, offset: u64)
  -> Result<Option<(P::Bounds,u64)>,Error> {
    match self.range.cache.get(&offset) {
      None => {},
      Some(r) => return Ok(Some(*r))
    };
    let rows = self.list(offset).await?;
    if rows.is_empty() {
      return Ok(None);
    }
    let bbox = match P::bounds(&rows.iter().map(|(p,_,_)| *p).collect()) {
      None => bail!["invalid data at offset {}", offset],
      Some(bbox) => bbox
    };
    let result = (bbox,rows.len() as u64);
    self.range.cache.put(offset, result.clone());
    Ok(Some(result))
  }
}

pub struct DataRange<S,P>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point {
  pub store: S,
  pub cache: LruCache<u64,(P::Bounds,u64)>
}

impl<S,P> DataRange<S,P>
where S: RandomAccess<Error=Error>+Send+Sync, P: Point {
  pub fn new (store: S, cache_size: usize) -> Self {
    Self {
      store,
      cache: LruCache::new(cache_size)
    }
  }
  pub async fn write (&mut self, b: &(u64,P::Range,u64)) -> Result<(),Error> {
    let offset = self.store.len().await?;
    let data = b.to_bytes()?;
    self.store.write(offset, &data).await
  }
  pub async fn list (&mut self) -> Result<Vec<(u64,P,u64)>,Error> {
    let len = self.store.len().await?;
    // TODO: read in chunks instead of all at once
    let buf = self.store.read(0, len).await?;
    let mut offset = 0usize;
    let mut results: Vec<(u64,P,u64)> = vec![];
    while (offset as u64) < len {
      let (size, result) = <(u64,P,u64)>::from_bytes(&buf[offset..])?;
      results.push(result);
      offset += size;
    }
    Ok(results)
  }
}
