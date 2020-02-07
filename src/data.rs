use crate::{Point,Value,Location,read_block::read_block};
use random_access_storage::RandomAccess;
use failure::{Error,ensure,bail};
use std::rc::Rc;
use std::cell::RefCell;
use lru::LruCache;
use std::collections::HashMap;

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
      let mut combined: Vec<(P,V)> = vec![];
      for row in rows {
        let pvs: Vec<(P,V)> = dstore.list(row.1)?.iter().map(|c| {
          (c.0, c.1.clone())
        }).collect();
        combined.extend(pvs);
      }
      ensure![combined.len() <= max, "data size limit exceeded in data merge"];
      dstore.batch(&combined.iter().collect())
    }
  }
}

//#[derive(Debug,Clone)]
pub struct DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  range: DataRange<S,P>,
  list_cache: LruCache<u64,Vec<(P,V,Location)>>,
  pub max_data_size: usize,
  pub bincode: Rc<bincode::Config>
}

impl<S,P,V> DataBatch<P,V> for DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  fn batch (&mut self, rows: &Vec<&(P,V)>) -> Result<u64,Error> {
    ensure![rows.len() <= self.max_data_size,
      "data size limit exceeded in data merge"];
    let bitfield_len = (rows.len()+7)/8;
    let mut data: Vec<u8> = vec![0;6+bitfield_len];
    for (i,_row) in rows.iter().enumerate() {
      data[6+i/8] |= 1<<(i%8);
    }
    for row in rows.iter() {
      let buf = self.bincode.serialize(row)?;
      //ensure_eq!(buf.len(), P::size_of() + size_of::<V>(),
      //  "unexpected length in data batch");
      data.extend(buf);
    }
    let len = data.len() as u32;
    data[0..4].copy_from_slice(&len.to_be_bytes());
    data[4..6].copy_from_slice(&(bitfield_len as u16).to_be_bytes());
    let offset = self.store.len()? as u64;
    self.store.write(offset, &data)?;
    let bbox = match P::bounds(&rows.iter().map(|(p,_)| *p).collect()) {
      None => bail!["invalid data at offset {}", offset],
      Some(bbox) => bbox
    };
    self.range.write(&(offset,P::bounds_to_range(bbox),rows.len() as u64))?;
    Ok(offset as u64)
  }
}

impl<S,P,V> DataStore<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S, range_store: S,
  max_data_size: usize, bbox_cache_size: usize,
  list_cache_size: usize, bincode: Rc<bincode::Config>) -> Result<Self,Error> {
    Ok(Self {
      store,
      range: DataRange::new(
        range_store, bbox_cache_size, Rc::clone(&bincode)
      ),
      list_cache: LruCache::new(list_cache_size),
      max_data_size,
      bincode
    })
  }
  pub fn commit (&mut self) -> Result<(),Error> {
    self.store.sync_all()?;
    Ok(())
  }
  pub fn query (&mut self, offset: u64, bbox: &P::Bounds)
  -> Result<Vec<(P,V,Location)>,Error> {
    let rows = self.list(offset)?;
    Ok(rows.iter().filter(|row| {
      row.0.overlaps(bbox)
    }).map(|row| { row.clone() }).collect())
  }
  pub fn list (&mut self, offset: u64) -> Result<Vec<(P,V,Location)>,Error> {
    match self.list_cache.get(&offset) {
      Some(rows) => return Ok(rows.to_vec()),
      None => {}
    }
    let buf = self.read(offset)?;
    let rows = self.parse(&buf)?.iter().map(|row| {
      (row.0,row.1.clone(),(offset+1,row.2))
    }).collect();
    self.list_cache.put(offset, rows);
    Ok(self.list_cache.peek(&offset).unwrap().to_vec())
  }
  pub fn parse (&self, buf: &Vec<u8>) -> Result<Vec<(P,V,usize)>,Error> {
    let mut results = vec![];
    let mut offset = 0;
    let bitfield_len = u16::from_be_bytes([buf[0],buf[1]]) as usize;
    offset += 2;
    let bitfield: &[u8] = &buf[offset..offset+bitfield_len];
    offset += bitfield_len;
    let mut index = 0;
    while offset < buf.len() {
      let psize = P::size_of();
      let vsize = V::take_bytes(offset+psize, &buf);
      let n = psize + vsize;
      if ((bitfield[index/8]>>(index%8))&1) == 1 {
        let pv: (P,V) = self.bincode.deserialize(&buf[offset..offset+n])?;
        results.push((pv.0,pv.1,offset));
      }
      offset += n;
      index += 1;
    }
    Ok(results)
  }
  pub fn read (&mut self, offset: u64) -> Result<Vec<u8>,Error> {
    let len = self.store.len()? as u64;
    read_block(&mut self.store, offset, len, 1024)
  }
  // todo: replace() similar to delete but with an additional array of
  // replacement candidates
  pub fn delete (&mut self, locations: &Vec<Location>) -> Result<(),Error> {
    let mut by_block: HashMap<u64,Vec<usize>> = HashMap::new();
    for (block,index) in locations {
      if *block == 0 { continue } // staging block
      match by_block.get_mut(&block) {
        Some(indexes) => {
          indexes.push(*index);
        },
        None => {
          by_block.insert(*block, vec![*index]);
        },
      }
    }
    for (block,indexes) in by_block.iter() {
      let max_i = match indexes.iter().max() {
        Some(i) => *i as u64,
        None => bail!["indexes is an empty array"],
      };
      let len = 6 + (max_i+7)/8 + 1;
      ensure![len <= self.store.len()?-*block-1,
        "index length past the end of the block"];
      let mut header = self.store.read(*block-1, len)?;
      let block_size = u32::from_be_bytes(
        [header[0],header[1],header[2],header[3]]
      ) as u64;
      ensure![len <= block_size, "data block is too small"];
      for index in indexes.iter() {
        header[6+index/8] &= 0xff & (1<<(index%8));
        eprintln!["header[6+{}/8] = 0xff & ({} & 1<<({}%8))",
          index, header[6+index/8], index];
      }
      self.store.write(*block-1+6, &header[6..])?
    }
    Ok(())
  }
  pub fn bytes (&mut self) -> Result<u64,Error> {
    Ok(self.store.len()? as u64)
  }
  pub fn bbox (&mut self, offset: u64)
  -> Result<Option<(P::Bounds,u64)>,Error> {
    match self.range.cache.get(&offset) {
      None => {},
      Some(r) => return Ok(Some(*r))
    };
    let rows = self.list(offset)?;
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
where S: RandomAccess<Error=Error>, P: Point {
  pub store: S,
  pub cache: LruCache<u64,(P::Bounds,u64)>,
  bincode: Rc<bincode::Config>
}

impl<S,P> DataRange<S,P>
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
    let bsize = <P::Range as Point>::size_of() as u64;
    let n = 8 + bsize + 8;
    let len = self.store.len()?;
    let buf_size = {
      let x = 1024 * 1024;
      x - (x % n) // ensure buf_size is a multiple of n
    };
    for j in 0..(len+buf_size-1)/buf_size {
      let buf = self.store.read(j*buf_size,((j+1)*buf_size).min(len))?;
      for i in 0..buf.len()/(n as usize) {
        let offset = i * (n as usize);
        results.push(self.bincode.deserialize(
          &buf[offset..offset+(n as usize)])?
        );
      }
    }
    Ok(results)
  }
}
