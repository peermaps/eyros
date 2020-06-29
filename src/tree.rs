use random_access_storage::RandomAccess;
use failure::{Error,format_err};
use async_std::sync::{Arc,Mutex};
use std::mem::size_of;
use async_std::stream::Stream;
use futures::stream::unfold;

use crate::{Point,Value,Location};
use crate::branch::{Branch,Node};
use crate::data::{DataStore,DataMerge,DataBatch};
use crate::read_block::read_block;

#[doc(hidden)]
#[macro_export]
macro_rules! iwrap {
  ($x:expr) => {
    match $x {
      Err(e) => { return Some(Err(e.into())) },
      Ok(b) => { b }
    }
  };
}

#[doc(hidden)]
#[macro_export]
macro_rules! swrap {
  ($x:expr) => {
    match $x {
      Err(e) => { return Poll::Ready(Some(Err(Error::from(e)))) },
      Ok(b) => { b }
    }
  };
}

pub struct TreeStream<S,P,V> where
S: RandomAccess<Error=Box<Error>>+Send+Sync, P: Point, V: Value {
  tree: Arc<Mutex<Tree<S,P,V>>>,
  bbox: Arc<P::Bounds>,
  cursors: Vec<(u64,usize)>,
  blocks: Vec<u64>,
  queue: Vec<(P,V,Location)>,
  tree_size: u64
}

impl<S,P,V> TreeStream<S,P,V> where
S: RandomAccess<Error=Box<Error>>+Send+Sync, P: Point, V: Value {
  pub async fn new (tree: Arc<Mutex<Tree<S,P,V>>>, bbox: Arc<P::Bounds>)
  -> Result<Self,Box<Error>> {
    let tree_size = tree.lock().await.store.len().await? as u64;
    Ok(Self {
      tree,
      tree_size,
      bbox,
      cursors: vec![(0,0)],
      blocks: vec![],
      queue: vec![]
    })
  }
  async fn get_next(&mut self) -> Option<Result<(P,V,Location),Box<Error>>> {
    let bf = self.tree.lock().await.branch_factor;

    // todo: used cached size or rolling max to implicitly read an appropriate
    // amount of data
    while !self.cursors.is_empty() || !self.blocks.is_empty()
    || !self.queue.is_empty() {
      if !self.queue.is_empty() {
        return Some(Ok(self.queue.pop().unwrap()));
      }
      if !self.blocks.is_empty() { // data block:
        let offset = self.blocks.pop().unwrap();
        let rows = {
          let tree = self.tree.lock().await;
          let mut dstore = tree.data_store.lock().await;
          iwrap![dstore.query(offset, &self.bbox).await]
        };
        self.queue.extend(rows);
        continue
      }
      // branch block:
      let (cursor,depth) = self.cursors.pop().unwrap();
      if cursor >= self.tree_size { continue }

      let buf = {
        let mut tree = self.tree.lock().await;
        iwrap![read_block(&mut tree.store, cursor, self.tree_size, 1024).await]
      };
      let (cursors,blocks) = iwrap![
        P::query_branch(&buf, &self.bbox, bf, depth)
      ];
      self.blocks.extend(blocks);
      self.cursors.extend(cursors);
    }
    None
  }
}

pub struct TreeOpts<S,P,V>
where S: RandomAccess<Error=Box<Error>>+Send+Sync, P: Point, V: Value {
  pub store: S,
  pub data_store: Arc<Mutex<DataStore<S,P,V>>>,
  pub branch_factor: usize,
  pub max_data_size: usize,
  pub index: usize,
}

pub struct Tree<S,P,V>
where S: RandomAccess<Error=Box<Error>>+Send+Sync, P: Point, V: Value {
  pub store: S,
  data_store: Arc<Mutex<DataStore<S,P,V>>>,
  data_merge: Arc<Mutex<DataMerge<S,P,V>>>,
  branch_factor: usize,
  pub bytes: u64,
  pub index: usize,
  max_data_size: usize,
}

impl<S,P,V> Tree<S,P,V>
where S: RandomAccess<Error=Box<Error>>+Send+Sync, P: Point, V: Value {
  pub async fn open (opts: TreeOpts<S,P,V>) -> Result<Self,Box<Error>> {
    let bytes = opts.store.len().await? as u64;
    let data_merge = Arc::new(Mutex::new(
      DataMerge::new(Arc::clone(&opts.data_store))));
    Ok(Self {
      store: opts.store,
      data_store: opts.data_store,
      data_merge,
      index: opts.index,
      bytes,
      branch_factor: opts.branch_factor,
      max_data_size: opts.max_data_size,
    })
  }
  pub async fn clear (&mut self) -> Result<(),Box<Error>> {
    if self.bytes > 0 {
      self.bytes = 0;
      self.store.truncate(0).await?;
    }
    self.store.sync_all().await?;
    Ok(())
  }
  pub async fn is_empty (&mut self) -> Result<bool,Box<Error>> {
    let r = self.store.is_empty().await?;
    Ok(r)
  }
  pub async fn build (&mut self, rows: &Vec<(P,V)>) -> Result<(),Box<Error>> {
    let dstore = Arc::clone(&self.data_store);
    self.builder(
      Arc::new(rows.iter().map(|row| { (row.clone(),1u64) }).collect()),
      dstore
    ).await
  }
  pub async fn build_from_blocks (&mut self, blocks: Vec<(P::Bounds,u64,u64)>)
  -> Result<(),Box<Error>> {
    let inserts: Vec<(P::Range,u64)> = blocks.iter()
      .map(|(bbox,offset,_)| { (P::bounds_to_range(*bbox),*offset) })
      .collect();
    let rows = blocks.iter().enumerate().map(|(i,(_,_,len))| {
      (inserts[i],*len)
    }).collect();
    let dmerge = Arc::clone(&self.data_merge);
    self.builder(Arc::new(rows), dmerge).await
  }
  pub async fn builder<D,T,U> (&mut self, rows: Arc<Vec<((T,U),u64)>>,
  data_store: Arc<Mutex<D>>) -> Result<(),Box<Error>>
  where D: DataBatch<T,U>, T: Point, U: Value {
    self.clear().await?;
    let bucket = (0..rows.len()).collect();
    let b = Branch::<D,T,U>::new(
      0,
      self.index,
      self.max_data_size,
      self.branch_factor,
      Arc::clone(&data_store),
      bucket, rows
    )?;
    let mut branches = vec![Node::Branch(b)];
    match branches[0] {
      Node::Branch(ref mut b) => {
        let alloc = &mut {|bytes| self.alloc(bytes) };
        b.alloc(alloc);
      },
      _ => panic!["unexpected initial node type"]
    };
    while !branches.is_empty() {
      let mut nbranches = vec![];
      for mut branch in branches {
        match branch {
          Node::Empty => {},
          Node::Data(_) => {},
          Node::Branch(ref mut b) => {
            let (data,nb) = {
              let alloc = &mut {|bytes| self.alloc(bytes) };
              b.build(alloc).await?
            };
            self.store.write(b.offset, &data).await?;
            self.bytes = self.bytes.max(b.offset + (data.len() as u64));
            nbranches.extend(nb);
          }
        }
      }
      branches = nbranches;
    }
    self.store.sync_all().await?;
    Ok(())
  }
  pub async fn query (tree: Arc<Mutex<Self>>, bbox: Arc<P::Bounds>)
  -> Result<impl Stream<Item=Result<(P,V,Location),Box<Error>>>,Box<Error>> {
    let ts = TreeStream::new(tree, bbox).await?;
    Ok(unfold(ts, async move |mut ts| {
      let res = ts.get_next().await;
      match res {
        Some(p) => Some((p,ts)),
        None => None
      }
    }))
  }
  fn alloc (&mut self, bytes: usize) -> u64 {
    let addr = self.bytes;
    self.bytes += bytes as u64;
    addr
  }
  pub async fn merge (trees: &mut Vec<Arc<Mutex<Self>>>, dst: usize, src: Vec<usize>,
  rows: &Vec<(P,V)>) -> Result<(),Box<Error>> {
    let mut blocks = vec![];
    for i in src.iter() {
      blocks.extend(trees[*i].lock().await.unbuild().await?);
    }
    {
      let tree = trees[dst].lock().await;
      let mut dstore = tree.data_store.lock().await;
      let m = tree.max_data_size;
      let mut srow_len = 0;
      for i in 0..(rows.len()+m-1)/m {
        let srows = &rows[i*m..((i+1)*m).min(rows.len())];
        srow_len += srows.len();
        let inserts: Vec<(P,V)> = srows.iter()
          .map(|(p,v)| (*p,v.clone())).collect();
        let offset = dstore.batch(&inserts.iter().map(|pv| pv).collect()).await?;
        match P::bounds(&inserts.iter().map(|(p,_)| *p).collect()) {
          None => fail!["invalid data at offset {}", offset],
          Some(bbox) => blocks.push((bbox,offset,inserts.len() as u64))
        }
      }
      ensure_eq_box!(srow_len, rows.len(), "divided rows incorrectly");
    }
    trees[dst].lock().await.build_from_blocks(blocks).await?;
    for i in src.iter() {
      trees[*i].lock().await.clear().await?
    }
    Ok(())
  }
  async fn unbuild (&mut self) -> Result<Vec<(P::Bounds,u64,u64)>,Box<Error>> {
    let mut offsets: Vec<u64> = vec![];
    let mut cursors: Vec<(u64,usize)> = vec![(0,0)];
    let bf = self.branch_factor;
    let n = bf*2-3;
    let tree_size = self.store.len().await? as u64;
    while !cursors.is_empty() {
      let (c,depth) = cursors.pop().unwrap();
      let buf = read_block(&mut self.store, c, tree_size, 1024).await?;
      let mut offset = 0;
      for _i in 0..n {
        offset += P::count_bytes_at(&buf[offset..], depth)?;
      }
      let d_start = offset;
      let i_start = d_start + (n+bf+7)/8;
      let b_start = i_start + n*size_of::<u64>();
      let b_end = b_start+bf*size_of::<u64>();
      ensure_eq_box!(b_end, buf.len(), "unexpected block length");
      for i in 0..n {
        let offset = u64::from_be_bytes([
          buf[i_start+i*8+0], buf[i_start+i*8+1],
          buf[i_start+i*8+2], buf[i_start+i*8+3],
          buf[i_start+i*8+4], buf[i_start+i*8+5],
          buf[i_start+i*8+6], buf[i_start+i*8+7]
        ]);
        let is_data = ((buf[d_start+i/8]>>(i%8))&1) == 1;
        if offset > 0 && is_data {
          offsets.push(offset-1);
        } else if offset > 0 {
          cursors.push((offset-1,depth+1));
        }
      }
      for i in 0..bf {
        let offset = u64::from_be_bytes([
          buf[b_start+i*8+0], buf[b_start+i*8+1],
          buf[b_start+i*8+2], buf[b_start+i*8+3],
          buf[b_start+i*8+4], buf[b_start+i*8+5],
          buf[b_start+i*8+6], buf[b_start+i*8+7]
        ]);
        let j = i + n;
        let is_data = ((buf[d_start+(j/8)]>>(j%8))&1) == 1;
        if offset > 0 && is_data {
          offsets.push(offset-1);
        } else if offset > 0 {
          cursors.push((offset-1,depth+1));
        }
      }
    }
    let mut blocks = Vec::with_capacity(offsets.len());
    let mut dstore = self.data_store.lock().await;
    for offset in offsets {
      match dstore.bbox(offset).await? {
        Some((bbox,len)) => blocks.push((bbox,offset,len)),
        None => {},
      }
    }
    Ok(blocks)
  }
}
