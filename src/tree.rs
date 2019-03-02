use random_access_storage::RandomAccess;
use failure::{Error,bail};
use std::marker::PhantomData;
use std::cell::RefCell;
use std::rc::Rc;
use std::mem::size_of;

use ::{Row,Point,Value};

use branch::{Branch,Node};
use data::DataStore;
use read_block::read_block;

pub struct TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  tree: &'a mut Tree<S,P,V>,
  bbox: &'b P::Bounds,
  cursors: Vec<(u64,usize)>,
  blocks: Vec<u64>,
  queue: Vec<(P,V)>,
  tree_size: u64
}

impl<'a,'b,S,P,V> TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (tree: &'a mut Tree<S,P,V>, bbox: &'b P::Bounds)
  -> Result<Self,Error> {
    let tree_size = tree.store.len()? as u64;
    Ok(Self {
      tree,
      tree_size,
      bbox,
      cursors: vec![(0,0)],
      blocks: vec![],
      queue: vec![]
    })
  }
}

macro_rules! iwrap {
  ($x:expr) => {
    match $x {
      Err(e) => { return Some(Err(Error::from(e))) },
      Ok(b) => { b }
    }
  };
}

impl<'a,'b,S,P,V> Iterator for TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    let store = &mut self.tree.store;
    let order = &self.tree.order;
    let bf = self.tree.branch_factor;
    let n = bf*2-3;

    // todo: used cached size or rolling max to implicitly read an appropriate
    // amount of data
    while !self.cursors.is_empty() || !self.blocks.is_empty()
    || !self.queue.is_empty() {
      if !self.queue.is_empty() {
        return Some(Ok(self.queue.pop().unwrap()))
      }
      if !self.blocks.is_empty() { // data block:
        let offset = self.blocks.pop().unwrap();
        let mut dstore = iwrap![
          self.tree.data_store.try_borrow_mut()
        ];
        self.queue.extend(iwrap![dstore.query(offset, self.bbox)]);
        continue
      }
      // branch block:
      let (cursor,depth) = self.cursors.pop().unwrap();
      if cursor >= self.tree_size { continue }

      let buf = iwrap![read_block(store, cursor, self.tree_size, 1024)];
      let psize = P::pivot_size_at(depth % P::dim());
      let p_start = 0;
      let d_start = p_start + n*psize;
      let i_start = d_start + (n+bf+7)/8;
      let b_start = i_start + n*size_of::<u64>();
      let b_end = b_start+bf*size_of::<u64>();
      assert_eq!(b_end, buf.len(), "unexpected block length");

      let mut bcursors = vec![0];
      let mut bitfield: Vec<bool> = vec![false;bf]; // which buckets
      while !bcursors.is_empty() {
        let c = bcursors.pop().unwrap();
        let i = order[c];
        let cmp: (bool,bool) = iwrap![P::cmp_buf(
          &buf[p_start+i*psize..p_start+(i+1)*psize],
          &self.bbox,
          depth % P::dim()
        )];
        let is_data = ((buf[d_start+i/8]>>(i%8))&1) == 1;
        let i_offset = i_start + i*8;
        if cmp.0 && cmp.1 && is_data { // intersection
          let offset = u64::from_be_bytes([
            buf[i_offset+0], buf[i_offset+1],
            buf[i_offset+2], buf[i_offset+3],
            buf[i_offset+4], buf[i_offset+5],
            buf[i_offset+6], buf[i_offset+7],
          ]);
          if offset > 0 {
            self.blocks.push(offset-1);
          }
        }
        if cmp.0 && c*2+1 < n { // left internal
          bcursors.push(c*2+1);
        } else if cmp.0 { // left branch
          bitfield[i/2] = true;
        }
        if cmp.1 && c*2+2 < n { // right internal
          bcursors.push(c*2+2);
        } else if cmp.1 { // right branch
          bitfield[i/2+1] = true;
        }
        // internal leaves are even integers in (0..n)
        // which map to buckets `i/2+0` and/or `i/2+1`
        // depending on left/right comparisons
        /*                7
                   3             11
                1     5       9      13
              0   2 4  6    8  10  12  14
          B: 0  1  2  3   4  5   6   7   8
        */
      }
      for (i,b) in bitfield.iter().enumerate() {
        let j = i+n;
        let is_data = (buf[d_start+j/8]>>(j%8))&1 == 1;
        if !b { continue }
        let offset = u64::from_be_bytes([
          buf[b_start+i*8+0], buf[b_start+i*8+1],
          buf[b_start+i*8+2], buf[b_start+i*8+3],
          buf[b_start+i*8+4], buf[b_start+i*8+5],
          buf[b_start+i*8+6], buf[b_start+i*8+7]
        ]);
        if offset > 0 && is_data {
          self.blocks.push(offset-1);
        } else if offset > 0 {
          self.cursors.push((offset-1,depth+1));
        }
      }
    }
    None
  }
}

pub struct Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub store: S,
  data_store: Rc<RefCell<DataStore<S,P,V>>>,
  branch_factor: usize,
  size: u64,
  max_data_size: usize,
  order: Rc<Vec<usize>>,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (mut store: S, data_store: Rc<RefCell<DataStore<S,P,V>>>,
  branch_factor: usize, max_data_size: usize,
  order: Rc<Vec<usize>>) -> Result<Self,Error> {
    let size = store.len()? as u64;
    Ok(Self {
      store,
      data_store,
      size,
      order,
      branch_factor,
      max_data_size,
      _marker: PhantomData
    })
  }
  pub fn clear (&mut self) -> Result<(),Error> {
    self.store.truncate(0)?;
    Ok(())
  }
  pub fn is_empty (&mut self) -> Result<bool,Error> {
    let r = self.store.is_empty()?;
    Ok(r)
  }
  pub fn build (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let bf = self.branch_factor;
    if self.size > 0 {
      self.size = 0;
      self.store.truncate(0)?;
    }
    if rows.len() < bf*2-3 {
      bail!("tree must have at least {} records", bf*2-3);
    }
    let irows: Vec<(P,V)> = rows.iter()
      .filter(|row| { match row { Row::Insert(_,_) => true, _ => false } })
      .map(|row| { match row {
        Row::Insert(p,v) => (*p,*v),
        _ => panic!("unexpected")
      } })
      .collect();
    let bucket = (0..rows.len()).collect();
    let b = Branch::new(0, self.max_data_size,
      Rc::clone(&self.order),
      Rc::clone(&self.data_store),
      bucket, &irows
    );
    let mut branches = vec![Node::Branch(b)];
    match branches[0] {
      Node::Branch(ref mut b) => {
        let alloc = &mut {|bytes| self.alloc(bytes) };
        b.alloc(alloc);
      },
      _ => panic!("unexpected initial node type")
    };
    for _level in 0.. {
      if branches.is_empty() { break }
      let mut nbranches = vec![];
      for mut branch in branches {
        match branch {
          Node::Empty => {},
          Node::Data(_) => {},
          Node::Branch(ref mut b) => {
            let (data,nb) = {
              let alloc = &mut {|bytes| self.alloc(bytes) };
              b.build(alloc)?
            };
            self.store.write(b.offset as usize, &data)?;
            nbranches.extend(nb);
          }
        }
      }
      branches = nbranches;
    }
    self.flush()?;
    Ok(())
  }
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::Bounds)
  -> Result<TreeIterator<'a,'b,S,P,V>,Error> {
    TreeIterator::new(self, bbox)
  }
  fn alloc (&mut self, bytes: usize) -> u64 {
    let addr = self.size;
    //println!("ALLOC {}: addr={}",bytes, addr);
    self.size += bytes as u64;
    addr
  }
  fn flush (&mut self) -> Result<(),Error> {
    Ok(())
  }
  pub fn merge (trees: &mut Vec<Self>, dst: usize, src: Vec<usize>,
  rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    eprintln!("MERGE {} {:?} {}", dst, src, rows.len());
    for i in src.iter() {
      let blocks = trees[*i].unbuild()?;
      eprintln!("blocks={:?}", blocks);
    }
    for i in src.iter() {
      trees[*i].clear()?
    }
    trees[dst].clear()?;
    Ok(())
  }
  fn unbuild (&mut self) -> Result<Vec<(P::Bounds,u64)>,Error> {
    let mut offsets: Vec<u64> = vec![];
    let mut cursors: Vec<(u64,usize)> = vec![(0,0)];
    let bf = self.branch_factor;
    let n = bf*2-3;
    let tree_size = self.store.len()? as u64;
    while !cursors.is_empty() {
      let (c,depth) = cursors.pop().unwrap();
      let buf = read_block(&mut self.store, c, tree_size, 1024)?;
      let psize = P::pivot_size_at(depth % P::dim());
      let p_start = 0;
      let d_start = p_start + n*psize;
      let i_start = d_start + (n+bf+7)/8;
      let b_start = i_start + n*size_of::<u64>();
      let b_end = b_start+bf*size_of::<u64>();
      assert_eq!(b_end, buf.len(), "unexpected block length");
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
    let mut dstore = self.data_store.try_borrow_mut()?;
    for offset in offsets {
      let rows = dstore.list(offset)?;
      if rows.is_empty() {
        panic!["empty data block"];
      }
      match P::bounds(&rows.iter().map(|(p,_)| *p).collect()) {
        None => panic!["invalid data at offset {}", offset],
        Some(bbox) => blocks.push((bbox,offset))
      }
    }
    Ok(blocks)
  }
}
