use random_access_storage::RandomAccess;
use failure::{Error,bail};
use std::marker::PhantomData;
use std::cell::RefCell;
use std::rc::Rc;
use std::mem::size_of;
use std::cmp::Ordering;

use ::{Row,Point,Value};

use branch::{Branch,Node};
use data::DataStore;

pub struct TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  tree: &'a mut Tree<S,P,V>,
  bbox: &'b P::BBox,
  cursors: Vec<(u64,usize)>,
  blocks: Vec<u64>,
  queue: Vec<(P,V)>,
  tree_size: u64
}

impl<'a,'b,S,P,V> TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (tree: &'a mut Tree<S,P,V>, bbox: &'b P::BBox)
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
    while !self.cursors.is_empty() || !self.blocks.is_empty() {
      if !self.queue.is_empty() {
        return Some(Ok(self.queue.pop().unwrap()))
      }
      if !self.blocks.is_empty() { // data block:
        let offset = self.blocks.pop().unwrap();
        println!("BLOCK {}", offset);
        let mut dstore = iwrap![
          self.tree.data_store.try_borrow_mut()
        ];
        self.queue.extend(iwrap![dstore.query(offset, self.bbox)]);
        continue
      }
      // branch block:
      let (cursor,depth) = self.cursors.pop().unwrap();
      if cursor >= self.tree_size { continue }
      let size_guess = 1024.min(self.tree_size-cursor);
      let fbuf: Vec<u8> = iwrap![
        store.read(cursor as usize, size_guess as usize)
      ];
      let len = u32::from_be_bytes([fbuf[0],fbuf[1],fbuf[2],fbuf[3]]) as u64;
      let mut buf = Vec::with_capacity(len as usize);
      match size_guess.cmp(&len) {
        Ordering::Equal => {
          buf = fbuf;
        },
        Ordering::Greater => {
          buf.extend_from_slice(&fbuf[0..len as usize])
        },
        Ordering::Less => {
          buf.extend(fbuf);
          buf.extend(iwrap![store.read(
            (cursor+len) as usize,
            (len-size_guess) as usize
          )]);
        }
      };
      println!("{}:buf={:?}", cursor, buf);
      let psize = P::pivot_size_at(depth % P::dim());
      let p_start = size_of::<u32>();
      let d_start = p_start + n*psize;
      let i_start = d_start + (n+bf+7)/8;
      let b_start = i_start + n*size_of::<u64>();
      let b_end = b_start+bf*size_of::<u64>();
      assert_eq!(b_end as u64,len, "unexpected block length");

      let mut bcursors = vec![0];
      while !bcursors.is_empty() {
        let c = bcursors.pop().unwrap();
        if c >= n {
          continue;
        }
        let i = order[c];
        let cmp: (bool,bool) = iwrap![P::cmp_buf(
          &buf[p_start+i*psize..p_start+(i+1)*psize],
          &self.bbox,
          depth % P::dim()
        )];
        let is_data = ((buf[d_start+(i+7)/8]>>(i%8))&1) == 1;
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
        if cmp.0 { // left
          bcursors.push(c*2+1);
        }
        if cmp.1 { // right
          bcursors.push(c*2+2);
        }

        /*
        if c >= bf+n { continue }
        if c >= n {
          let j = order[(c-1)/2];
          let is_data = ((buf[d_start+(j+7)/8]>>(j%8))&1) == 1;
          let offset = u64::from_be_bytes([
            buf[b_start+j*8+0], buf[b_start+j*8+1],
            buf[b_start+j*8+2], buf[b_start+j*8+3],
            buf[b_start+j*8+4], buf[b_start+j*8+5],
            buf[b_start+j*8+6], buf[b_start+j*8+7]
          ]);
          if offset > 0 && is_data {
            println!("BLOCK={} j={}", offset, j);
            self.blocks.push(offset-1);
          } else if offset > 0 {
            println!("BRANCH={} j={}", offset, j);
            self.cursors.push((offset-1,depth+1));
          }
          continue
        }
        */
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
          Node::Data(d) => {},
          Node::Branch(ref mut b) => {
            let (data,nb) = {
              let alloc = &mut {|bytes| self.alloc(bytes) };
              b.build(alloc)?
            };
            println!("WRITE {}", b.offset);
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
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::BBox)
  -> Result<TreeIterator<'a,'b,S,P,V>,Error> {
    TreeIterator::new(self, bbox)
  }
  fn alloc (&mut self, bytes: usize) -> u64 {
    let addr = self.size;
    self.size += bytes as u64;
    addr
  }
  fn flush (&mut self) -> Result<(),Error> {
    Ok(())
  }
  pub fn merge (trees: &mut Vec<Self>, dst: usize, src: Vec<usize>,
  rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    println!("MERGE {} {:?} {}", dst, src, rows.len());
    // TODO
    for i in src { trees[i].clear()? }
    trees[dst].clear()?;
    Ok(())
  }
}
