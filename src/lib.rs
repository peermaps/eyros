//! # eyros
//!
//! eyros (εύρος) is a multi-dimensional interval database.
//!
//! The database is based on [bkd][] and [interval][] trees.
//!
//! * high batch-write performance (expect 100,000s to 1,000,000s writes per second
//!   on modest hardware)
//! * designed for peer-to-peer distribution and query-driven sparse replication
//! * compiles to web assembly for use in the browser
//! * good for geospatial and time-series data
//!
//! eyros operates on scalar (x) or interval (min,max) coordinates for each
//! dimension. There are 2 operations: batched write (for inserting and deleting)
//! and query by bounding box. All features that intersect the bounding box are
//! returned in the query results.
//!
//! This is an early release missing important features such as atomicity and
//! concurrency. The data format is still in flux and will likely change in the
//! future, requiring data migrations.
//!
//! [bkd]: https://users.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf
//! [interval]: http://www.dgp.toronto.edu/~jstewart/378notes/22intervals/
//!
//! # example
//!
//! This example generates 800 random features in 3 dimensions: `x`, `y`, and `time`
//! with a `u32` `value` payload. The `x` and `y` dimensions are both intervals with
//! a minimum and maximum `f32` and `time` is a scalar `f32`.
//!
//! After the data is written to the database, all features with an `x` interval
//! that overlaps `(-0.5,0.3)`, a `y` interval that overlaps `(-0.8,-0.5)`, and a
//! `time` scalar that is between `0.0` and `100.0` are printed to stdout.
//!
//! ```rust,no_run
//! use eyros::{DB,Row};
//! use rand::random;
//! use failure::Error;
//! use random_access_disk::RandomAccessDisk;
//! use std::path::PathBuf;
//!
//! type P = ((f32,f32),(f32,f32),f32);
//! type V = u32;
//!
//! fn main() -> Result<(),Error> {
//!   let mut db: DB<_,_,((f32,f32),(f32,f32),f32),u32> = DB::open(storage)?;
//!   let polygons: Vec<Row<P,V>> = (0..800).map(|_| {
//!     let xmin: f32 = random::<f32>()*2.0-1.0;
//!     let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
//!     let ymin: f32 = random::<f32>()*2.0-1.0;
//!     let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
//!     let time: f32 = random::<f32>()*1000.0;
//!     let value: u32 = random();
//!     let point = ((xmin,xmax),(ymin,ymax),time);
//!     Row::Insert(point, value)
//!   }).collect();
//!   db.batch(&polygons)?;
//!
//!   let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
//!   for result in db.query(&bbox)? {
//!     println!("{:?}", result?);
//!   }
//!   Ok(())
//! }
//!
//! fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
//!   let mut p = PathBuf::from("/tmp/eyros-db/");
//!   p.push(name);
//!   Ok(RandomAccessDisk::builder(p)
//!     .auto_sync(false)
//!     .build()?)
//! }
//! ```
//!
//! The output from this program is of the form `(coords, value, location)`:
//!
//! ```sh
//! $ cargo run --example polygons -q
//! (((-0.014986515, -0.014986515), (-0.5801666, -0.5801663), 45.314373), 1518966744, (0, 200))
//! (((-0.0892005, -0.015534878), (-0.65783, -0.65783), 3.6987066), 66257667, (0, 267))
//! (((0.1931547, 0.1931547), (-0.6388786, -0.60205233), 67.85113), 2744609531, (0, 496))
//! (((-0.28907382, -0.26248854), (-0.7761978, -0.77617484), 55.273056), 3622408505, (0, 651))
//! (((-0.080417514, -0.080417514), (-0.60076225, -0.5929384), 29.592216), 722871034, (0, 784))
//! (((0.14104307, 0.14104307), (-0.539363, -0.539363), 31.965792), 2866780128, (0, 933))
//! (((-0.12689173, -0.12689173), (-0.56708515, -0.56643564), 65.072), 1858542500, (0, 983))
//! (((-0.12520671, -0.1250745), (-0.6836084, -0.6836084), 93.58209), 3942792215, (0, 1019))
//! (((0.026417613, 0.026417613), (-0.786397, -0.786397), 61.52451), 1197187917, (0, 1102))
//! (((-0.18799019, -0.18799017), (-0.50418067, -0.50418067), 82.93134), 2811117540, (0, 1199))
//! (((-0.34033966, -0.34033966), (-0.53603613, -0.53603613), 91.07471), 302136936, (0, 1430))
//! (((-0.008744121, 0.54438573), (-0.73665094, -0.73665094), 69.67532), 719725479, (0, 1504))
//! (((-0.38071227, -0.38071224), (-0.75237143, -0.75237143), 72.245895), 2200140390, (0, 1628))
//! (((0.020396352, 0.020396352), (-0.7957357, -0.77274036), 40.785194), 2166765724, (0, 1708))
//! (((0.117452025, 0.117452025), (-0.7027955, -0.7026706), 82.033394), 2451987859, (0, 1886))
//! (((-0.11418259, -0.11418259), (-0.74327374, -0.74327374), 28.591274), 4283568770, (0, 1983))
//! (((-0.19130886, -0.19130856), (-0.7012402, -0.7012042), 2.1106005), 4226013993, (0, 2048))
//! (((-0.3000791, -0.3000791), (-0.7601782, -0.7601782), 24.528027), 2776778380, (0, 2349))
//! ```
//!
//! The `coords` and `value` are the values that were written earlier: in this case,
//! the coords are `((xmin,xmax),(ymin,ymax),time)`.
//!
//! The `location` is used to quickly delete records without needing to perform
//! additional lookups. You'll need to keep the `location` around from the result of
//! a query when you intend to delete a record. Locations that begin with a `0` are
//! stored in the staging cache, so their location may change after the next write.

#![recursion_limit="1024"]

#[macro_use] mod ensure;
mod setup;
mod meta;
mod point;
#[macro_use] mod tree;
mod branch;
mod staging;
mod planner;
mod bits;
mod order;
mod data;
mod read_block;
mod pivots;
mod write_cache;
mod take_bytes;

pub use crate::setup::{Setup,SetupFields};
use crate::staging::{Staging,StagingIterator};
use crate::planner::plan;
pub use crate::point::{Point,Scalar};
#[doc(hidden)] pub use crate::tree::{Tree,TreeIterator,TreeOpts};
#[doc(hidden)] pub use crate::branch::Branch;
use crate::order::pivot_order;
#[doc(hidden)] pub use crate::data::{DataStore,DataRange};
use crate::take_bytes::TakeBytes;
use crate::meta::Meta;

use random_access_storage::RandomAccess;
use failure::{Error,format_err};
use serde::{Serialize,de::DeserializeOwned};
use std::fmt::Debug;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashSet;

#[doc(hidden)]
pub enum SubIterator<'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  Tree(TreeIterator<'b,S,P,V>),
  Staging(StagingIterator<'b,P,V>)
}

/// Data to use for the payload portion stored at a coordinate.
pub trait Value: Debug+Clone+TakeBytes+Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Debug+Clone+TakeBytes+Serialize+DeserializeOwned+'static {}

/// Stores where a record is stored to avoid additional queries during deletes.
/// Locations are only valid until the next `batch()`. There is no runtime check
/// yet to ensure that batches will invalidate existing locations, so you will
/// need to be careful of this yourself. Otherwise the wrong data could be
/// deleted.
pub type Location = (u64,usize);

/// Container to insert or delete data for a `batch()`.
#[derive(Clone,Debug)]
pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(Location)
}

/// Top-level database API.
pub struct DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  open_store: U,
  pub trees: Vec<Rc<RefCell<Tree<S,P,V>>>>,
  order: Rc<Vec<usize>>,
  pub staging: Staging<S,P,V>,
  pub data_store: Rc<RefCell<DataStore<S,P,V>>>,
  meta: Meta<S>,
  pub fields: SetupFields,
  pub bincode: Rc<bincode::Config>
}

impl<S,U,P,V> DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  /// Create a new database instance from `open_store`, a function that receives
  /// a string path as an argument and returns a Result with a RandomAccess
  /// store. The database will be created with the default configuration.
  ///
  /// For example:
  ///
  /// ```rust,no_run
  /// use eyros::DB;
  /// use random_access_disk::RandomAccessDisk;
  /// use std::path::PathBuf;
  /// use failure::Error;
  ///
  /// type P = ((f32,f32),(f32,f32));
  /// type V = u32;
  ///
  /// fn main () -> Result<(),Error> {
  ///   let mut db: DB<_,_,P,V> = DB::open(storage)?;
  ///   // ...
  ///   Ok(())
  /// }
  ///
  /// fn storage (name: &str) -> Result<RandomAccessDisk,Error> {
  ///   let mut p = PathBuf::from("/tmp/eyros-db/");
  ///   p.push(name);
  ///   Ok(RandomAccessDisk::builder(p).auto_sync(false).build()?)
  /// }
  /// ```
  pub fn open(open_store: U) -> Result<Self,Error> {
    Setup::new(open_store).build()
  }

  /// Create a new database instance from `setup`, a configuration builder.
  ///
  /// ```rust,no_run
  /// # use eyros::{DB,Setup};
  /// # use failure::Error;
  /// # use random_access_disk::RandomAccessDisk;
  /// # use std::path::PathBuf;
  /// # fn main () -> Result<(),Error> {
  /// # type P = ((f32,f32),(f32,f32));
  /// # type V = u32;
  /// let mut db: DB<_,_,P,V> = DB::open_from_setup(
  ///   Setup::new(storage)
  ///     .branch_factor(5)
  ///     .max_data_size(3_000)
  ///     .base_size(1_000)
  /// )?;
  /// # Ok(()) }
  /// #
  /// # fn storage(name: &str) -> Result<RandomAccessDisk,Error> {
  /// #   let mut p = PathBuf::from("/tmp/eyros-db/");
  /// #   p.push(name);
  /// #   Ok(RandomAccessDisk::builder(p).auto_sync(false).build()?)
  /// # }
  /// ```
  ///
  /// You can also use `Setup`'s `.build()?` method to get a `DB` instance:
  ///
  /// ```rust,no_run
  /// use eyros::{DB,Setup};
  /// # use failure::Error;
  /// # use std::path::PathBuf;
  /// # use random_access_disk::RandomAccessDisk;
  ///
  /// # type P = ((f32,f32),(f32,f32));
  /// # type V = u32;
  /// # fn main () -> Result<(),Error> {
  /// let mut db: DB<_,_,P,V> = Setup::new(storage)
  ///   .branch_factor(5)
  ///   .max_data_size(3_000)
  ///   .base_size(1_000)
  ///   .build()?;
  /// # Ok(()) }
  /// #
  /// # fn storage(name: &str) -> Result<RandomAccessDisk,Error> {
  /// #   let mut p = PathBuf::from("/tmp/eyros-db/");
  /// #   p.push(name);
  /// #   Ok(RandomAccessDisk::builder(p).auto_sync(false).build()?)
  /// # }
  /// ```
  ///
  /// Always open a database with the same settings. Things will break if you
  /// change . There is no runtime check yet to ensure a database is opened with
  /// the same configuration that it was created with.
  pub fn open_from_setup(setup: Setup<S,U>) -> Result<Self,Error> {
    let meta = Meta::open((setup.open_store)("meta")?)?;
    let staging = Staging::open(
      (setup.open_store)("staging_inserts")?,
      (setup.open_store)("staging_deletes")?
    )?;
    let mut bcode = bincode::config();
    bcode.big_endian();
    let r_bcode = Rc::new(bcode);
    let data_store = DataStore::open(
      (setup.open_store)("data")?,
      (setup.open_store)("range")?,
      setup.fields.max_data_size,
      setup.fields.bbox_cache_size,
      setup.fields.data_list_cache_size,
      Rc::clone(&r_bcode)
    )?;
    let bf = setup.fields.branch_factor;
    let mut db = Self {
      open_store: setup.open_store,
      staging,
      bincode: Rc::clone(&r_bcode),
      data_store: Rc::new(RefCell::new(data_store)),
      order: Rc::new(pivot_order(bf)),
      meta: meta,
      trees: vec![],
      fields: setup.fields
    };
    for i in 0..db.meta.mask.len() {
      db.create_tree(i)?;
    }
    Ok(db)
  }

  /// Write a collection of updates to the database. Each update can be a
  /// `Row::Insert(point,value)` or a `Row::Delete(location)`.
  pub fn batch (&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    let inserts: Vec<(P,V)> = rows.iter()
      .filter(|r| match r { Row::Insert(_p,_v) => true, _ => false })
      .map(|r| match r {
        Row::Insert(p,v) => (p.clone(),v.clone()),
        _ => panic!["unexpected non-insert row type"]
      })
      .collect();
    let mut deletes: Vec<Location> = rows.iter()
      .filter(|r| match r { Row::Delete(_loc) => true, _ => false })
      .map(|r| match r {
        Row::Delete(loc) => *loc,
        _ => panic!["unexpected non-delete row type"]
      })
      .collect();
    let n = (self.staging.inserts.try_borrow()?.len()+inserts.len()) as u64;
    let ndel = (self.staging.deletes.try_borrow()?.len()+deletes.len()) as u64;
    let base = self.fields.base_size as u64;
    if ndel >= base && n <= base {
      deletes.extend_from_slice(&self.staging.deletes.try_borrow()?);
      let mut dstore = self.data_store.try_borrow_mut()?;
      dstore.delete(&deletes)?;
      dstore.commit()?;
      self.staging.batch(&inserts, &vec![])?;
      self.staging.delete(&deletes)?;
      self.staging.clear_deletes()?;
      self.staging.commit()?;
      return Ok(())
    } else if n <= base {
      self.staging.batch(&inserts, &deletes)?;
      self.staging.commit()?;
      return Ok(())
    }
    let count = (n/base)*base;
    let rem = n - count;
    let mut mask = vec![];
    for tree in self.trees.iter_mut() {
      mask.push(!tree.try_borrow_mut()?.is_empty()?);
    }
    let p = plan(
      &bits::num_to_bits(n/base),
      &mask
    );
    let mut offset = 0;
    let slen = self.staging.inserts.try_borrow()?.len();
    for (i,staging,trees) in p {
      let mut irows: Vec<(usize,usize)> = vec![];
      for j in staging {
        let size = (2u64.pow(j as u32) * base) as usize;
        irows.push((offset,offset+size));
        offset += size;
      }
      for t in trees.iter() {
        self.create_tree(*t)?;
      }
      self.create_tree(i)?;
      for _ in self.meta.mask.len()..i+1 {
        self.meta.mask.push(false);
      }
      let mut srows: Vec<(P,V)> = vec![];
      for (i,j) in irows {
        for k in i..j {
          srows.push(
            if k < slen { self.staging.inserts.try_borrow()?[k].clone() }
            else { inserts[k-slen].clone() }
          );
        }
      }
      if trees.is_empty() {
        self.meta.mask[i] = true;
        self.trees[i].try_borrow_mut()?.build(&srows)?;
      } else {
        self.meta.mask[i] = true;
        for t in trees.iter() {
          self.meta.mask[*t] = false;
        }
        Tree::merge(&mut self.trees, i, trees, &srows)?;
      }
    }
    ensure_eq!(n-(offset as u64), rem, "offset-n ({}-{}={}) != rem ({}) ",
      offset, n, (offset as u64)-n, rem);
    let mut rem_rows = vec![];
    for k in offset..n as usize {
      rem_rows.push(
        if k < slen { self.staging.inserts.try_borrow()?[k].clone() }
        else { inserts[k-slen].clone() }
      );
    }
    ensure_eq!(rem_rows.len(), rem as usize,
      "unexpected number of remaining rows (expected {}, actual {})",
      rem, rem_rows.len());
    deletes.extend_from_slice(&self.staging.deletes.try_borrow()?);
    self.staging.clear()?;
    self.staging.batch(&rem_rows, &vec![])?;
    self.staging.delete(&deletes)?;
    self.staging.commit()?;
    if !deletes.is_empty() {
      let mut dstore = self.data_store.try_borrow_mut()?;
      dstore.delete(&deletes)?;
      dstore.commit()?;
    }
    self.meta.save()?;
    Ok(())
  }

  fn create_tree (&mut self, index: usize) -> Result<(),Error> {
    for i in self.trees.len()..index+1 {
      let store = (self.open_store)(&format!("tree{}",i))?;
      self.trees.push(Rc::new(RefCell::new(Tree::open(TreeOpts {
        store,
        index,
        data_store: Rc::clone(&self.data_store),
        order: Rc::clone(&self.order),
        bincode: Rc::clone(&self.bincode),
        branch_factor: self.fields.branch_factor,
        max_data_size: self.fields.max_data_size,
      })?)));
    }
    Ok(())
  }

  /// Query the database for all records that intersect the bounding box.
  ///
  /// The bounding box is a 2-tuple of n-tuples (for an n-dimensional point
  /// type) representing the `(min,max)` coordinates. In 2d with a conventional
  /// x-y cartesian grid, the first bbox point would be the "bottom-left" (or
  /// west-south) and the second point would be the "top-right" (or east-north).
  ///
  /// You will receive an iterator of `Result<(P,V,Location),Error>` results
  /// that you can step through like this:
  ///
  /// ```rust,no_run
  /// # use eyros::DB;
  /// # use failure::Error;
  /// # use std::path::PathBuf;
  /// # use random_access_disk::RandomAccessDisk;
  /// # fn main () -> Result<(),Error> {
  /// # let mut db: DB<_,_,((f32,f32),(f32,f32)),u32> = DB::open(storage)?;
  /// let bbox = ((-0.5,-0.8),(0.3,-0.5));
  /// for result in db.query(&bbox)? {
  ///   let (point,value,location) = result?;
  ///   // ...
  /// }
  /// # Ok(()) }
  /// # fn storage(name: &str) -> Result<RandomAccessDisk,Error> {
  /// #   let mut p = PathBuf::from("/tmp/eyros-db/");
  /// #   p.push(name);
  /// #   Ok(RandomAccessDisk::builder(p).auto_sync(false).build()?)
  /// # }
  /// ```
  ///
  /// If you want to delete records, you will need to use the `Location` records
  /// you get from a query. However, these locations are only valid until the
  /// next `.batch()`.
  pub fn query<'b> (&mut self, bbox: &'b P::Bounds)
  -> Result<QueryIterator<'b,S,P,V>,Error> {
    let mut mask: Vec<bool> = vec![];
    for tree in self.trees.iter_mut() {
      mask.push(!tree.try_borrow_mut()?.is_empty()?);
    }
    let mut queries = Vec::with_capacity(1+self.trees.len());
    queries.push(SubIterator::Staging(self.staging.query(bbox)));
    for (i,tree) in self.trees.iter_mut().enumerate() {
      if !mask[i] { continue }
      queries.push(SubIterator::Tree(Tree::query(Rc::clone(tree),bbox)?));
    }
    QueryIterator::new(queries, Rc::clone(&self.staging.delete_set))
  }
}

/// Iterator of `Result<(Point,Value,Location)>` data returned by `db.query()`.
pub struct QueryIterator<'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  index: usize,
  queries: Vec<SubIterator<'b,S,P,V>>,
  deletes: Rc<RefCell<HashSet<Location>>>
}

impl<'b,S,P,V> QueryIterator<'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (queries: Vec<SubIterator<'b,S,P,V>>,
  deletes: Rc<RefCell<HashSet<Location>>>) -> Result<Self,Error> {
    Ok(Self { deletes, queries, index: 0 })
  }
}

impl<'b,S,P,V> Iterator for QueryIterator<'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V,Location),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    while !self.queries.is_empty() {
      let len = self.queries.len();
      {
        let q = &mut self.queries[self.index];
        let next = match q {
          SubIterator::Tree(x) => {
            let result = x.next();
            match &result {
              Some(Ok((_,_,loc))) => {
                if iwrap![self.deletes.try_borrow()].contains(loc) {
                  self.index = (self.index+1) % len;
                  continue;
                }
              },
              _ => {}
            };
            result
          },
          SubIterator::Staging(x) => x.next()
        };
        match next {
          Some(result) => {
            self.index = (self.index+1) % len;
            return Some(result);
          },
          None => {}
        }
      }
      self.queries.remove(self.index);
      if self.queries.len() > 0 {
        self.index = self.index % self.queries.len();
      }
    }
    None
  }
}
