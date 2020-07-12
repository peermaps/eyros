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
//! # fixed example
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
//! use std::path::PathBuf;
//! use async_std::prelude::*;
//!
//! type P = ((f32,f32),(f32,f32),f32);
//! type V = u32;
//! type E = Box<dyn std::error::Error+Sync+Send>;
//!
//! #[async_std::main]
//! async fn main() -> Result<(),E> {
//!   let mut db: DB<_,P,V> = DB::open_from_path(
//!     &PathBuf::from("/tmp/eyros-polygons.db")
//!   ).await?;
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
//!   db.batch(&polygons).await?;
//!
//!   let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
//!   let mut stream = db.query(&bbox).await?;
//!   while let Some(result) = stream.next().await {
//!     println!("{:?}", result?);
//!   }
//!   Ok(())
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
//!
//! # mix example
//!
//! You can also mix and match scalar and interval values for each dimension.
//!
//! An example of where these mixed types might be useful is storing geographic
//! features to display on a map. Some of the features will be points and some will
//! be lines or polygons which are contained in bounding boxes (intervals).
//!
//! This example stores 2 dimensional points and regions in the same database, so
//! that bounding box queries will return both types of features.
//!
//! ```rust,no_run
//! use eyros::{DB,Row,Mix,Mix2};
//! use rand::random;
//! use std::path::PathBuf;
//! use async_std::prelude::*;
//!
//! type P = Mix2<f32,f32>;
//! type V = u32;
//! type E = Box<dyn std::error::Error+Sync+Send>;
//!
//! #[async_std::main]
//! async fn main() -> Result<(),E> {
//!   let mut db: DB<_,P,V> = DB::open_from_path(
//!     &PathBuf::from("/tmp/eyros-mix.db")
//!   ).await?;
//!   let batch: Vec<Row<P,V>> = (0..1_000).map(|_| {
//!     let value = random::<u32>();
//!     if random::<f32>() > 0.5 {
//!       let xmin: f32 = random::<f32>()*2.0-1.0;
//!       let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
//!       let ymin: f32 = random::<f32>()*2.0-1.0;
//!       let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
//!       Row::Insert(Mix2::new(
//!         Mix::Interval(xmin,xmax),
//!         Mix::Interval(ymin,ymax)
//!       ), value)
//!     } else {
//!       let x: f32 = random::<f32>()*2.0-1.0;
//!       let y: f32 = random::<f32>()*2.0-1.0;
//!       Row::Insert(Mix2::new(
//!         Mix::Scalar(x),
//!         Mix::Scalar(y)
//!       ), value)
//!     }
//!   }).collect();
//!   db.batch(&batch).await?;
//!
//!   let bbox = ((-0.5,-0.8),(0.3,-0.5));
//!   let mut stream = db.query(&bbox).await?;
//!   while let Some(result) = stream.next().await {
//!     println!("{:?}", result?);
//!   }
//!   Ok(())
//! }
//! ```

#![recursion_limit="1024"]
#![feature(nll)]
#![feature(async_closure)]

#[macro_use] mod ensure;
mod setup;
mod meta;
mod point;
mod mix;
#[macro_use] mod tree;
mod branch;
mod staging;
mod planner;
mod order;
mod bits;
mod data;
mod read_block;
mod pivots;
mod store;

pub use crate::setup::{Setup,SetupFields};
use crate::staging::{Staging,StagingIterator};
use crate::planner::plan;
pub use crate::point::{Point,Scalar,Cursor,Block};
pub use crate::mix::{Mix,Mix2,Mix3,Mix4,Mix5,Mix6,Mix7,Mix8};
#[doc(hidden)] pub use crate::tree::{Tree,TreeStream,TreeOpts};
#[doc(hidden)] pub use crate::branch::Branch;
#[doc(hidden)] pub use crate::data::{DataStore,DataRange};
use crate::meta::Meta;
pub use order::{order,order_len};

#[cfg(not(feature="wasm"))]
pub use store::{Storage,FileStore};
#[cfg(feature="wasm")]
pub use store::Storage;

#[cfg(feature="wasm")]
mod wasm;

use random_access_storage::RandomAccess;
use failure::format_err;
pub type Error = Box<dyn std::error::Error+Sync+Send>;
use desert::{ToBytes,FromBytes,CountBytes};
use std::fmt::Debug;
use async_std::{sync::{Arc,Mutex}};
use std::collections::HashSet;

use std::pin::Pin;
use async_std::{prelude::*,stream::Stream};
mod unfold;
use unfold::unfold;

#[doc(hidden)]
pub enum SubStream<P,V> where P: Point, V: Value {
  Tree(Pin<Box<dyn Stream<Item=Result<(P,V,Location),Error>>>>),
  Staging(StagingIterator<P,V>)
}

/// Data to use for the payload portion stored at a coordinate.
pub trait Value: Debug+Clone+Send+Sync
  +ToBytes+FromBytes+CountBytes {}
impl<T> Value for T where T: Debug+Clone+Send+Sync
  +ToBytes+FromBytes+CountBytes {}

/// Stores where a record is stored to avoid additional queries during deletes.
/// Locations are only valid until the next `batch()`. There is no runtime check
/// yet to ensure that batches will invalidate existing locations, so you will
/// need to be careful of this yourself. Otherwise the wrong data could be
/// deleted.
pub type Location = (u64,u32);

/// Container to insert or delete data for a `batch()`.
#[derive(Clone,Debug)]
pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(Location)
}

/// Top-level database API.
pub struct DB<S,P,V> where
S: RandomAccess<Error=Error>+Send+Sync+Unpin,
P: Point, V: Value {
  storage: Box<dyn Storage<S>>,
  pub trees: Vec<Arc<Mutex<Tree<S,P,V>>>>,
  pub staging: Staging<S,P,V>,
  pub data_store: Arc<Mutex<DataStore<S,P,V>>>,
  meta: Meta<S>,
  pub fields: SetupFields
}

impl<S,P,V> DB<S,P,V> where
S: RandomAccess<Error=Error>+Send+Sync+'static+Unpin,
P: Point+'static, V: Value+'static {
  /// Create a new database instance from `storage`, a struct that implements
  /// the `eyros::Storage` trait. Storage providers have an `.open()` method
  /// which returns a new `RandomAccess` instance for a given string. Often
  /// these strings will correspond to files under a sub-directory.
  /// The database will be created with the default configuration.
  ///
  /// For example:
  ///
  /// ```rust,no_run
  /// use eyros::DB;
  /// use random_access_disk::RandomAccessDisk;
  /// use std::path::PathBuf;
  /// use async_std::prelude::*;
  ///
  /// type P = ((f32,f32),(f32,f32));
  /// type V = u32;
  /// type E = Box<dyn std::error::Error+Sync+Send>;
  ///
  /// #[async_std::main]
  /// async fn main () -> Result<(),E> {
  ///   let mut db: DB<_,P,V> = DB::open_from_storage(
  ///     Box::new(DiskStore { path: PathBuf::from("/tmp/eyros-db/") })
  ///   ).await?;
  ///   // ...
  ///   Ok(())
  /// }
  ///
  /// struct DiskStore { path: PathBuf }
  ///
  /// #[async_trait::async_trait]
  /// impl eyros::Storage<RandomAccessDisk> for DiskStore {
  ///   async fn open (&mut self, name: &str) -> Result<RandomAccessDisk,E> {
  ///     let mut p = self.path.join(PathBuf::from(name));
  ///     Ok(RandomAccessDisk::builder(p).auto_sync(false).build().await?)
  ///   }
  /// }
  /// ```
  pub async fn open_from_storage(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    Setup::from_storage(storage).build().await
  }

  /// Create a new database instance from `setup`, a configuration builder.
  ///
  /// ```rust,no_run
  /// # use eyros::{DB,Setup};
  /// # use std::path::PathBuf;
  /// # #[async_std::main]
  /// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
  /// # type P = ((f32,f32),(f32,f32));
  /// # type V = u32;
  /// let mut db: DB<_,P,V> = DB::open_from_setup(
  ///   Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
  ///     .branch_factor(5)
  ///     .max_data_size(3_000)
  ///     .base_size(1_000)
  /// ).await?;
  /// # Ok(()) }
  /// ```
  ///
  /// You can also use `Setup`'s `.build()?` method to get a `DB` instance:
  ///
  /// ```rust,no_run
  /// use eyros::{DB,Setup};
  /// # use std::path::PathBuf;
  ///
  /// # type P = ((f32,f32),(f32,f32));
  /// # type V = u32;
  /// # #[async_std::main]
  /// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
  /// let mut db: DB<_,P,V> = Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
  ///   .branch_factor(5)
  ///   .max_data_size(3_000)
  ///   .base_size(1_000)
  ///   .build()
  ///   .await?;
  /// # Ok(()) }
  /// ```
  ///
  /// Always open a database with the same settings. Things will break if you
  /// change . There is no runtime check yet to ensure a database is opened with
  /// the same configuration that it was created with.
  pub async fn open_from_setup(mut setup: Setup<S>) -> Result<Self,Error> {
    let meta = Meta::open(setup.storage.open("meta").await?).await?;
    let staging = Staging::open(
      setup.storage.open("staging_inserts").await?,
      setup.storage.open("staging_deletes").await?
    ).await?;
    let data_store = DataStore::open(
      setup.storage.open("data").await?,
      setup.storage.open("range").await?,
      setup.fields.max_data_size,
      setup.fields.bbox_cache_size,
      setup.fields.data_list_cache_size
    )?;
    let mut db = Self {
      storage: setup.storage,
      staging,
      data_store: Arc::new(Mutex::new(data_store)),
      meta: meta,
      trees: vec![],
      fields: setup.fields
    };
    for i in 0..db.meta.mask.len() {
      db.create_tree(i).await?;
    }
    Ok(db)
  }

  /// Write a collection of updates to the database. Each update can be a
  /// `Row::Insert(point,value)` or a `Row::Delete(location)`.
  pub async fn batch (&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
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
    let (slen,n,ndel) = {
      let s_inserts = self.staging.inserts.lock().await;
      let s_deletes = self.staging.deletes.lock().await;
      let slen = s_inserts.len();
      let n = (slen + inserts.len()) as u64;
      let ndel = (s_deletes.len() + deletes.len()) as u64;
      (slen,n,ndel)
    };
    let base = self.fields.base_size as u64;
    if ndel >= base && n <= base {
      deletes.extend_from_slice(&self.staging.deletes.lock().await);
      let mut dstore = self.data_store.lock().await;
      dstore.delete(&deletes).await?;
      dstore.commit().await?;
      self.staging.batch(&inserts, &vec![]).await?;
      self.staging.delete(&deletes).await?;
      self.staging.clear_deletes().await?;
      self.staging.commit().await?;
      return Ok(())
    } else if n <= base {
      self.staging.batch(&inserts, &deletes).await?;
      self.staging.commit().await?;
      return Ok(())
    }
    let count = (n/base)*base;
    let rem = n - count;
    let mut mask = vec![];
    for tree in self.trees.iter_mut() {
      mask.push(!tree.lock().await.is_empty().await?);
    }
    let p = plan(
      &bits::num_to_bits(n/base),
      &mask
    );
    let mut offset = 0;
    for (i,staging,trees) in p {
      let mut irows: Vec<(usize,usize)> = vec![];
      for j in staging {
        let size = (2u64.pow(j as u32) * base) as usize;
        irows.push((offset,offset+size));
        offset += size;
      }
      for t in trees.iter() {
        self.create_tree(*t).await?;
      }
      self.create_tree(i).await?;
      for _ in self.meta.mask.len()..i+1 {
        self.meta.mask.push(false);
      }
      let mut srows: Vec<(P,V)> = vec![];
      {
        let s_inserts = self.staging.inserts.lock().await;
        for (i,j) in irows {
          for k in i..j {
            srows.push(
              if k < slen { s_inserts[k].clone() }
              else { inserts[k-slen].clone() }
            );
          }
        }
      }
      if trees.is_empty() {
        self.meta.mask[i] = true;
        self.trees[i].lock().await.build(&srows).await?;
      } else {
        self.meta.mask[i] = true;
        for t in trees.iter() {
          self.meta.mask[*t] = false;
        }
        Tree::merge(&mut self.trees, i, trees, &srows).await?;
      }
    }
    ensure_eq_box!(n-(offset as u64), rem, "offset-n ({}-{}={}) != rem ({}) ",
      offset, n, (offset as u64)-n, rem);
    let mut rem_rows = vec![];
    {
      let s_inserts = self.staging.inserts.lock().await;
      for k in offset..n as usize {
        rem_rows.push(
          if k < slen { s_inserts[k].clone() }
          else { inserts[k-slen].clone() }
        );
      }
    }
    ensure_eq_box!(rem_rows.len(), rem as usize,
      "unexpected number of remaining rows (expected {}, actual {})",
      rem, rem_rows.len());
    deletes.extend_from_slice(&self.staging.deletes.lock().await);
    self.staging.clear().await?;
    self.staging.batch(&rem_rows, &vec![]).await?;
    self.staging.delete(&deletes).await?;
    self.staging.commit().await?;
    if !deletes.is_empty() {
      let mut dstore = self.data_store.lock().await;
      dstore.delete(&deletes).await?;
      dstore.commit().await?;
    }
    self.meta.save().await?;
    Ok(())
  }

  async fn create_tree (&mut self, index: usize) -> Result<(),Error> {
    for i in self.trees.len()..index+1 {
      let store = self.storage.open(&format!("tree{}",i)).await?;
      self.trees.push(Arc::new(Mutex::new(Tree::open(TreeOpts {
        store,
        index,
        data_store: Arc::clone(&self.data_store),
        branch_factor: self.fields.branch_factor,
        max_data_size: self.fields.max_data_size,
      }).await?)));
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
  /// You will receive a stream of `Result<(P,V,Location),Error>` results
  /// that you can step through like this:
  ///
  /// ```rust,no_run
  /// # use eyros::DB;
  /// # use std::path::PathBuf;
  /// # use random_access_disk::RandomAccessDisk;
  /// # use async_std::prelude::*;
  /// # #[async_std::main]
  /// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
  /// # let mut db: DB<_,((f32,f32),(f32,f32)),u32> = DB::open_from_path(
  /// #   &PathBuf::from("/tmp/eyros-db/")).await?;
  /// let bbox = ((-0.5,-0.8),(0.3,-0.5));
  /// let mut stream = db.query(&bbox).await?;
  /// while let Some(result) = stream.next().await {
  ///   let (point,value,location) = result?;
  ///   // ...
  /// }
  /// # Ok(()) }
  /// ```
  ///
  /// If you want to delete records, you will need to use the `Location` records
  /// you get from a query. However, these locations are only valid until the
  /// next `.batch()`.
  pub async fn query (&mut self, bbox: &P::Bounds)
  -> Result<Box<impl Stream<Item=Result<(P,V,Location),Error>>>,Error> {
    let mut mask: Vec<bool> = vec![];
    for tree in self.trees.iter_mut() {
      mask.push(!tree.lock().await.is_empty().await?);
    }
    let rbox = Arc::new(bbox.clone());
    let mut queries = Vec::with_capacity(1+self.trees.len());
    queries.push(SubStream::Staging(self.staging.query(Arc::clone(&rbox))));
    for (i,tree) in self.trees.iter_mut().enumerate() {
      if !mask[i] { continue }
      queries.push(SubStream::Tree(Box::pin(Tree::query(
        Arc::clone(tree),
        Arc::clone(&rbox)
      ).await?)));
    }
    let qs = QueryStream::new(queries, Arc::clone(&self.staging.delete_set))?;
    Ok(Box::new(unfold(qs, async move |mut qs| {
      let res = qs.get_next().await;
      match res {
        Some(p) => Some((p,qs)),
        None => None
      }
    })))
  }
}

type Out<P,V> = Option<Result<(P,V,Location),Error>>;

pin_project_lite::pin_project! {
  /// Stream of `Result<(Point,Value,Location)>` data returned by `db.query()`.
  pub struct QueryStream<P,V> where P: Point, V: Value {
    index: usize,
    #[pin]
    queries: Vec<SubStream<P,V>>,
    deletes: Arc<Mutex<HashSet<Location>>>
  }
}

impl<P,V> QueryStream<P,V> where P: Point, V: Value {
  pub fn new (queries: Vec<SubStream<P,V>>,
  deletes: Arc<Mutex<HashSet<Location>>>) -> Result<Self,Error> {
    Ok(Self {
      deletes,
      queries,
      index: 0,
    })
  }
  async fn get_next (&mut self) -> Out<P,V> {
    while !self.queries.is_empty() {
      let len = self.queries.len();
      {
        let ix = self.index;
        let q = &mut self.queries[ix];
        let next = match q {
          SubStream::Tree(x) => {
            let result = x.next().await;
            match result {
              Some(Err(e)) => return Some(Err(e.into())),
              Some(Ok((_,_,loc))) => {
                if self.deletes.lock().await.contains(&loc) {
                  self.index = (self.index+1) % len;
                  continue;
                }
              },
              _ => {}
            };
            result
          },
          SubStream::Staging(x) => x.next().await
        };
        match next {
          Some(result) => {
            self.index = (self.index+1) % len;
            return Some(result);
          },
          None => {}
        }
      }
      let ix = self.index;
      self.queries.remove(ix);
      if self.queries.len() > 0 {
        self.index = self.index % self.queries.len();
      }
    }
    None
  }
}
