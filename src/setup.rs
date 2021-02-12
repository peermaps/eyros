use crate::{DB,Tree,Storage,Point,Value,Error,RA};
use async_std::sync::{Arc,Mutex};

/// Struct for reading database properties.
#[derive(Debug,Clone)]
pub struct SetupFields {
  pub branch_factor: usize,
  pub max_depth: usize,
  pub max_records: usize,
  pub inline: usize,
  pub tree_cache_size: usize,
  pub rebuild_depth: usize,
}

impl SetupFields {
  pub fn default() -> Self {
    Self {
      branch_factor: 6,
      max_depth: 8,
      max_records: 20_000,
      inline: 500,
      tree_cache_size: 1000,
      rebuild_depth: 2,
    }
  }
}

/// Builder to configure and instantiate an eyros database.
///
/// The `Setup` builder lets you create a database with a more custom
/// configuration:
///
/// ```rust,no_run
/// use eyros::{DB,Coord,Tree2,Setup};
/// use random_access_disk::RandomAccessDisk;
/// use std::path::PathBuf;
///
/// type T = Tree2<f32,f32,V>;
/// type P = (Coord<f32>,Coord<f32>);
/// type V = u32;
///
/// # #[async_std::main]
/// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
/// let mut db: DB<_,T,P,V> = Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
///   .branch_factor(6)
///   .max_depth(8)
///   .max_records(20_000)
///   .inline(500)
///   .tree_cache_size(1000)
///   .rebuild_depth(2)
///   .build()
///   .await?;
/// # Ok(()) }
/// ```
#[derive(Debug,Clone)]
pub struct Setup<S> where S: RA {
  pub storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  pub fields: SetupFields
}

impl<S> Setup<S> where S: RA {
  /// Create a new `Setup` builder from a storage function.
  pub fn from_storage(storage: Box<dyn Storage<S>>) -> Self {
    Self {
      storage: Arc::new(Mutex::new(storage)),
      fields: SetupFields::default()
    }
  }
  pub fn branch_factor(mut self, bf: usize) -> Self {
    self.fields.branch_factor = bf;
    self
  }
  pub fn max_depth(mut self, md: usize) -> Self {
    self.fields.max_depth = md;
    self
  }
  pub fn max_records(mut self, mr: usize) -> Self {
    self.fields.max_records = mr;
    self
  }
  pub fn inline(mut self, n: usize) -> Self {
    self.fields.inline = n;
    self
  }
  pub fn tree_cache_size(mut self, n: usize) -> Self {
    self.fields.tree_cache_size = n;
    self
  }
  pub fn rebuild_depth(mut self, n: usize) -> Self {
    self.fields.rebuild_depth = n;
    self
  }
  pub async fn build<T,P,V> (self) -> Result<DB<S,T,P,V>,Error>
  where P: Point, V: Value, T: Tree<P,V> {
    DB::open_from_setup(self).await
  }
}
