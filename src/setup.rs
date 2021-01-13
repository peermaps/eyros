use crate::{DB,Tree,Storage,Point,Value,Error};
use random_access_storage::RandomAccess;
use async_std::sync::{Arc,Mutex};

/// Struct for reading database properties.
pub struct SetupFields {
  /*
  pub max_data_size: usize,
  pub base_size: usize,
  pub branch_factor: usize,
  pub bbox_cache_size: usize,
  pub data_list_cache_size: usize
  */
}

impl SetupFields {
  pub fn default () -> Self {
    Self {
      /*
      branch_factor: 5,
      max_data_size: 3_000,
      base_size: 9_000,
      bbox_cache_size: 10_000,
      data_list_cache_size: 16_000
      */
    }
  }
}

/// Builder to configure and instantiate an eyros database.
///
/// The `Setup` builder lets you create a database with a more custom
/// configuration:
///
/// ```rust,no_run
/// use eyros::{DB,Setup};
/// use random_access_disk::RandomAccessDisk;
/// use std::path::PathBuf;
///
/// type P = ((f32,f32),(f32,f32));
/// type V = u32;
///
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
pub struct Setup<S> where S: RandomAccess<Error=Error>+Unpin+Send+Sync {
  pub storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  pub fields: SetupFields
}

impl<S> Setup<S> where S: RandomAccess<Error=Error>+'static+Unpin+Send+Sync {
  /// Create a new `Setup` builder from a storage function.
  pub fn from_storage (storage: Box<dyn Storage<S>>) -> Self {
    Self {
      storage: Arc::new(Mutex::new(storage)),
      fields: SetupFields::default()
    }
  }
  /*
  pub fn branch_factor (mut self, bf: usize) -> Self {
    self.fields.branch_factor = bf;
    self
  }
  pub fn base_size (mut self, size: usize) -> Self {
    self.fields.base_size = size;
    self
  }
  pub fn max_data_size (mut self, size: usize) -> Self {
    self.fields.max_data_size = size;
    self
  }
  pub fn bbox_cache_size (mut self, size: usize) -> Self {
    self.fields.bbox_cache_size = size;
    self
  }
  pub fn data_list_cache_size (mut self, size: usize) -> Self {
    self.fields.data_list_cache_size = size;
    self
  }
  */
  pub async fn build<T,P,V> (self) -> Result<DB<S,T,P,V>,Error> where P: Point, V: Value, T: Tree<P,V> {
    DB::open_from_setup(self).await
  }
}
