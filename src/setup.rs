use crate::{DB,Point,Value};
use failure::Error;
use random_access_storage::RandomAccess;
use async_std::future::Future;

/// Struct for reading database properties.
pub struct SetupFields {
  pub max_data_size: usize,
  pub base_size: usize,
  pub branch_factor: usize,
  pub bbox_cache_size: usize,
  pub data_list_cache_size: usize
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
/// # use failure::Error;
///
/// type P = ((f32,f32),(f32,f32));
/// type V = u32;
///
/// # fn main () -> Result<(),Error> {
/// let mut db: DB<_,_,P,V> = Setup::new(storage)
///   .branch_factor(5)
///   .max_data_size(3_000)
///   .base_size(1_000)
///   .build()?;
/// # Ok(()) }
///
/// fn storage(name: &str) -> Result<RandomAccessDisk,Error> {
///   let mut p = PathBuf::from("/tmp/eyros-db/");
///   p.push(name);
///   Ok(RandomAccessDisk::builder(p).auto_sync(false).build()?)
/// }
/// ```
pub struct Setup<S,U> where
S: RandomAccess<Error=Error>+Send+Sync+Unpin,
U: (Fn(&str) -> Box<dyn Future<Output=Result<S,S::Error>>+Unpin>) {
  pub open_store: U,
  pub fields: SetupFields
}

impl<S,U> Setup<S,U> where
S: RandomAccess<Error=Error>+Send+Sync+'static+Unpin,
U: (Fn(&str) -> Box<dyn Future<Output=Result<S,S::Error>>+Unpin>) {
  /// Create a new `Setup` builder from a storage function.
  pub fn new (open_store: U) -> Self {
    Self {
      open_store,
      fields: SetupFields {
        branch_factor: 5,
        max_data_size: 3_000,
        base_size: 9_000,
        bbox_cache_size: 10_000,
        data_list_cache_size: 16_000
      }
    }
  }
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
  pub async fn build<P,V> (self) -> Result<DB<S,U,P,V>,Error>
  where P: Point+'static, V: Value+'static {
    DB::open_from_setup(self).await
  }
}
