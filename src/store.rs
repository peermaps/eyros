#[cfg(not(feature="wasm"))]
use crate::{Scalar,Value,Error,SetupFields,d2};
#[cfg(not(feature="wasm"))]
use std::path::{Path,PathBuf};
#[cfg(not(feature="wasm"))]
type S = RandomAccessDisk;
#[cfg(not(feature="wasm"))]
use random_access_disk::RandomAccessDisk;

#[cfg(feature="wasm")]
use crate::Error;

/// Return random access storage adaptors for files by a string name
#[async_trait::async_trait]
pub trait Storage<S> {
  async fn open (&mut self, name: &str) -> Result<S,Error>;
}

#[cfg(not(feature="wasm"))]
pub struct FileStore {
  path: PathBuf
}

#[cfg(not(feature="wasm"))]
impl FileStore {
  pub fn open (path: &Path) -> Self {
    Self { path: path.to_path_buf() }
  }
}

#[cfg(not(feature="wasm"))]
#[async_trait::async_trait]
impl Storage<S> for FileStore {
  async fn open (&mut self, name: &str) -> Result<S,Error> {
    let p = self.path.join(name);
    S::builder(p)
      .auto_sync(false)
      .build()
      .await
      .map_err(|e| e.into())
  }
}

#[cfg(not(feature="wasm"))]
impl<X,Y,V> d2::DB<S,X,Y,V> where X: Scalar+'static, Y: Scalar+'static, V: Value+'static {
  pub async fn open_from_path(path: &Path) -> Result<Self,Error> {
    Ok(d2::Setup::from_path(path).build().await?)
  }
}

#[cfg(not(feature="wasm"))]
impl d2::Setup<S> {
  /// Create a new `Setup` builder from a string file path.
  pub fn from_path (path: &Path) -> Self {
    Self {
      storage: Box::new(FileStore::open(Path::new(path))),
      fields: SetupFields::default()
    }
  }
}
