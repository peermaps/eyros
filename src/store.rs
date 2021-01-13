#[cfg(not(feature="wasm"))]
use crate::{DB,Tree,Point,Value,Error,Setup,SetupFields,Scalar,Coord};
#[cfg(not(feature="wasm"))]
use std::path::{Path,PathBuf};
#[cfg(not(feature="wasm"))]
type S = RandomAccessDisk;
#[cfg(not(feature="wasm"))]
use random_access_disk::RandomAccessDisk;
use async_std::sync::{Arc,Mutex};

#[cfg(feature="wasm")]
use crate::Error;

/// Return random access storage adaptors for files by a string name
#[async_trait::async_trait]
pub trait Storage<S> {
  async fn open (&mut self, name: &str) -> Result<S,Error>;
  async fn remove (&mut self, name: &str) -> Result<(),Error>;
}

#[cfg(not(feature="wasm"))]
pub struct FileStore {
  path: PathBuf
}

#[cfg(not(feature="wasm"))]
impl FileStore {
  pub fn new (path: &Path) -> Self {
    Self { path: path.to_path_buf() }
  }
}

#[cfg(not(feature="wasm"))]
#[async_trait::async_trait]
impl Storage<S> for FileStore {
  async fn open(&mut self, name: &str) -> Result<S,Error> {
    let p = self.path.join(name);
    S::builder(p)
      .auto_sync(false)
      .build()
      .await
      .map_err(|e| e.into())
  }
  async fn remove(&mut self, name: &str) -> Result<(),Error> {
    unimplemented![];
  }
}

#[cfg(not(feature="wasm"))]
impl<T,P,V> DB<S,T,P,V> where P: Point+'static, V: Value+'static, T: Tree<P,V> {
  pub async fn open_from_path(path: &Path) -> Result<Self,Error> {
    Ok(Setup::from_path(path).build().await?)
  }
}

#[cfg(not(feature="wasm"))]
impl Setup<S> {
  /// Create a new `Setup` builder from a string file path.
  pub fn from_path(path: &Path) -> Self {
    Self {
      storage: Arc::new(Mutex::new(Box::new(FileStore::new(Path::new(path))))),
      fields: SetupFields::default()
    }
  }
}

macro_rules! impl_open {
  ($Tree:ident,$open_from_path:ident,($($T:tt),+)) => {
    #[cfg(not(feature="wasm"))]
    use crate::$Tree;
    #[cfg(not(feature="wasm"))]
    pub async fn $open_from_path<$($T),+,V>(path: &Path)
    -> Result<DB<S,$Tree<$($T),+,V>,($(Coord<$T>),+),V>,Error>
    where $($T: Scalar),+, V: Value {
      <DB<S,$Tree<$($T),+,V>,($(Coord<$T>),+),V>>::open_from_path(path).await
    }
  }
}

#[cfg(feature="2d")] impl_open![Tree2,open_from_path2,(P0,P1)];
#[cfg(feature="3d")] impl_open![Tree3,open_from_path3,(P0,P1,P2)];
#[cfg(feature="4d")] impl_open![Tree4,open_from_path4,(P0,P1,P2,P3)];
#[cfg(feature="5d")] impl_open![Tree5,open_from_path5,(P0,P1,P2,P3,P4)];
#[cfg(feature="6d")] impl_open![Tree6,open_from_path6,(P0,P1,P2,P3,P4,P5)];
#[cfg(feature="7d")] impl_open![Tree7,open_from_path7,(P0,P1,P2,P3,P4,P5,P6)];
#[cfg(feature="8d")] impl_open![Tree8,open_from_path8,(P0,P1,P2,P3,P4,P5,P6,P7)];
