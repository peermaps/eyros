use crate::{DB,Point,Value,Setup,SetupFields,Error};
use random_access_disk::RandomAccessDisk;
use std::path::{Path,PathBuf};
type S = RandomAccessDisk;

#[async_trait::async_trait]
pub trait Storage<S> {
  async fn open (&mut self, name: &str) -> Result<S,Error>;
}

pub struct FileStore {
  path: PathBuf
}

impl FileStore {
  pub fn open (path: &Path) -> Self {
    Self { path: path.to_path_buf() }
  }
}

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

impl<P,V> DB<S,P,V> where P: Point+'static, V: Value+'static {
  pub async fn open_from_path(path: &Path) -> Result<Self,Error> {
    Ok(Setup::from_path(path).build().await?)
  }
}

impl Setup<S> {
  /// Create a new `Setup` builder from a string file path.
  pub fn from_path (path: &Path) -> Self {
    Self {
      storage: Box::new(FileStore::open(Path::new(path))),
      fields: SetupFields::default()
    }
  }
}
