use crate::{Storage,Error};
use random_access_storage::RandomAccess;

pub struct RandomAccessWeb {
}

impl RandomAccessWeb {
  pub fn open() -> Self {
    Self {}
  }
}

#[async_trait::async_trait]
impl RandomAccess for RandomAccessWeb {
  type Error = Box<dyn std::error::Error+Sync+Send>;

  async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, Self::Error> {
    unimplemented![]
  }

  async fn read_to_writer(&mut self, offset: u64, length: u64,
  buf: &mut (impl futures_io::AsyncWrite + Send)) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn del(&mut self, offset: u64, length: u64) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn truncate(&mut self, length: u64) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn len(&self) -> Result<u64, Self::Error> {
    unimplemented![]
  }

  async fn is_empty(&mut self) -> Result<bool, Self::Error> {
    unimplemented![]
  }

  async fn sync_all(&mut self) -> Result<(), Self::Error> {
    unimplemented![]
  }
}

pub struct StorageWeb {
}

#[async_trait::async_trait]
impl Storage<RandomAccessWeb> for StorageWeb {
  async fn open(&mut self, name: &str) -> Result<RandomAccessWeb,Error> {
    Ok(RandomAccessWeb {})
  }
}
