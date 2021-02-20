use eyros::{DB,Coord};
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,_,P,V> = eyros::open_from_path2(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;

  let bbox = ((-180.0,-90.0),(180.0,90.0));
  let mut stream = db.query(&bbox).await?;
  let mut count = 0;
  while let Some(_result) = stream.next().await {
    count += 1;
  }
  println!["count={}", count];
  Ok(())
}
