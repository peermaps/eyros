use eyros::{DB,Coord};
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let args: Vec<String> = std::env::args().collect();
  let mut db: DB<_,_,P,V> = eyros::open_from_path2(
    &std::path::PathBuf::from(args[1].clone())
  ).await?;

  let wsen: Vec<f32> = args[2].split(",").map(|x| x.parse().unwrap()).collect();
  let bbox = ((wsen[0],wsen[1]),(wsen[2],wsen[3]));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!["{:?}", result?];
  }
  Ok(())
}
