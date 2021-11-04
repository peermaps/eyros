use eyros::{DB,Row,Coord};
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>);
type V = u32;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let args: Vec<String> = std::env::args().collect();
  let mut db: DB<_,_,P,V> = eyros::open_from_path2(
    &std::path::PathBuf::from(args[1].clone())
  ).await?;

  db.batch(&vec![
    Row::Insert((Coord::Scalar(-4.0), Coord::Scalar(-2.0)), 100),
    Row::Insert((Coord::Interval(3.2,3.6), Coord::Interval(-0.3,-0.2)), 101),
    Row::Insert((Coord::Scalar(5.0), Coord::Scalar(6.0)), 102),
  ]).await?;
  let mut stream = db.query(&((-10.0,-10.0),(10.0,10.0))).await?;
  while let Some(result) = stream.next().await {
    println!["{:?}", result?];
  }
  
  println!["---"];

  db.batch(&vec![
    Row::Delete((Coord::Interval(3.2,3.6), Coord::Interval(-0.3,-0.2)), 101),
    Row::Delete((Coord::Scalar(5.0), Coord::Scalar(6.0)), 102),
    Row::Insert((Coord::Scalar(-1.3), Coord::Scalar(4.5)), 103),
  ]).await?;
  let mut stream = db.query(&((-10.0,-10.0),(10.0,10.0))).await?;
  while let Some(result) = stream.next().await {
    println!["{:?}", result?];
  }

  Ok(())
}
