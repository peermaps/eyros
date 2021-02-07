use eyros::{DB,Coord};
use std::time;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,_,P,V> = eyros::open_from_path2(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let start = time::Instant::now();
  db.optimize(5).await?;
  let elapsed0 = start.elapsed().as_secs_f64();
  db.sync().await?;
  let elapsed1 = start.elapsed().as_secs_f64();
  eprintln!["OPTIMIZE in {}s", elapsed0];
  eprintln!["SYNC in {}s", elapsed1];
  Ok(())
}
