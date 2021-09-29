use eyros::{DB,Coord};
use std::time;

type P = (Coord<f32>,Coord<f32>);
type V = u32;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let args: Vec<String> = std::env::args().collect();
  let db_dir = std::path::PathBuf::from(args[1].clone());
  let mut db: DB<_,_,P,V> = eyros::open_from_path2(&db_dir).await?;
  let start = time::Instant::now();
  db.optimize(4).await?;
  let elapsed0 = start.elapsed().as_secs_f64();
  db.sync().await?;
  let elapsed1 = start.elapsed().as_secs_f64();
  eprintln!["OPTIMIZE in {}s", elapsed0];
  eprintln!["SYNC in {}s", elapsed1];
  Ok(())
}
