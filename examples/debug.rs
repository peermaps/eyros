use eyros::{Setup,DB,Tree3,Row,Coord};
use rand::random;
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>,Coord<u16>);
type V = u64;
type T = Tree3<f32,f32,u16,V>;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,T,P,V> = Setup::from_path(&std::path::PathBuf::from("/tmp/eyros.db"))
    .debug(|msg: &str| eprintln!["[debug] {}", msg])
    .build()
    .await?;
  let batch: Vec<Row<P,V>> = (0..5_000).map(|i| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(16.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(16.0)*(90.0-ymin);
    let z = random::<u16>();
    let point = (
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
      Coord::Scalar(z)
    );
    Row::Insert(point, i)
  }).collect();
  db.batch(&batch).await?;
  db.sync().await?;

  let bbox = ((-120.0,20.0,10_000),(-100.0,35.0,20_000));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  Ok(())
}
