use eyros::{DB,Row,Coord};
//use eyros::Tree2;
use rand::random;
//use desert::FromBytes;
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,P,V> = DB::open_from_path(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let rows: Vec<Row<P,V>> = (0..1_000).map(|i| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(16.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(16.0)*(90.0-ymin);
    let point = (Coord::Interval(xmin,xmax), Coord::Interval(ymin,ymax));
    //let value = (random::<f32>() * 10_000.0) as u64;
    Row::Insert(point, i)
  }).collect();
  db.batch(&rows).await?;

  let bytes = db.trees[0].lock().await.to_bytes()?;
  //eprintln!["tree={:#?}", <Tree2<f32,f32,V>>::from_bytes(&bytes)?];
  eprintln!["{} bytes", bytes.len()];

  //let bbox = ((-50.0,-45.0),(-25.0,-15.0));
  let bbox = ((-180.0,-90.0),(180.0,90.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  Ok(())
}
