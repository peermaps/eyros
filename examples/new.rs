use eyros::{DB,Tree2,Row,Coord};
use rand::random;
use desert::FromBytes;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,P,V> = DB::open_from_path(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let rows: Vec<Row<P,V>> = (0..1_000).map(|_| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(4.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(4.0)*(90.0-xmin);
    let point = (Coord::Interval(xmin,xmax), Coord::Interval(ymin,ymax));
    let value = random::<u64>();
    Row::Insert(point, value)
  }).collect();
  db.batch(&rows).await?;

  let bytes = db.trees[0].to_bytes()?;
  //eprintln!["tree={:#?}", <Tree2<f32,f32,V>>::from_bytes(&bytes)?];
  eprintln!["{} bytes", bytes.len()];
  /*
  let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  */
  Ok(())
}
