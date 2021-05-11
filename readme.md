# eyros

eyros (εύρος) is a multi-dimensional interval database.

The database is based on [bkd][] and [interval][] trees.

* high batch-write performance
* designed for peer-to-peer distribution and query-driven sparse replication
* [compiles to web assembly for use in the browser][eyros-npm]
* good for geospatial and time-series data

eyros operates on scalar (x) or interval (min,max) coordinates for each
dimension. There are 2 operations: batched write (for inserting and deleting)
and query by bounding box. All features that intersect the bounding box are
returned in the query results.

[bkd]: https://users.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf
[interval]: http://www.dgp.toronto.edu/~jstewart/378notes/22intervals/
[eyros-npm]: https://www.npmjs.com/package/eyros

# example

This example inserts 5000 records, writes the data to disk, then queries and prints records inside
the bounding box `((-120.0,20.0,10_000),(-100.0,35.0,20_000))`.

The bounding box is of the form `((min_x,min_y,min_z),(max_x,max_y,max_z))`.

``` rust
use eyros::{Row,Coord};
use rand::random;
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>,Coord<u16>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db = eyros::open_from_path3(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
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
```

The output from this program is of the form `(coords, value)`:

``` sh
$ cargo run --example polygons -q
((Interval(-100.94689, -100.94689), Interval(20.108843, 20.109331), Scalar(16522)), 4580)
((Interval(-111.62768, -110.40406), Interval(-7.519809, 86.154755), Scalar(12384)), 2603)
((Interval(-114.46505, -31.340988), Interval(-57.901405, 20.235504), Scalar(11360)), 1245)
((Interval(-159.97859, 121.304184), Interval(32.35743, 32.35743), Scalar(10164)), 3294)
((Interval(-150.29192, -35.475517), Interval(-39.97779, 29.163605), Scalar(15333)), 2336)
((Interval(-162.45879, -92.46166), Interval(31.187943, 31.187975), Scalar(12221)), 2826)
((Interval(-160.53441, -88.66396), Interval(10.031784, 21.852394), Scalar(11711)), 2366)
((Interval(-132.39021, -98.14838), Interval(-0.06010294, 53.88453), Scalar(10685)), 3441)
```

# license

[license zero parity 7.0.0](https://paritylicense.com/versions/7.0.0.html)
and [apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
(contributions)
