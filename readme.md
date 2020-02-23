# eyros

eyros (εύρος) is a multi-dimensional interval database.

The database is based on [bkd][] and [interval][] trees.

* high batch-write performance (expect 100,000s to 1,000,000s writes per second
  on modest hardware)
* designed for peer-to-peer distribution and query-driven sparse replication
* compiles to web assembly for use in the browser

eyros operates on scalar (x) or interval (min,max) coordinates for each
dimension. There are 2 operations: batched write (for inserting and deleting)
and query by bounding box. All features that intersect the bounding box are
returned in the query results.

[bkd]: https://users.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf
[interval]: http://www.dgp.toronto.edu/~jstewart/378notes/22intervals/

# example

This example generates 800 random features in 3 dimensions: `x`, `y`, and `time`
with a `u32` `value` payload. The `x` and `y` dimensions are both intervals with
a minimum and maximum `f32` and `time` is a scalar `f32`.

After the data is written to the database, all features with an `x` interval
that overlaps `(-0.5,0.3)`, a `y` interval that overlaps `(-0.8,-0.5)`, and a
`time` scalar that is between `0.0` and `100.0` are printed to stdout.

``` rust
extern crate eyros;
extern crate failure;
extern crate rand;
extern crate random_access_disk;

use eyros::{DB,Row};
use rand::random;
use failure::Error;
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;

type P = ((f32,f32),(f32,f32),f32);
type V = u32;

fn main() -> Result<(),Error> {
  let mut db: DB<_,_,((f32,f32),(f32,f32),f32),u32> = DB::open(storage)?;
  let polygons: Vec<Row<P,V>> = (0..800).map(|_| {
    let xmin: f32 = random::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = random::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = random::<f32>()*1000.0;
    let value: u32 = random();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  db.batch(&polygons)?;

  let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
  for result in db.query(&bbox)? {
    println!("{:?}", result?);
  }
  Ok(())
}

fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
  let mut p = PathBuf::from("/tmp/eyros-db/");
  p.push(name);
  Ok(RandomAccessDisk::builder(p)
    .auto_sync(false)
    .build()?)
}
```

The output from this program is of the form `(coords, value, location)`:

```
$ cargo run --example polygons -q
(((-0.014986515, -0.014986515), (-0.5801666, -0.5801663), 45.314373), 1518966744, (0, 200))
(((-0.0892005, -0.015534878), (-0.65783, -0.65783), 3.6987066), 66257667, (0, 267))
(((0.1931547, 0.1931547), (-0.6388786, -0.60205233), 67.85113), 2744609531, (0, 496))
(((-0.28907382, -0.26248854), (-0.7761978, -0.77617484), 55.273056), 3622408505, (0, 651))
(((-0.080417514, -0.080417514), (-0.60076225, -0.5929384), 29.592216), 722871034, (0, 784))
(((0.14104307, 0.14104307), (-0.539363, -0.539363), 31.965792), 2866780128, (0, 933))
(((-0.12689173, -0.12689173), (-0.56708515, -0.56643564), 65.072), 1858542500, (0, 983))
(((-0.12520671, -0.1250745), (-0.6836084, -0.6836084), 93.58209), 3942792215, (0, 1019))
(((0.026417613, 0.026417613), (-0.786397, -0.786397), 61.52451), 1197187917, (0, 1102))
(((-0.18799019, -0.18799017), (-0.50418067, -0.50418067), 82.93134), 2811117540, (0, 1199))
(((-0.34033966, -0.34033966), (-0.53603613, -0.53603613), 91.07471), 302136936, (0, 1430))
(((-0.008744121, 0.54438573), (-0.73665094, -0.73665094), 69.67532), 719725479, (0, 1504))
(((-0.38071227, -0.38071224), (-0.75237143, -0.75237143), 72.245895), 2200140390, (0, 1628))
(((0.020396352, 0.020396352), (-0.7957357, -0.77274036), 40.785194), 2166765724, (0, 1708))
(((0.117452025, 0.117452025), (-0.7027955, -0.7026706), 82.033394), 2451987859, (0, 1886))
(((-0.11418259, -0.11418259), (-0.74327374, -0.74327374), 28.591274), 4283568770, (0, 1983))
(((-0.19130886, -0.19130856), (-0.7012402, -0.7012042), 2.1106005), 4226013993, (0, 2048))
(((-0.3000791, -0.3000791), (-0.7601782, -0.7601782), 24.528027), 2776778380, (0, 2349))
```

The `coords` and `value` are the values that were written earlier: in this case,
the coords are `((xmin,xmax),(ymin,ymax),time)`.

The `location` is used to quickly delete records without needing to perform
additional lookups. You'll need to keep the `location` around from the result of
a query when you intend to delete a record. Locations that begin with a `0` are
stored in the staging cache, so their location may change after the next write.

# status

Work in progress. under active development!

This is an important foundational component for the peermaps roadmap.

