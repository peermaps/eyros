window.RAW = require('random-access-web')
window.RAM = require('random-access-memory')
window.eyros = require('./eyros.js')
window.Buffer = require('buffer').Buffer

window.log = function (msg) {
  console.log(msg)
}

eyros.open(function (x) {
  console.log('storage',x)
  var r = new RAM()
  var {read,write} = r
  r.read = function (offset, length, cb) {
    console.log('READ',offset,length,cb)
    return read.call(r, offset,length,cb)
  }
  r.write = function (offset, buf, cb) {
    return write.call(r,offset,Buffer.isBuffer(buf)?buf:Buffer.from(buf),cb)
  }
  r.len = function (cb) { cb(null, r.length) }
  return r
})
  .then((db) => {
    window.db = db
    return db.batch([
      {type:'insert', point:[1,2], value: Uint8Array.from([97,98,99])}
    ])
  })
  .then(() => console.log('batch complete'))
  .catch(err => console.log(err))
