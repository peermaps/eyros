module.exports = function (storage) {
  return function (x) {
    var r = storage(x)
    return {
      write: function (offset, buf, cb) {
        if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
        r.write(offset, buf, cb)
      },
      read: function (offset, length, cb) {
        r.read(offset, length, cb)
      },
      len: function (cb) {
        if (typeof r.length === 'number') {
          process.nextTick(cb, null, r.length)
        } else if (typeof r.len === 'function') {
          r.len(cb)
        } else if (typeof r.length === 'function') {
          r.length(cb)
        } else {
          cb(new Error('len() implementation not provided'))
        }
      },
      truncate: function (length, cb) {
        if (typeof r.truncate === 'function') {
          r.truncate(length, cb)
        } else {
          cb(new Error('truncate() implementation not provided'))
        }
      },
      del: function (cb) {
        if (typeof r.del === 'function') {
          r.del(cb)
        } else if (typeof r['delete'] === 'function') {
          r['delete'](cb)
        } else {
          cb(new Error('del() implementation not provided'))
        }
      },
      sync: function (cb) {
        if (typeof r.sync === 'function') {
          r.sync(cb)
        } else {
          process.nextTick(cb)
        }
      }
    }
  }
}
