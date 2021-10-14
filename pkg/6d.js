var api = require('./lib/6d-api.js')
var wrapStorage = require('./lib/wrap-storage.js')
var setWasm = require('./lib/set-wasm.js')

module.exports = function (opts) {
  if (!opts.storage) throw new Error('opts.storage not provided')
  return setWasm(api, opts).then(function (r) {
    if (typeof opts.getId === 'function') {
      api.set_getid(opts.getId)
    }
    return api.open_f32_f32_f32_f32_f32_f32(Object.assign({}, opts, {
      storage: wrapStorage(opts.storage),
      remove: opts.remove || function () {}
    }))
  })
}
