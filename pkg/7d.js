var api = require('./lib/7d-api.js')
var wrapStorage = require('./lib/wrap-storage.js')
var setWasmModule = require('./lib/set-wasm-module.js')

module.exports = function (opts) {
  if (!opts.storage) throw new Error('opts.storage not provided')
  setWasmModule(api, opts)
  if (typeof opts.getId === 'function') {
    api.set_getid(opts.getId)
  }
  return api.open_f32_f32_f32_f32_f32_f32_f32(Object.assign({}, opts, {
    storage: wrapStorage(opts.storage),
    remove: opts.remove || function () {}
  }))
}
