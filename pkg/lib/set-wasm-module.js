module.exports = function (w, opts) {
  if (opts.wasmModule) {
    w.__setWasmModule(opts.wasmModule)
  } else if (opts.wasmSource) {
    w.__setWasmModule(new WebAssembly.Module(opts.wasmSource))
  } else {
    throw new Error('must provide one of opts.wasmModule or opts.wasmSource')
  }
}
