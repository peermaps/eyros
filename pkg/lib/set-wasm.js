module.exports = function (w, opts) {
  if (opts.wasmModule) {
    return WebAssembly.instantiate(opts.wasmModule, w.__wasmImports)
    .then(function (instance) {
      w.__setWasmInstance(instance)
      return { module: opts.wasmModule, instance }
    })
  } else if (opts.wasmSource && isResponse(opts.wasmSource)) {
    return createStreaming(opts.wasmSource)
  } else if (opts.wasmSource && isPromise(opts.wasmSource)) {
    return opts.wasmSource.then(function (src) {
      return isResponse(src) ? createStreaming(src) : create(src)
    })
  } else if (opts.wasmStream) {
    return createStreaming(opts.wasmStream)
  } else if (opts.wasmSource) {
    return create(opts.wasmSource)
  } else {
    throw new Error('must provide one of opts.wasmModule or opts.wasmSource')
  }
  function create(src) {
    return WebAssembly.instantiate(src, w.__wasmImports).then(function (r) {
      w.__setWasmInstance(r.instance)
      return r
    })
  }
  function createStreaming(src) {
    return WebAssembly.instantiateStreaming(src, w.__wasmImports).then(function (r) {
      w.__setWasmInstance(r.instance)
      return r
    })
  }
}

function isResponse(x) {
  if (!x) return false
  if (x.constructor && x.constructor.name === 'Response') return true
  if (typeof x.arrayBuffer === 'function' && typeof x.blob === 'function') {
    return true
  }
  return false
}

function isPromise(x) {
  return x && typeof x.then === 'function'
}

function isStreaming(x) {
  return isResponse(x) || isPromise(x)
}
