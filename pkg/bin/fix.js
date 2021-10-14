var fs = require('fs')
var infile = process.argv[2]
var outfile = process.argv[3]
var lines = fs.readFileSync(infile, 'utf8').split('\n')
console.log(lines.map((line,i) => {
  return line
    .replace(/^const \{ TextDecoder \} = .*/, '')
    .replace(/^const \{ TextDecoder, TextEncoder \} = .*/, '')
    .replace(/^const path = .*/, '')
    .replace(/^const bytes = .*/, '')
    .replace(/^const wasmModule = .*/, '')
    .replace(/^const wasmInstance = .*/, '')
    .replace(/^wasm = .*/, `
      module.exports.__setWasmInstance = function (wasmInstance) {
        wasm = wasmInstance.exports;
        module.exports.__wasm = wasm;
      };
      module.exports.__wasmImports = imports;
    `.trim().replace(/^      /gm, ''))
    .replace(/^module\.exports\.__wasm = .*/, '')
}).filter(line => line.length > 0).join('\n'))
