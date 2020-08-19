var fs = require('fs')
var file = process.argv[2]
var lines = fs.readFileSync(file, 'utf8').split('\n')
console.log(lines.map((line,i) => {
  return line
    .replace(/^const \{ TextDecoder \} = .*/, '')
    .replace(/^const path = .*/, "const path = require('path');")
    .replace(
      /^const bytes = .*/,
      `const bytes = require('fs').readFileSync(`
        + `path.join(__dirname,'eyros.wasm'));`
    )
}).join('\n'))
