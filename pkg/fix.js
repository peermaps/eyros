var fs = require('fs')
var file = process.argv[2]
var lines = fs.readFileSync(file, 'utf8').split('\n')
fs.writeFileSync(file, lines.map(line => {
  return line
    .replace(/^const \{ TextDecoder \} = .*/, '')
    .replace(/^const path = .*/, "const path = require('path');")
    .replace(
      /^const bytes = .*/,
      `const bytes = require('fs').readFileSync(`
        + `path.join(__dirname,'eyros_bg.wasm'));`
    )
}).join('\n'), 'utf8')
