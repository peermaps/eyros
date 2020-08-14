var fs = require('fs')
var file = process.argv[2]
var lines = fs.readFileSync(file, 'utf8').split('\n')
console.log(lines.map((line,i) => {
  if (/^\s*this\.ptr = 0;/.test(line) && /^\s*(batch|query)\(/.test(lines[i-2])) {
    return line.replace(/(\S)/, '// $1')
  }
  return line
    .replace(/^const \{ TextDecoder \} = .*/, '')
    .replace(/^const path = .*/, "const path = require('path');")
    .replace(
      /^const bytes = .*/,
      `const bytes = require('fs').readFileSync(`
        + `path.join(__dirname,'eyros_bg.wasm'));`
    )
}).join('\n'))
