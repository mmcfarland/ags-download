var fs = require('fs');

var f = fs.readFileSync('./j.json')

var c = JSON.parse(f);

console.log(c.features.length);
