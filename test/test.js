const func = require('../index');

(async () => {
  await func.http({method: 'GET'}, {end: (statusCode, msg) => {
    console.log(statusCode, msg)
  }}, (err) => {
    if (err) return console.error(err)
  })
})()
