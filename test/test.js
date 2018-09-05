const func = require('../index');

(async () => {
  await func.http({method: 'GET', query: {Key: "test"}}, {status: (statusCode) => {
    console.log(statusCode)
    return {end:(msg)=>{console.log(msg)}}
  }}, (err) => {
    if (err) return console.error(err)
  })
})()
