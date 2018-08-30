var parser = require('cron-parser');
 
var options = {
  currentDate: Date.now(),
  tz: 'Asia/Hong_Kong'
};
 
try {
  var interval = parser.parseExpression('0 * * * *', options);
 
  console.log('Date: ', interval.next().toString()); // Date:  Sun Mar 27 2016 01:00:00 GMT+0200
  console.log('Date: ', interval.next().toString()); // Date:  Sun Mar 27 2016 02:00:00 GMT+0200
  console.log('Date: ', interval.next().toString()); // Date:  Sun Mar 27 2016 04:00:00 GMT+0300 (Notice DST transition)
} catch (err) {
  console.log('Error: ' + err.message);
}
