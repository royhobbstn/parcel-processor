// @ts-check

const express = require('express');
const { appRouter } = require('./router/worker-routes.js');
const { commonRouter } = require('./router/common-routes');
const bodyParser = require('body-parser');
const config = require('config');
const app = express();
const port = config.get('Worker.port');

app.use(bodyParser.json());
app.use(express.static('public'));

app.use(function (req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

commonRouter(app);
appRouter(app);

app.listen(port, () => console.log(`Worker Listening at http://localhost:${port}`));
