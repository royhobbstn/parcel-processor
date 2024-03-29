// @ts-check

const express = require('express');
const { appRouter } = require('./router/app-routes.js');
const { commonRouter } = require('./router/common-routes');
const bodyParser = require('body-parser');

const app = express();
const port = 4000;

app.use(bodyParser.json());
app.use(express.static('public'));

app.use(function (req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

commonRouter(app);
appRouter(app);

app.listen(port, () => console.log(`App Listening at http://localhost:${port}`));
