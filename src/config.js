exports.readConfig = function() {
  
  let dotenv = require("dotenv");
  dotenv.config();

  var config = {
    "db": {
      "host" : process.env.db_host,
      "user" : process.env.db_user,
      "password" : process.env.db_password,
      "timezone" : "utc",
      "database" : process.env.db_name
    },
    "logger": {
      "level": process.env.logger_level || 'info'
    }
  };
  
  global.config = config;
};