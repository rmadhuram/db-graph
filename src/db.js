var mysql = require('mysql')
var pool = mysql.createPool(global.config.db)

const logger = require('./logger').logger

function fetchData(sql, callback) {
  logger.trace(sql, params)
  pool.getConnection(function(err, connection) {
    if(err) {
      console.log('DB error when getConnection: --> ', err)
      logger.error('DB error when getConnection: ' + err);
      callback(true); return;
    }

    connection.query(sql, [], function(err, results) {
      connection.release(); // always put connection back in pool after last query
      logger.trace(results.message)
      if (err) {
        console.log(sql)
        logger.error('DB error when query: ' + err);
        callback(err); return;
      }

      callback(false, results);
    });
  });
}

async function executeQuery(sql, params) {
  logger.trace(sql, params)
  return new Promise((resolve, reject) => {
    pool.getConnection((err, connection) => {
      if (err) {
        reject(err)
      } else {
        connection.query(sql, params, function (error, results) {
          connection.release(); // always put connection back in pool after last query
          logger.trace(results)
          if (error) {
            logger.error(error)
            reject(error)
          } else {
            resolve(results)
          }
        });
      }
    })
  })  
}

exports.executeQuery = executeQuery
exports.runQuery = fetchData
exports.format = mysql.format

exports.end = function() {
  return new Promise((resolve) => {
    pool.end(err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

