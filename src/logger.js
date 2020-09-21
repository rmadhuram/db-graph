const logger = require('pino')(global.config.logger || {})
if (global.config.logger) {
  logger.info('Log level set to ' + global.config.logger.level)
}
exports.logger = logger