/**
 * Logger configuration for CMG sync scripts
 * Using Winston for structured logging
 */

const winston = require('winston');

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss',
    }),
    winston.format.errors({ stack: true }),
    winston.format.splat(),
    winston.format.printf(({ level, message, timestamp, ...meta }) => {
      let msg = `${timestamp} ${level}: ${message}`;
      if (Object.keys(meta).length > 0 && meta.stack) {
        msg += `\n${meta.stack}`;
      }
      return msg;
    }),
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ level, message, timestamp }) => {
          return `${timestamp} ${level}: ${message}`;
        }),
      ),
    }),
  ],
});

module.exports = logger;

