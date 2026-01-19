/**
 * Logger configuration for CMG sync scripts
 * Using Winston for structured logging
 */

const winston = require('winston');
const path = require('path');

// Determine script name for log file
const scriptName = path.basename(process.argv[1], '.js');
const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
const logFileName = `/tmp/${scriptName}_${timestamp}.log`;

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
    new winston.transports.File({
      filename: logFileName,
      format: winston.format.combine(
        winston.format.timestamp({
          format: 'YYYY-MM-DD HH:mm:ss',
        }),
        winston.format.printf(({ level, message, timestamp }) => {
          return `${timestamp} ${level}: ${message}`;
        }),
      ),
    }),
  ],
});

// Log the file location on startup
logger.info(`Logs are being written to: ${logFileName}`);

module.exports = logger;

