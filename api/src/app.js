const fs = require('fs');

const express = require('express');
const cookieParser = require('cookie-parser');
const requestLogger = require('morgan');
const compression = require('compression');
const swaggerUi = require('swagger-ui-express');
const config = require('config');

const indexRouter = require('./routes/index');
const {
  notFound,
  errorHandler,
  prismaNotFoundHandler,
  assertionErrorHandler,
  axiosErrorHandler,
  prismaConstraintFailedHandler,
} = require('./middleware/error');
const { authenticate } = require('./middleware/auth');

// Register application
const app = express();

// TEMPORARY: Debug logging for datahub endpoint
// remove fingerprinting header
app.disable('x-powered-by');

// Mount TUS server BEFORE ALL middleware (including morgan)
// TUS needs completely raw request/response objects
const uploadService = require('./services/upload');

const tusServer = uploadService.getServer();
const logger = require('./services/logger');

logger.info('Mounting TUS server directly in app.js BEFORE all middleware');

// Mount TUS at root level so it can see full paths
// Handle both /uploads/files and /api/uploads/files requests
// (TUS returns Location headers with /api prefix for external clients)
app.use((req, res, next) => {
  // Check for both paths - TUS client may use either depending on context
  const isTusPath = req.path.startsWith('/uploads/files') || req.path.startsWith('/api/uploads/files');

  if (isTusPath) {
    const uploadId = req.path.split('/').pop();
    logger.info(`[TUS] ${req.method} ${req.path}`, {
      uploadId: uploadId !== 'files' ? uploadId : 'NEW',
      contentLength: req.headers['content-length'],
      contentType: req.headers['content-type'],
      uploadOffset: req.headers['upload-offset'],
      uploadLength: req.headers['upload-length'],
      tusResumable: req.headers['tus-resumable'],
    });
    
    // Authenticate first
    authenticate(req, res, (err) => {
      if (err) {
        logger.error(`[TUS] Authentication failed for ${req.method} ${req.path}:`, {
          error: err.message,
          uploadId: uploadId !== 'files' ? uploadId : 'NEW',
        });
        return next(err);
      }
      
      logger.info(`[TUS] Authentication successful for user: ${req.user?.username || 'unknown'}`);
      
      // TEST ONLY: Mark upload for failure simulation at FileStore level
      // Usage: Add header 'X-Simulate-Failure: mid-upload' to trigger failure after writing ~1MB
      // Configurable: fail N times before allowing success (tests TUS retry exhaustion)
      // CRITICAL: Only count PATCH requests with Content-Length (actual data uploads, not HEAD/OPTIONS)
      const hasUploadData = req.headers['content-length'] && parseInt(req.headers['content-length'], 10) > 0;
      const shouldSimulateFailure = req.headers['x-simulate-failure'] === 'mid-upload' && req.method === 'PATCH' && hasUploadData;
      if (shouldSimulateFailure && uploadId !== 'files') {
        // Initialize tracking maps
        if (!global.tusFailureSimulation) {
          global.tusFailureSimulation = new Map();
        }
        if (!global.tusFailureSimulationCount) {
          global.tusFailureSimulationCount = new Map();
        }
        
        // Configuration: How many times should this upload fail?
        // 1 = fail once, then succeed (tests resume)
        // 5 = fail 5 times, then succeed (tests retry exhaustion, user sees "Retry" button)
        // 999 = fail forever (tests complete failure scenario)
        const MAX_FAILURES = parseInt(req.headers['x-simulate-failure-count'] || '1', 10);
        
        const currentFailCount = global.tusFailureSimulationCount.get(uploadId) || 0;
        
        if (currentFailCount < MAX_FAILURES) {
          logger.warn(`[TUS] Marking upload ${uploadId} for mid-upload failure simulation (attempt ${currentFailCount + 1}/${MAX_FAILURES})`, {
            uploadId,
            method: req.method,
            contentLength: req.headers['content-length'],
            failuresRemaining: MAX_FAILURES - currentFailCount,
          });
          
          global.tusFailureSimulation.set(uploadId, true);
          global.tusFailureSimulationCount.set(uploadId, currentFailCount + 1);
        } else {
          logger.info(`[TUS] Upload ${uploadId} has exhausted failure quota (${currentFailCount} failures), allowing retry to proceed`, {
            uploadId,
            maxFailures: MAX_FAILURES,
          });
        }
        
        // Continue to TUS server - the FileStore will trigger the failure after writing data
      }
      
      // Normalize URL to have /api prefix for TUS server
      // TUS server is configured with path: '/api/uploads/files'
      if (!req.url.startsWith('/api/uploads/files')) {
        req.url = `/api${req.url}`;
      }
      
      // Intercept response to log completion/errors
      const originalEnd = res.end;
      const originalWriteHead = res.writeHead;
      let statusCode = 200;
      
      res.writeHead = function(...args) {
        statusCode = args[0];
        return originalWriteHead.apply(this, args);
      };
      
      res.end = function(...args) {
        const isSuccess = statusCode >= 200 && statusCode < 300;
        const logLevel = isSuccess ? 'info' : 'error';
        
        logger[logLevel](`[TUS] ${req.method} ${req.path} completed`, {
          statusCode,
          uploadId: uploadId !== 'files' ? uploadId : 'NEW',
          user: req.user?.username,
          success: isSuccess,
        });
        
        return originalEnd.apply(this, args);
      };
      
      // Then hand off to TUS - don't catch errors, let Express handle them
      return tusServer.handle(req, res);
    });
  } else {
    next();
  }
});

logger.info('TUS server mounted at /uploads/files');

// request logger - https://github.com/expressjs/morgan
if (config.get('mode') === 'production') {
  app.use(requestLogger('combined', { skip: (req, res) => res.statusCode < 400 }));
} else {
  app.use(requestLogger('dev'));
}

// request parsing middleware
app.use(express.json({ limit: '50mb' }));

// extended: false -> use querystring instead of qs library to parse urlencoded
// query string removes ? ex: ?a=b will be {a: b} does not parse nested objects:
// ?person[name]=bobby&person[age]=3 will be { 'person[age]': '3',
// 'person[name]': 'bobby' } see https://stackoverflow.com/questions/29960764/what-does-extended-mean-in-express-4-0
app.use(express.urlencoded({ limit: '50mb', extended: false }));
app.use(cookieParser());

// compress all responses EXCEPT binary genomic files and TUS upload endpoints
// Binary files (BigWig, BAM, etc.) must not be compressed for genome browsers to parse them
// TUS endpoints must not be compressed to avoid interfering with protocol
app.use(compression({
  filter: (req, res) => {
    // Don't compress TUS upload endpoints
    if (req.path && req.path.startsWith('/uploads/files')) {
      return false;
    }
    // Don't compress file exposure endpoints (genome browser files)
    if (req.path && req.path.includes('/files/expose')) {
      return false;
    }
    // Use default compression filter for everything else
    return compression.filter(req, res);
  },
}));

if (!['production', 'test'].includes(config.get('mode'))) {
  // mount swagger ui
  try {
    const swaggerFile = JSON.parse(fs.readFileSync('./swagger_output.json'));
    app.use('/doc', swaggerUi.serve, swaggerUi.setup(swaggerFile));
  } catch (e) {
    console.error('Unable to load "./swagger_output.json"', e);
  }
}

// mount router
app.use('/', indexRouter);

// handle unknown routes
app.use(notFound);

// handle prisma errors
app.use(prismaNotFoundHandler);
app.use(prismaConstraintFailedHandler);

// handle assertions errors and send 400
app.use(assertionErrorHandler);

// handle axios errors
app.use(axiosErrorHandler);

// pass any unhandled errors to the error handler
app.use(errorHandler);

module.exports = app;
