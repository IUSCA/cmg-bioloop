# Secure Genome Browser File Serving Implementation

## Overview

This document describes the implementation of secure file serving for genome browsers (WashU, IGV) using the `secure_download` microservice with token-based authorization.

## Changes Made

### 1. Configuration Updates

#### Main API (`api/config/default.json`)
```json
{
  "oauth": {
    "genome_browser": {
      "client_id": "",
      "client_secret": "",
      "scope_prefix": "genome_browser_file:"
    }
  },
  "secure_download": {
    "base_url": "http://localhost:3060",
    "genome_browser_path_prefix": "/genome-browser"
  }
}
```

#### Secure Download Service (`secure_download/config/default.json`)
```json
{
  "genome_browser_scope_prefix": "genome_browser_file:",
  "data_root": "/opt/sca/data"
}
```

### 2. New Secure Download Endpoint

**File**: `secure_download/src/routes/genome-browser.js`

- **Endpoint**: `GET /genome-browser/*`
- **Purpose**: Serve genomic files with token-based authorization
- **Security Features**:
  - JWT token validation with `genome_browser_file:` scope
  - Exact path matching between token scope and request
  - Path traversal protection
  - File existence validation
  - CORS headers for browser compatibility
  - HTTP Range support for large files (BAM, etc.)

**Key Security Invariants**:
- Token scope must match requested path exactly
- All files must be under `/opt/sca/data`
- No partial prefix matches allowed
- JWT validation via JWKS endpoint

### 3. Updated Main API

**File**: `api/src/routes/sessions.js`

#### New Helper Functions:
- `getRelativeFilePathForGenomeBrowser()` - Computes file paths for secure download
- `getGenomeBrowserFileToken()` - Generates file-scoped JWT tokens (placeholder implementation)
- `buildGenomeBrowserUrl()` - Constructs secure download URLs

#### Updated `/sessions/:id/datahub` Endpoint:
- Now generates secure URLs via `secure_download` service
- Uses token-based authorization instead of public file serving
- Maintains WashU DataHub format compatibility

#### Removed Insecure Endpoint:
- **Removed**: `GET /sessions/:session_id/files/:file_id`
- **Reason**: Replaced with secure token-based file serving

### 4. URL Format Changes

#### Before (Insecure):
```
https://api.example.com/api/sessions/123/files/456
```

#### After (Secure):
```
https://secure-download.example.com/genome-browser/staged_data/datasets/42/sample.bam?token=eyJhbGciOi...
```

## Token & Scope Design

### Scope Format:
```
genome_browser_file:/staged_data/datasets/42/sample.bam
```

### Token Claims:
```json
{
  "scope": "genome_browser_file:/staged_data/datasets/42/sample.bam",
  "exp": 1640995200,
  "iat": 1640908800
}
```

## File Path Resolution

### Path Construction:
1. **Dataset Stage Alias**: `staged_data/datasets/42` (from `dataset.metadata.stage_alias`)
2. **File Path**: `sample.bam` (from `dataset_file.path`)
3. **Relative Path**: `/staged_data/datasets/42/sample.bam`
4. **Full Path**: `/opt/sca/data/staged_data/datasets/42/sample.bam`

### Security Validation:
- Path must resolve under `/opt/sca/data`
- No `../` traversal allowed
- Token scope must match exact path

## Integration Flow

### 1. Session Creation
User creates a genome browser session with selected tracks.

### 2. DataHub Request
```
GET /api/sessions/123/datahub
```

### 3. Token Generation (Per File)
```javascript
const relativePath = getRelativeFilePathForGenomeBrowser({ dataset, datasetFile });
const token = await getGenomeBrowserFileToken(relativePath);
const url = buildGenomeBrowserUrl(relativePath, token);
```

### 4. WashU DataHub Response
```json
[
  {
    "type": "bam",
    "name": "Sample Alignments",
    "url": "http://localhost:3060/genome-browser/staged_data/datasets/42/sample.bam?token=eyJhbGciOi...",
    "showOnHubLoad": true
  }
]
```

### 5. Genome Browser File Access
WashU browser calls the secure URL directly:
```
GET /genome-browser/staged_data/datasets/42/sample.bam?token=eyJhbGciOi...
```

### 6. Secure File Serving
- Validate JWT token
- Check scope matches path
- Serve file with CORS headers
- Support Range requests for large files

## OAuth2 Implementation

✅ **OAuth2 integration is now complete!** The implementation uses the existing OAuth2 infrastructure:

### Token Generation (`api/src/services/auth.js`):
```javascript
const oAuth2GenomeBrowserClient = new OAuth2Client({
  server: config.get('oauth.base_url'),
  clientId: config.get('oauth.genome_browser.client_id'),
  clientSecret: config.get('oauth.genome_browser.client_secret'),
  tokenEndpoint: 'oauth/token',
});

function get_file_exposure_token(file_path) {
  return oAuth2GenomeBrowserClient.clientCredentials({
    scope: [`${config.get('oauth.genome_browser.scope_prefix')}${file_path}`],
  });
}
```

### Usage in Sessions API:
```javascript
async function getGenomeBrowserFileToken(relativePath) {
  const tokenResponse = await authService.get_file_exposure_token(relativePath);
  return tokenResponse.accessToken;
}
```

### Configuration Required:
To activate the OAuth2 integration, set these configuration values:
- `oauth.base_url`: OAuth2 server URL
- `oauth.genome_browser.client_id`: Client ID for genome browser access
- `oauth.genome_browser.client_secret`: Client secret for genome browser access

## Benefits

1. **Security**: Token-based authorization replaces public file access
2. **Auditability**: All file access is logged with token information
3. **Scalability**: Dedicated file service handles large genomic files
4. **Flexibility**: Easy to add rate limiting, caching, etc.
5. **Compliance**: Proper access control for sensitive genomic data

## Testing

### Manual Testing:
1. Create a genome browser session
2. Call `/sessions/:id/datahub` endpoint
3. Verify secure URLs are generated
4. Test file access via secure_download service

### Integration Testing:
1. Test with actual WashU browser
2. Verify CORS headers work correctly
3. Test Range requests for large BAM files
4. Validate token expiration handling

## Production Deployment

### Nginx Configuration:
The secure_download service should be configured with nginx for optimal performance:

```nginx
location /genome-browser/ {
    proxy_pass http://secure_download_service;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    
    # Enable large file streaming
    proxy_buffering off;
    proxy_request_buffering off;
}
```

### Environment Variables:
- `DATA_ROOT`: Path to genomic data files
- `JWKS_URI`: OAuth2 JWKS endpoint for token validation
- `OAUTH_CLIENT_ID`: Client ID for token generation
- `OAUTH_CLIENT_SECRET`: Client secret for token generation
