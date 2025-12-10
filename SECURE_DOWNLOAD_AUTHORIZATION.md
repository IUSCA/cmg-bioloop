# Secure File Download Authorization in CMG Bioloop

## Overview

The CMG Bioloop application implements a multi-layered security system for file downloads using a dedicated `secure_download` microservice. This document explains how files are securely exposed for downloading and what guarantees that only authorized actors can access them.

## Architecture Components

### 1. Main Bioloop API (`api/`)
- Handles user authentication and authorization
- Issues scoped OAuth2 tokens for file access
- Validates user permissions before generating download URLs

### 2. Secure Download Microservice (`secure_download/`)
- Dedicated service for serving files
- Validates OAuth2 tokens with specific scopes
- Serves files via nginx X-Accel-Redirect or direct streaming

## Authorization Flow

### Step 1: User Authentication & Dataset Access Check

When a user requests to download a dataset, the flow begins in `DatasetDownloadModal.vue`:

```javascript
// User clicks download button, triggers:
datasetService.get_file_download_data({
  dataset_id: props.dataset.id,
})
```

This calls the API endpoint `GET /datasets/download/:id` which includes the critical `dataset_access_check` middleware:

```javascript
router.get(
  '/download/:id',
  validate([...]),
  datasetService.dataset_access_check,  // ← AUTHORIZATION CHECKPOINT
  asyncHandler(async (req, res, next) => {
    // Token generation logic
  })
);
```

### Step 2: Dataset Access Authorization

The `dataset_access_check` middleware performs two-tier authorization:

```javascript
const dataset_access_check = asyncHandler(async (req, res, next) => {
  // Check 1: Role-based permissions
  const permission = getPermission({
    resource: 'datasets',
    action: 'read',
    requester_roles: req?.user?.roles,
  });

  if (!permission.granted) {
    // Check 2: Project-based association
    const user_dataset_assoc = await has_dataset_assoc({
      user_id: req.user.id,
      dataset_id: req.params.id,
    });
    if (!user_dataset_assoc) {
      return next(createError.Forbidden()); // ← ACCESS DENIED
    }
  }
  next(); // ← ACCESS GRANTED
});
```

**Authorization Rules:**
1. **Admin/Operator roles**: Can access any dataset (`'read:any': ['*']`)
2. **User role**: Can only access datasets through project membership
3. **Project Association**: User must be member of a project that contains the dataset

### Step 3: Project-Dataset Association Check

The `has_dataset_assoc` function ensures users can only access datasets from their projects:

```javascript
async function has_dataset_assoc({ dataset_id, user_id }) {
  const projects = await prisma.project.findMany({
    where: {
      users: {
        some: { user: { id: user_id } }  // User is project member
      },
      datasets: {
        some: { dataset: { id: dataset_id } }  // Project contains dataset
      },
    },
  });
  return projects.length > 0;
}
```

### Step 4: Scoped OAuth2 Token Generation

Once authorized, the API generates a scoped OAuth2 token:

```javascript
const get_download_url = async ({ dataset, file = null } = {}) => {
  if (dataset.metadata.stage_alias) {
    const download_file_path = file
      ? `${dataset.metadata.stage_alias}/${file.path}`
      : `${get_bundle_name(dataset)}`;
    
    // Generate OAuth2 token with file-specific scope
    const download_token = await authService.get_download_token(url.pathname);
    
    return {
      url: downloadUrl.href,
      bearer_token: download_token.accessToken,  // ← SCOPED TOKEN
    };
  }
};
```

**Key Security Feature**: The OAuth2 token includes a **file-specific scope**:
```javascript
function get_download_token(file_path) {
  return oAuth2SecureTransferClient.clientCredentials({
    scope: [`${config.get('oauth.download.scope_prefix')}${file_path}`],
    //      ↑ "download_file:/path/to/specific/file"
  });
}
```

### Step 5: Secure Download Service Validation

The `secure_download` microservice validates the token and scope:

```javascript
router.get('/:bundle_name*', 
  validate([...]),
  asyncHandler(async (req, res, next) => {
    const SCOPE_PREFIX = config.get('scope_prefix'); // "download_file:"
    
    // Extract scopes from JWT token
    const scopes = (req.token?.scope || '').split(' ');
    const download_scopes = scopes.filter(scope => scope.startsWith(SCOPE_PREFIX));
    
    // Verify token grants access to requested file path
    const token_file_path = remove_leading_slash(
      download_scopes[0].slice(SCOPE_PREFIX.length)
    );
    const req_path = remove_leading_slash(req.path);
    
    // Serve file if paths match
    const fullFilePath = path.join('/opt/sca/data/downloads/', token_file_path);
    // ... file serving logic
  })
);
```

### Step 6: Token Verification

The secure download service verifies JWT tokens using shared public keys:

```javascript
// secure_download/src/middleware/auth.js
function authenticate(req, res, next) {
  let token = req.query?.token || extractBearerToken(req.headers.authorization);
  
  authService.checkJWT(token).then((decoded_token) => {
    if (!decoded_token) return next(invalid_token_err);
    req.token = decoded_token;  // ← Token validated and decoded
    next();
  });
}
```

## Security Guarantees

### 1. **Multi-Layer Authorization**
- **Layer 1**: User authentication (JWT verification)
- **Layer 2**: Dataset access permissions (role + project membership)
- **Layer 3**: File-specific OAuth2 scopes
- **Layer 4**: Path validation in secure download service

### 2. **Principle of Least Privilege**
- Tokens are scoped to specific file paths
- Users can only access datasets from their projects
- No wildcard access to file system

### 3. **Token Scope Validation**
```
Token Scope: "download_file:/staged_data/dataset123/file.bam"
Request Path: "/staged_data/dataset123/file.bam"
Result: ✅ ALLOWED

Token Scope: "download_file:/staged_data/dataset123/file.bam"  
Request Path: "/staged_data/dataset456/file.bam"
Result: ❌ FORBIDDEN
```

### 4. **Project-Based Isolation**
- Users can only access datasets assigned to their projects
- No cross-project data access
- Admin/operator roles have elevated permissions

### 5. **Temporal Security**
- OAuth2 tokens have expiration times
- Tokens are generated per-request
- No persistent file access credentials

## Configuration

### Main API OAuth Configuration (`api/config/default.json`):
```json
{
  "oauth": {
    "download": {
      "client_id": "...",
      "client_secret": "...",
      "scope_prefix": "download_file:"
    }
  }
}
```

### Secure Download Configuration (`secure_download/config/default.json`):
```json
{
  "scope_prefix": "download_file:",
  "auth": {
    "jwks_uri": "http://127.0.0.1:5050/oauth/jwks"
  }
}
```

## File Serving Methods

### Production (nginx X-Accel-Redirect):
```javascript
res.set('X-Accel-Redirect', `/data/${token_file_path}`);
res.set('content-type', 'application/octet-stream; charset=utf-8');
res.set('X-Accel-Buffering', 'no'); // Prevents 1GB buffer limit
```

### Development (Direct Streaming):
```javascript
const fileStream = fs.createReadStream(fullFilePath);
fileStream.pipe(res);
```

## Security Considerations

### ✅ **Implemented Protections**
1. **Path Traversal Prevention**: File paths are validated and resolved
2. **Scope Enforcement**: Tokens are limited to specific file paths
3. **Project Isolation**: Users can only access their project's datasets
4. **Role-Based Access**: Admin/operator roles have elevated permissions
5. **Token Expiration**: OAuth2 tokens have limited lifetimes

### ⚠️ **Potential Improvements**
1. **Rate Limiting**: Could add download rate limiting per user
2. **Audit Logging**: Enhanced logging of download attempts
3. **IP Restrictions**: Could restrict downloads to specific networks
4. **File Integrity**: Could add checksum verification

## Summary

The secure download system ensures that only authorized users can download files by:

1. **Authenticating users** via JWT tokens
2. **Authorizing dataset access** through role-based permissions and project membership
3. **Generating scoped OAuth2 tokens** that grant access to specific file paths only
4. **Validating token scopes** in the secure download service before serving files
5. **Isolating file access** to prevent unauthorized cross-dataset access

This multi-layered approach provides strong security guarantees while maintaining usability for legitimate users.
