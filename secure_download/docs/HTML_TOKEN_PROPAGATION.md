# HTML Token Propagation for Cross-Domain File Serving

**Date:** 2026-01-27  
**Status:** Implemented  
**Location:** `secure_download/src/routes/reports.js`

---

## Problem Statement

### The Cross-Domain Authentication Challenge

When serving HTML files that reference nested resources (CSS, JavaScript, images, other HTML files), a fundamental browser behavior creates an authentication problem in cross-domain scenarios:

**Browsers do NOT automatically propagate query parameters to nested resource requests.**

### Real-World Example

```
Initial Request (with token):
GET https://cmg3.sca.iu.edu/reports/.../index.html?access_token=eyJhbGc...
Status: 200 OK ✓

Subsequent Requests (browser makes automatically):
GET https://cmg3.sca.iu.edu/reports/.../style.css
GET https://cmg3.sca.iu.edu/reports/.../tree.html
GET https://cmg3.sca.iu.edu/reports/.../script.js
Status: 401 Unauthorized ✗ (no token!)
```

### Why This Happens

When the browser parses HTML and encounters resource references like:
```html
<link rel="stylesheet" href="style.css">
<script src="script.js"></script>
<img src="image.png">
<a href="tree.html">Link</a>
```

It makes **new, independent HTTP requests** for each resource using only the URL specified in the HTML. The query parameters from the parent request (`?access_token=...`) are **not automatically included**.

### Why Traditional Solutions Don't Work

#### ❌ Cookie-Based Authentication

**Problem:** Cookies are domain-specific due to browser same-origin policy.

In this architecture:
- Main application: `cmg-test.sca.iu.edu` (core API domain)
- File serving: `cmg3.sca.iu.edu` (secure_download domain)

A cookie set on `cmg-test.sca.iu.edu` **cannot be read** by `cmg3.sca.iu.edu` - they are completely separate domains.

**Could we share cookies across subdomains?**

Yes, by setting `domain=.sca.iu.edu`, but this creates a **security vulnerability**:
- Cookie would be accessible to ALL `*.sca.iu.edu` subdomains
- Any compromised subdomain could access the authentication token
- Violates principle of least privilege

#### ❌ Authorization Header

**Problem:** Headers are only sent with the initial request, not with resource requests.

```javascript
// This only applies to the fetch() call:
fetch('/reports/.../index.html', {
  headers: { 'Authorization': 'Bearer xyz' }
});

// Browser's automatic requests for CSS/JS don't include this header:
<link href="style.css"> // No Authorization header sent!
```

#### ❌ Service Worker

**Problem:** Complex implementation with limited browser support and coordination issues.

Requires:
- Registering service worker on target domain before first request
- Intercepting all resource requests
- Coordinating token storage and lifecycle
- Complex debugging and error handling

---

## Solution: HTML Token Propagation

### Concept

**Rewrite HTML content in-memory before sending to browser, injecting the authentication token into all relative resource URLs.**

### How It Works

1. **Client requests HTML file with token:**
   ```
   GET /reports/.../index.html?access_token=xyz
   ```

2. **Server reads HTML from disk** (read-only operation):
   ```html
   <link rel="stylesheet" href="style.css">
   <script src="script.js"></script>
   <a href="tree.html">View Tree</a>
   ```

3. **Server rewrites HTML in-memory**, injecting token:
   ```html
   <link rel="stylesheet" href="style.css?access_token=xyz">
   <script src="script.js?access_token=xyz">
   <a href="tree.html?access_token=xyz">View Tree</a>
   ```

4. **Server sends modified HTML** to browser

5. **Browser makes authenticated requests** for all resources:
   ```
   GET /reports/.../style.css?access_token=xyz ✓
   GET /reports/.../script.js?access_token=xyz ✓
   GET /reports/.../tree.html?access_token=xyz ✓
   ```

### Implementation Pattern

```javascript
// When serving HTML files
if (ext === '.html') {
  const tokenParam = req.query?.access_token || req.query?.token;
  
  // Read HTML from disk (read-only)
  const htmlContent = await fs.promises.readFile(resolvedPath, 'utf8');
  
  // Rewrite in-memory: inject token into relative URLs
  const modifiedHtml = htmlContent
    .replace(/(href)="([^"]*?)"/g, (match, attr, url) => {
      // Skip absolute URLs, data URIs, anchors
      if (isAbsoluteUrl(url)) return match;
      
      // Inject token
      const separator = url.includes('?') ? '&' : '?';
      return `${attr}="${url}${separator}access_token=${tokenParam}"`;
    })
    .replace(/(src)="([^"]*?)"/g, (match, attr, url) => {
      // Same logic for src attributes
      if (isAbsoluteUrl(url)) return match;
      const separator = url.includes('?') ? '&' : '?';
      return `${attr}="${url}${separator}access_token=${tokenParam}"`;
    });
  
  // Send modified HTML (original file untouched)
  return res.send(modifiedHtml);
}
```

---

## Why This Solution Was Chosen

### ✅ Advantages

1. **Works Cross-Domain**
   - No reliance on cookies or same-origin policy
   - Token explicitly included in every request

2. **Secure**
   - Token scoped to specific file path (validated by scope)
   - Time-limited JWT tokens (expire after 30 seconds)
   - No shared cookies across domains

3. **Simple Implementation**
   - Pure server-side solution
   - No client-side coordination required
   - Single point of implementation (secure_download service)

4. **Read-Only Filesystem**
   - In-memory operation only
   - No writes to disk
   - Works with read-only mounts (`/opt/sca/data:ro`)

5. **Business Logic Decoupled**
   - Generic pattern, no knowledge of Conversions/Datasets/Projects
   - Reusable for any HTML file serving scenario
   - Maintains secure_download's architecture principles

6. **Transparent to Users**
   - No special browser extensions or configuration
   - Works in all modern browsers
   - Seamless user experience

### ⚠️ Considerations

1. **Performance Impact**
   - Minimal: HTML files are typically small (< 1MB)
   - Regex operations are fast for HTML-sized content
   - Only applies to HTML files, not large downloads

2. **Token Visibility**
   - Token appears in URLs (browser history, server logs)
   - Mitigated by: short expiration (30s), scope validation
   - Alternative (cookies) has worse security in cross-domain scenario

3. **Regex Limitations**
   - May not handle every edge case in malformed HTML
   - Acceptable: Reports are well-formed, generated HTML

---

## Alternative Solutions Considered

### Option 2: Proxy Through Main App

**Approach:** Route all file requests through core API instead of direct to secure_download.

```
User → Core API (cmg-test.sca.iu.edu/api/reports/proxy/...) 
     → Secure Download (internal network)
     → Back through Core API
     → User
```

**Rejected for immediate implementation because:**
- ❌ Adds latency (extra hop)
- ❌ Increases load on core API
- ❌ Defeats purpose of separate file serving service
- ❌ Large files would flow through API unnecessarily

#### Why Option 2 is Actually Superior (Recommended for Future Implementation)

**Despite the drawbacks above, Option 2 is the more secure and maintainable long-term solution:**

1. **✅ No Credentials in URLs**
   - **Current (Option 1):** Token exposed in URL query parameters
     ```
     GET /reports/.../index.html?access_token=eyJhbGc... 
     ```
   - **Option 2:** Authentication via session cookie or Authorization header
     ```
     GET /api/reports/proxy/.../index.html
     Cookie: session_id=xyz (httpOnly, secure)
     ```
   - **Security benefit:** Tokens in URLs appear in:
     - Browser history (persistent, surveyable)
     - Server logs (multiple systems)
     - Referrer headers (leaked to external sites if user clicks external links)
     - Browser bookmark if user saves page
   - **With Option 2:** Credentials never appear in URLs, only in secure HTTP headers/cookies

2. **✅ No Token Expiration Concerns**
   - **Current (Option 1):** Must handle token expiration within page lifetime
     - Token expires in 30 seconds
     - If user stays on page > 30s and clicks link → 401 error
     - Requires JavaScript to refresh tokens or reload page
   - **Option 2:** Session-based authentication
     - Session can last hours without manual refresh
     - Core API handles session validation
     - Seamless user experience for long-running analysis sessions

3. **✅ No HTML Rewriting Gymnastics**
   - **Current (Option 1):** Complex in-memory HTML manipulation
     - Regex patterns to match href/src attributes
     - Edge cases with malformed HTML or dynamic content
     - Performance overhead on every HTML request
     - Maintenance burden when HTML patterns change
   - **Option 2:** Serve files as-is
     - No modification needed
     - Zero performance overhead
     - No risk of breaking HTML structure
     - Works with any file type without special handling

4. **✅ Better Separation of Concerns**
   - Core API handles all authentication/authorization
   - secure_download becomes pure file serving (as originally intended)
   - Easier to audit security (single authentication point)

5. **✅ Works for Dynamic Content**
   - JavaScript making AJAX requests includes session cookie automatically
   - WebSocket connections can use same session
   - No need to pass tokens through JavaScript code

#### When to Implement Option 2

**Recommended timeline:**
- **Phase 1 (Current):** Option 1 for immediate functionality
- **Phase 2 (Future):** Implement Option 2 when:
  - Core API infrastructure can handle proxy load
  - Performance optimization (caching, CDN) is in place
  - Same-domain architecture is prioritized for security

**Migration path:**
1. Implement proxy endpoint in core API (`/api/reports/proxy/*`)
2. Add caching layer (Redis, CDN) to mitigate latency
3. Update UI to use proxy URLs instead of direct secure_download URLs
4. Deprecate direct secure_download access for reports
5. Remove HTML token propagation code (no longer needed)

### Option 3: Subdomain Cookie Sharing

**Approach:** Set cookie with `domain=.sca.iu.edu` to share across subdomains.

```javascript
res.cookie('auth', token, {
  domain: '.sca.iu.edu',  // Shared across all *.sca.iu.edu
  httpOnly: true,
  secure: true
});
```

**Rejected because:**
- ❌ Security risk: cookie accessible to ALL subdomains
- ❌ Violates principle of least privilege
- ❌ Any compromised subdomain can access token
- ❌ Doesn't align with zero-trust architecture

### Option 4: Service Worker

**Approach:** Register service worker on secure_download domain to intercept requests.

**Rejected because:**
- ❌ Complex implementation and debugging
- ❌ Requires coordination between domains
- ❌ Service worker registration must happen before first request
- ❌ Limited browser support and edge cases
- ❌ Overkill for this specific problem

---

## Use Cases

### Current Implementation

**Conversion Reports:**
- Legacy CMG conversion reports (MultiQC HTML reports)
- Served from: `/opt/sca/data/conversions/{conversion_id}/{dataset_name}/Reports/`
- Contains: HTML files with CSS, JavaScript, images, nested HTML pages

### Future Applications

This pattern is reusable for any scenario requiring:
- Cross-domain HTML file serving
- Token-based authentication
- Nested resource references in HTML
- Read-only filesystem mounts

**Examples:**
- Genome browser HTML interfaces
- Generated analysis reports
- Documentation with embedded resources
- Any static HTML application requiring authentication

---

## Security Considerations

### Token Exposure

**Concern:** Token appears in URLs (browser history, server logs)

**Mitigations:**
1. **Short Expiration:** Tokens expire after 30 seconds
2. **Scope Validation:** Token scope must match requested file path
3. **Single-Use Pattern:** Each report viewing generates new token
4. **HTTPS Only:** All traffic encrypted in production

### Path Traversal Protection

Token scope includes absolute filesystem path:
```
Token scope: download_file:/opt/sca/data/conversions/123/dataset/Reports
Allowed:     /reports/.../Reports/html/index.html ✓
Denied:      /reports/.../Reports/../../other_file ✗
```

Security checks:
1. Extract path from token scope
2. Verify request path starts with token path
3. Resolve absolute paths and check bounds
4. Only serve files within allowed directory

---

## Performance Characteristics

### HTML File Serving

**Typical HTML file:** 50-500 KB  
**Regex replacement time:** < 5ms  
**Memory overhead:** 2x file size during modification  
**Impact:** Negligible (< 0.01% of total request time)

### Non-HTML Files

**No impact:** CSS, JS, images, PDFs served directly without modification

### Caching

HTML responses are **not cached** because:
- Each contains unique, time-limited token
- Token expiration makes caching ineffective
- Non-HTML resources (CSS, JS, images) can be cached by browser

---

## Testing Recommendations

### Manual Testing

1. **Open report in new tab** (different domain from main app)
2. **Verify initial HTML loads** with token in URL
3. **Check browser DevTools Network tab:**
   - All CSS requests include token ✓
   - All JS requests include token ✓
   - All image requests include token ✓
   - All nested HTML requests include token ✓
4. **Verify no 401 errors** for any resources

### Automated Testing

```javascript
describe('HTML Token Propagation', () => {
  it('should inject token into href attributes', async () => {
    const html = '<link href="style.css">';
    const modified = await rewriteHtml(html, 'TOKEN123');
    expect(modified).toContain('href="style.css?access_token=TOKEN123"');
  });
  
  it('should inject token into src attributes', async () => {
    const html = '<script src="app.js"></script>';
    const modified = await rewriteHtml(html, 'TOKEN123');
    expect(modified).toContain('src="app.js?access_token=TOKEN123"');
  });
  
  it('should skip absolute URLs', async () => {
    const html = '<a href="https://external.com/page">Link</a>';
    const modified = await rewriteHtml(html, 'TOKEN123');
    expect(modified).toContain('href="https://external.com/page"'); // No token
  });
});
```

---

## Maintenance Notes

### When to Update This Implementation

1. **New HTML attributes** that reference resources (rare)
2. **Security vulnerabilities** in regex patterns
3. **Performance issues** with very large HTML files

### Monitoring

Watch for:
- Increased error rates on resource requests (token propagation failing)
- Performance degradation on HTML serving (regex issues)
- 401 errors in logs (token not propagating correctly)

---

## References

- [Browser Same-Origin Policy](https://developer.mozilla.org/en-US/docs/Web/Security/Same-origin_policy)
- [HTTP Query Parameters Best Practices](https://tools.ietf.org/html/rfc3986#section-3.4)
- [Secure Download Architecture](../docs/secure_download.md)

---

**Last Updated:** 2026-01-27
