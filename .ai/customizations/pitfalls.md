# CMG-Bioloop Customization Pitfalls

**Extends:** `/bioloop/pitfalls.md`

This document catalogs mistakes specific to CMG customizations (genome browsers, sessions, tracks).

---

## Genome Browser File Serving

1. **❌ Mounting file exposure router after global auth middleware**
   - File exposure routers must be mounted BEFORE global auth
   - They handle their own cookie-based authentication

2. **❌ Enabling compression for binary genome files**
   - Always disable compression for `/files/expose/` routes
   - Binary files must not be compressed

3. **❌ Not supporting range requests**
   - Genome browsers need range request support
   - Set `Accept-Ranges: bytes` header

---

## React-in-Vue Integration

4. **❌ Not unmounting React components in Vue wrapper**
   - Always call `reactRoot.unmount()` in `onBeforeUnmount()`
   - Memory leaks will occur otherwise

5. **❌ Using relative URLs for React components with Web Workers**
   - Convert to absolute URLs
   - Workers cannot resolve relative paths

---

## Genome Browser Naming

6. **❌ Hardcoding browser type strings instead of using constants**
   - Use constants from `@/constants`
   - Avoid magic strings

7. **❌ Using browser-specific naming in shared code**
   - Use generic names: `genomeBrowser`, `selectedBrowserType`
   - Not: `igvBrowser`, `washuBrowser` in shared components

---

**Last Updated:** 2026-01-16

