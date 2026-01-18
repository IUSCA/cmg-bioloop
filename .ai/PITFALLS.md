# Common Pitfalls to Avoid

This document catalogs frequent mistakes and anti-patterns in the Bioloop codebase.

---

## Database & Prisma

1. **❌ Creating new Prisma instances instead of reusing from `@/db`**
   - Always use: `const prisma = require('@/db');`
   - Never use: `const prisma = new PrismaClient();`

2. **❌ Not using transactions for multi-operation API calls**
   - Use `prisma.$transaction()` when operations must be atomic
   - Pass transaction instance to service methods

3. **❌ Repeating complex Prisma includes instead of using constants**
   - Define shared includes in `api/src/constants/prismaIncludes.js`
   - Reuse across routes

---

## Authentication & Authorization

4. **❌ Using `auth.hasRole()` directly instead of `auth.canAdmin`/`auth.canOperate`**
   - Use semantic helper methods for clarity and maintainability

5. **❌ Mounting file exposure router after global auth middleware**
   - File exposure routers must be mounted BEFORE global auth
   - They handle their own cookie-based authentication

---

## UI Components (Vuestic)

6. **❌ Using `va-progress-circular` (correct: `va-progress-circle`)**

7. **❌ Using `va-radio-group` (correct: individual `va-radio` components)**

8. **❌ Forgetting `text-by` and `value-by` on `va-select`**
   - Always specify these props for proper option binding

9. **❌ Adding styling classes without being asked**
   - Avoid: `text-sm`, `bg-gray-100`, `text-red-500`
   - Use Vuestic component props instead

---

## UI Logic

10. **❌ Clearing form fields on track selection (preserve manual input)**
    - Only auto-populate if fields are empty
    - Don't overwrite user's manual selections

11. **❌ Showing toast notifications for UI interactions**
    - Only show toasts for API success/failure
    - Not for: selecting items, opening modals, etc.

---

## Genome Browser

12. **❌ Enabling compression for binary genome files**
    - Always disable compression for `/files/expose/` routes
    - Binary files must not be compressed

13. **❌ Not unmounting React components in Vue wrapper**
    - Always call `reactRoot.unmount()` in `onBeforeUnmount()`
    - Memory leaks will occur otherwise

14. **❌ Using relative URLs for React components with Web Workers**
    - Convert to absolute URLs
    - Workers cannot resolve relative paths

15. **❌ Hardcoding browser type strings instead of using constants**
    - Use constants from `@/constants`
    - Avoid magic strings

---

## General Code Quality

16. **❌ Not organizing imports at the top of files**
    - All imports should be at the top
    - Group by category (external, db, middleware, services, etc.)

17. **❌ Using `console.log` in API code instead of `logger`**
    - Use `logger` for API/worker code
    - `console.log` only for CLI scripts

18. **❌ Manual error handling instead of using `asyncHandler`**
    - Let `asyncHandler` catch errors automatically
    - Use `createError` for HTTP errors

19. **❌ Hiding errors with try-catch blocks**
    - **Never** use try-catch to silence errors and return success responses
    - Try-catch is acceptable at interface boundaries (e.g., API route handlers catching service errors to set proper status codes)
    - **Never** abuse try-catch within services or business logic to hide failures
    - Example of what NOT to do:
      ```javascript
      try {
        const stats = fs.statSync(filePath);
      } catch (error) {
        logger.error(error);
        // BAD: continuing execution and returning 200
      }
      ```
    - Instead, let errors bubble up or explicitly handle them with proper error responses

---

**Last Updated:** 2026-01-17

