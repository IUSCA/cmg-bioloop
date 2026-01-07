# CMG-to-Bioloop Sync: Approach Comparison
**Date:** 2026-01-03  
**Purpose:** Compare different synchronization strategies and justify chosen approach

---

## Comparison Matrix

| Approach | Latency | Complexity | CMG Changes | Bioloop ID Stability | MongoDB 4.0.28 Compatible | Risk Level |
|----------|---------|------------|-------------|---------------------|--------------------------|------------|
| **Daily Full Rebuild** (Current) | 24 hours | Low | None | ❌ IDs change daily | ✅ | 🔴 High |
| **Polling-Based Incremental** (Chosen) | 2-5 minutes | Medium | None | ✅ IDs stable | ✅ | 🟢 Low |
| **MongoDB Change Streams** | <1 second | High | None | ✅ IDs stable | ❌ Requires replica set | 🟡 Medium |
| **CMG Event Webhooks** | <1 second | High | ✅ Required | ✅ IDs stable | ✅ | 🟡 Medium |
| **Dual-Write Pattern** | Real-time | Very High | ✅ Required | ✅ IDs stable | ✅ | 🔴 High |

---

## Approach 1: Daily Full Rebuild (Current Python Script)

### Description
Drop all Bioloop tables and repopulate from CMG MongoDB every 24 hours.

### Pros
- ✅ Simple implementation
- ✅ Guaranteed consistency (full snapshot)
- ✅ No change detection logic needed
- ✅ Works with any MongoDB version

### Cons
- ❌ **Critical:** Bioloop IDs regenerate daily
  - User JWT tokens become invalid
  - In-progress file population corrupts
  - URLs with IDs break
- ❌ 24-hour sync lag (unacceptable for active use)
- ❌ High database load (full table scans)
- ❌ Downtime during rebuild

### Verdict
**❌ Not viable for production use** due to ID instability

---

## Approach 2: Polling-Based Incremental Sync (CHOSEN)

### Description
Query CMG MongoDB every 2-5 minutes for records with `updatedAt > lastSync`, upsert in Bioloop using `cmg_id` as lookup key.

### Pros
- ✅ **Bioloop IDs remain stable** (upsert pattern)
- ✅ No CMG code modifications required
- ✅ Works with MongoDB 4.0.28 (no special features needed)
- ✅ Simple to implement and debug
- ✅ Acceptable latency (2-5 minutes) for low-traffic environment
- ✅ Low database load (indexed queries)
- ✅ Graceful degradation (partial sync failures don't break everything)
- ✅ Easy to monitor (sync_metadata table)

### Cons
- ❌ Not real-time (2-5 minute delay)
- ❌ Repeated MongoDB queries (minimal overhead for low traffic)
- ❌ Relies on CMG's `updatedAt` field being accurate

### Implementation Complexity
- **Low-Medium:** Standard Node.js + Prisma + MongoDB driver
- **Estimated effort:** 2-3 days

### Verdict
**✅ Best fit for requirements:**
- No CMG changes
- Stable Bioloop IDs
- Quick implementation
- Acceptable latency

---

## Approach 3: MongoDB Change Streams (CDC)

### Description
Use MongoDB's Change Streams feature to receive real-time notifications of database changes.

### Pros
- ✅ Real-time sync (<1 second latency)
- ✅ Efficient (no polling overhead)
- ✅ Bioloop IDs remain stable
- ✅ No CMG code changes

### Cons
- ❌ **Blocker:** Requires MongoDB replica set (not guaranteed in CMG)
- ❌ **Blocker:** MongoDB 3.6+ with replica set configuration
- ❌ Higher complexity (stream processing, resume tokens)
- ❌ Requires persistent connection to CMG MongoDB
- ❌ Connection failures require complex recovery logic

### MongoDB 4.0.28 Compatibility
**❌ Not compatible without replica set**

MongoDB Change Streams require:
1. MongoDB 3.6+ (✅ CMG has 4.0.28)
2. Replica set deployment (❌ Unknown if CMG has this)
3. Oplog enabled (❌ Unknown if CMG has this)

### Verdict
**❌ Not viable** unless CMG MongoDB is confirmed to be a replica set

---

## Approach 4: CMG Event Webhooks

### Description
Modify CMG to publish HTTP webhooks when key events occur (dataset registered, staging complete, etc.). Bioloop listens for these webhooks.

### Pros
- ✅ Real-time sync (<1 second latency)
- ✅ Efficient (event-driven, no polling)
- ✅ Bioloop IDs remain stable
- ✅ Works with any MongoDB version

### Cons
- ❌ **Blocker:** Requires modifying CMG source code
- ❌ CMG must implement webhook publishing logic
- ❌ CMG must maintain webhook delivery reliability
- ❌ Requires exposing Bioloop API endpoint to CMG
- ❌ Network failures require retry logic in CMG

### Implementation Complexity
- **High:** Requires changes to CMG API, workers, and database operations
- **Estimated effort:** 2-3 weeks (including CMG changes)

### Verdict
**❌ Not viable** due to constraint: "No CMG source code modifications"

---

## Approach 5: Dual-Write Pattern

### Description
Modify CMG to write to both CMG MongoDB and Bioloop PostgreSQL simultaneously on every operation.

### Pros
- ✅ Real-time sync (no lag)
- ✅ Bioloop IDs remain stable
- ✅ Guaranteed consistency (same transaction)

### Cons
- ❌ **Blocker:** Requires extensive CMG code modifications
- ❌ CMG must manage two database connections
- ❌ CMG must handle Bioloop database failures
- ❌ Distributed transaction complexity
- ❌ Performance impact on CMG operations
- ❌ Tight coupling between CMG and Bioloop

### Implementation Complexity
- **Very High:** Requires modifying every CMG database operation
- **Estimated effort:** 1-2 months

### Verdict
**❌ Not viable** due to:
- Constraint: "No CMG source code modifications"
- Excessive complexity
- High risk of breaking CMG

---

## Approach 6: Database Replication (PostgreSQL Foreign Data Wrapper)

### Description
Use PostgreSQL's Foreign Data Wrapper (FDW) to query CMG MongoDB directly from Bioloop.

### Pros
- ✅ No CMG code changes
- ✅ Query CMG data directly from Bioloop
- ✅ No sync lag (always current)

### Cons
- ❌ **Blocker:** Requires read access to CMG MongoDB from Bioloop
- ❌ **Blocker:** mongo_fdw extension has limited MongoDB 4.0 support
- ❌ Performance issues (remote queries)
- ❌ No Bioloop ID stability (data is virtual, not stored)
- ❌ Complex joins across databases
- ❌ CMG MongoDB becomes critical dependency for Bioloop

### Verdict
**❌ Not viable** due to:
- No Bioloop ID stability
- Performance concerns
- Tight coupling

---

## Decision Matrix: Why Polling-Based Incremental Sync Wins

### Requirements Checklist

| Requirement | Daily Rebuild | Polling Incremental | Change Streams | Webhooks | Dual-Write |
|-------------|---------------|---------------------|----------------|----------|------------|
| No CMG code changes | ✅ | ✅ | ✅ | ❌ | ❌ |
| Bioloop ID stability | ❌ | ✅ | ✅ | ✅ | ✅ |
| MongoDB 4.0.28 compatible | ✅ | ✅ | ❌ (needs replica set) | ✅ | ✅ |
| Quick implementation | ✅ | ✅ | ❌ | ❌ | ❌ |
| Acceptable latency (<5 min) | ❌ | ✅ | ✅ | ✅ | ✅ |
| Low risk | ❌ | ✅ | ❌ | ❌ | ❌ |

**Winner:** Polling-Based Incremental Sync (meets all requirements)

---

## Hybrid Approach: Polling + Event Detection

### Our Chosen Strategy Combines Two Patterns

**1. Polling for Data Sync (Every 2-5 minutes)**
- Query CMG for changed records
- Upsert in Bioloop using cmg_id
- Preserve Bioloop IDs

**2. State Change Detection (Every 2-5 minutes)**
- Poll CMG for workflow state changes (staged, archived, etc.)
- Log events in sync_event_log
- Trigger Bioloop-side actions (notifications, etc.)

### Why This Hybrid?
- ✅ Covers both data replication AND event detection
- ✅ Single polling mechanism serves dual purpose
- ✅ No additional infrastructure needed
- ✅ Simple to implement and maintain

---

## Alternative: Polling Frequency Trade-offs

| Frequency | Latency | Database Load | Use Case |
|-----------|---------|---------------|----------|
| 30 seconds | Low | High | High-traffic, near real-time |
| 2 minutes | Medium | Medium | **Recommended for CMG-Bioloop** |
| 5 minutes | Medium-High | Low | Conservative, low-traffic |
| 15 minutes | High | Very Low | Batch processing, non-critical |

**Chosen:** 2-5 minutes (configurable)
- Good balance of latency and load
- Acceptable for ~200 user environment
- Easy to adjust based on observed performance

---

## Migration Path to Real-Time (Future)

### Phase 1: Polling (Current)
- Implement polling-based sync
- Validate stability and correctness
- Monitor performance

### Phase 2: Optimize Polling
- Add indexes to CMG MongoDB (if allowed)
- Tune query performance
- Reduce polling interval if needed

### Phase 3: Hybrid (Polling + Webhooks)
- If CMG team allows, add webhooks for critical events only
- Keep polling as fallback
- Best of both worlds

### Phase 4: Full CDC (If Infrastructure Allows)
- Upgrade CMG MongoDB to replica set
- Implement Change Streams
- Retire polling

**Key:** Polling is not a dead-end; it's a stable foundation for future enhancements

---

## Risk Analysis

### Polling-Based Incremental Sync Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| CMG updatedAt field inaccurate | Low | Medium | Add createdAt fallback, periodic full reconciliation |
| MongoDB connection failures | Medium | Low | Retry logic, connection pooling |
| Sync script crashes | Low | Low | PM2/cron auto-restart, monitoring |
| Partial sync failures | Medium | Low | Continue with other tasks, log failures |
| Bioloop ID collisions | Very Low | High | Use cmg_id uniqueness, transaction safety |
| Sync lag exceeds tolerance | Low | Medium | Reduce polling interval, optimize queries |

**Overall Risk:** 🟢 Low (all risks have mitigations)

---

## Performance Projections

### Current CMG Data Volume (Estimated)
- Users: ~200
- Datasets (raw_data): ~5,000
- Data Products: ~50,000
- Projects: ~500
- Conversions: ~10,000

### Polling Query Performance (Estimated)

```javascript
// Query: Find users updated in last 2 minutes
db.users.find({ updatedAt: { $gt: ISODate("2026-01-03T12:00:00Z") } })

// Expected results per run: 0-10 records
// Query time: <100ms (with index on updatedAt)
```

### Sync Duration Breakdown
- Connect to MongoDB: 50ms
- Query changed records (5 collections): 500ms
- Upsert in Bioloop (10 records avg): 200ms
- Update sync metadata: 50ms
- **Total: <1 second per sync run**

### Resource Usage
- CPU: <5% during sync
- Memory: <100MB
- Network: <1MB per sync
- **Negligible impact on CMG or Bioloop**

---

## Conclusion

**Polling-Based Incremental Sync is the optimal choice because:**

1. ✅ **Meets all hard constraints:**
   - No CMG code changes
   - Bioloop ID stability
   - MongoDB 4.0.28 compatible
   - Quick implementation (2-3 days)

2. ✅ **Balances trade-offs well:**
   - Acceptable latency (2-5 minutes)
   - Low complexity
   - Low risk
   - Easy to monitor and debug

3. ✅ **Scales for current needs:**
   - ~200 users, low traffic
   - Minimal database overhead
   - Room for optimization

4. ✅ **Future-proof:**
   - Can evolve to hybrid (polling + webhooks)
   - Can migrate to CDC if infrastructure allows
   - Polling remains as reliable fallback

**Alternatives rejected:**
- Daily rebuild: ❌ ID instability
- Change Streams: ❌ Requires replica set
- Webhooks: ❌ Requires CMG changes
- Dual-write: ❌ Too complex, requires CMG changes

---

**Recommendation:** Proceed with Polling-Based Incremental Sync as documented in `CMG_BIOLOOP_DB_SYNC_INSTRUCTIONS.md`

