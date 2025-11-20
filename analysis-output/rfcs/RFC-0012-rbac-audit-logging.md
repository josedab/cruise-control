# RFC-0012: Role-Based Access Control and Audit Logging

**Status:** Proposed
**Created:** 2025-11-20
**Authors:** Analysis Team
**Commit Base:** [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

---

## Executive Summary

Cruise Control currently has **basic authentication** (JWT/Kerberos) but **no fine-grained access control** or **audit logging**. Any authenticated user can perform **any operation** (read state, trigger rebalance, stop execution). This RFC proposes **Role-Based Access Control (RBAC)** with **comprehensive audit logging** to meet enterprise security and compliance requirements.

**Impact:** CRITICAL for enterprise compliance (SOC 2, PCI-DSS, HIPAA, FedRAMP)
**Effort:** 25-35 developer-days (5-7 weeks)
**Risk:** Low (additive feature, no breaking changes)

---

## Problem Statement

### Current Security Gaps

#### 1. **No Fine-Grained Access Control**

**Current Behavior:**

```
User Authentication: ✓ (JWT / Kerberos / Basic Auth)
                      ↓
Authorization: ✗ (All or nothing!)
```

**Problem:**
- Junior engineer has same permissions as senior SRE
- Read-only users can trigger rebalances
- Developers can stop production executions
- No separation of duties

**Real Example:**
```
Dev engineer wants to VIEW cluster state (safe)
Current: Must give FULL access (can accidentally trigger rebalance!)
Desired: Give READ-ONLY access
```

#### 2. **No Audit Trail**

**Current State:** Minimal logging

```
[INFO] 2025-11-20 14:30:45 - Rebalance started
[INFO] 2025-11-20 15:15:30 - Rebalance completed
```

**Missing Information:**
- WHO triggered the rebalance?
- WHAT goals were used?
- WHEN was it approved?
- WHY was it triggered? (manual vs anomaly detection)
- WHICH brokers were affected?

**Compliance Problem:** Cannot answer "Who did what, when, and why?"

#### 3. **No Operation History**

**Questions operators need to answer:**
- "Who stopped the rebalance last night?"
- "What configuration changes were made last week?"
- "How many rebalances did user X trigger this month?"
- "Has anyone accessed production cluster over the weekend?"

**Current Answer:** "Check application logs manually (if retained)"

#### 4. **Compliance Violations**

**Regulations requiring audit logs:**

| Regulation | Requirement | Current Status |
|------------|-------------|----------------|
| SOC 2 | User activity logging | ❌ Not compliant |
| PCI-DSS | Access control + audit | ❌ Not compliant |
| HIPAA | Who accessed what data | ❌ Not compliant |
| GDPR | Data access audit trail | ❌ Not compliant |
| FedRAMP | Detailed audit logs | ❌ Not compliant |

**Impact:** Blocks enterprise adoption in regulated industries

---

## Real-World Incidents

### Incident 1: Accidental Production Impact

**Scenario:**
```
Dev engineer testing API: "Let me try this rebalance endpoint..."
POST /rebalance?dryrun=false  # Forgot dryrun=true!
→ Accidentally triggered production rebalance
→ 2 hours of cluster instability
→ Service degradation
```

**Root Cause:** No access control, any authenticated user can execute

**Should Have Been:**
- Dev engineer: READ-ONLY access
- Only SREs: EXECUTE permissions

### Incident 2: Security Audit Failure

**Scenario:**
```
Auditor: "Show me who accessed the production Kafka management
         system last month and what they did"

Response: "We have application logs but no structured audit trail"

Auditor: "Finding: Non-compliant with SOC 2 requirements"
```

**Impact:** Failed audit, delayed customer onboarding

### Incident 3: Insider Threat

**Scenario:**
```
Disgruntled employee with valid credentials:
→ Stops critical rebalance
→ Changes optimization goals to degrade performance
→ No record of who made changes
→ Investigation takes days
```

**Should Have Been:**
- Audit log immediately shows: User X stopped execution at Time Y
- RBAC limits what destructive actions users can perform

---

## Proposed Solution

### Architecture: RBAC + Audit System

```
┌─────────────────────────────────────────────────┐
│              HTTP Request                        │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
       ┌──────────────────┐
       │  Authentication  │  (JWT / Kerberos / SPNEGO)
       │     Filter       │  → Extract user identity
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────┐
       │  Authorization   │  ← Check permissions (NEW)
       │     Filter       │  → Allow or deny
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────┐
       │  Audit Logging   │  ← Log operation (NEW)
       │     Filter       │  → Who, What, When, Result
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────┐
       │   REST Handler   │
       │  (Execute Op)    │
       └──────────────────┘
```

---

## Component 1: Role-Based Access Control

### Permission Model

**Three-Level Hierarchy: Resource → Action → Permission**

#### Resources

```
- clusters          (all clusters or specific cluster ID)
- brokers           (specific broker operations)
- partitions        (partition-level operations)
- proposals         (optimization proposals)
- executions        (execution operations)
- config            (configuration management)
- state             (cluster state queries)
- admin             (administrative operations)
```

#### Actions

```
- read              (view/query operations)
- write             (modify operations)
- execute           (trigger operations)
- admin             (administrative actions)
```

#### Permissions Format

```
resource:action[:cluster]

Examples:
- state:read:*                    # Read state of all clusters
- state:read:us-east              # Read state of us-east cluster only
- executions:execute:production-* # Execute on all production clusters
- proposals:write:*               # Create/modify proposals
- admin:*:*                       # Full admin access
```

### Built-in Roles

#### 1. **Admin Role**

**Permissions:** Everything
```
admin:*:*
```

**Use Case:** Platform administrators, on-call SREs

#### 2. **Operator Role**

**Permissions:** Read all, execute rebalances, stop executions
```
state:read:*
proposals:read:*
proposals:write:*
executions:read:*
executions:execute:*
executions:stop:*
config:read:*
```

**Use Case:** Kafka operators, SREs managing day-to-day operations

#### 3. **Viewer Role**

**Permissions:** Read-only access to everything
```
state:read:*
proposals:read:*
executions:read:*
config:read:*
brokers:read:*
```

**Use Case:** Developers, analysts, monitoring dashboards

#### 4. **Executor Role**

**Permissions:** Read + execute pre-approved proposals only
```
state:read:*
proposals:read:*
executions:read:*
executions:execute:*
```

**Use Case:** Automation systems, scheduled rebalancing

#### 5. **Auditor Role**

**Permissions:** Read state + audit logs
```
state:read:*
audit:read:*
```

**Use Case:** Security auditors, compliance teams

### Configuration: RBAC Setup

**Location:** `security/rbac.properties`

```properties
# ============================================
# RBAC Configuration
# ============================================

# Enable RBAC (default: false for backward compatibility)
rbac.enabled=true

# Authorization provider
rbac.provider=file  # Options: file, ldap, database, custom

# ============================================
# User-Role Mappings (file provider)
# ============================================

# Admins (full access)
rbac.users.admin=alice@example.com,bob@example.com
rbac.role.admin=admin:*:*

# Operators (can execute operations)
rbac.users.operator=charlie@example.com,diana@example.com,ops-team@example.com
rbac.role.operator=state:read:*,proposals:*:*,executions:*:*,config:read:*

# Viewers (read-only)
rbac.users.viewer=eve@example.com,frank@example.com,dev-team@example.com
rbac.role.viewer=state:read:*,proposals:read:*,executions:read:*

# Auditors (read state + audit logs)
rbac.users.auditor=security-team@example.com
rbac.role.auditor=state:read:*,audit:read:*

# ============================================
# Custom Roles
# ============================================

# Production-only operator
rbac.users.prod-operator=grace@example.com
rbac.role.prod-operator=state:read:production-*,executions:execute:production-*

# Non-production operator
rbac.users.nonprod-operator=henry@example.com
rbac.role.nonprod-operator=state:read:staging-*,state:read:dev-*,executions:execute:staging-*,executions:execute:dev-*

# ============================================
# Default Role (unauthenticated/unknown users)
# ============================================
rbac.default.role=viewer  # Safe default: read-only
```

### Implementation: Authorization Service

```java
/**
 * Handles authorization decisions based on RBAC policies.
 */
public class AuthorizationService {
    private final RBACProvider _rbacProvider;

    /**
     * Checks if a user has permission to perform an action.
     *
     * @param user the authenticated user
     * @param permission the required permission (e.g., "executions:execute:us-east")
     * @return true if authorized, false otherwise
     */
    public boolean isAuthorized(User user, Permission permission) {
        // Get user's roles
        Set<Role> roles = _rbacProvider.getRoles(user);

        // Check if any role grants the permission
        for (Role role : roles) {
            if (role.grants(permission)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Enforces authorization, throwing exception if denied.
     *
     * @throws UnauthorizedException if user lacks permission
     */
    public void requirePermission(User user, Permission permission)
            throws UnauthorizedException {
        if (!isAuthorized(user, permission)) {
            throw new UnauthorizedException(
                String.format("User %s lacks permission: %s",
                    user.identity(), permission)
            );
        }
    }
}
```

### REST API Integration

**Before (No Authorization):**

```java
@POST
@Path("/rebalance")
public Response rebalance(@QueryParam("goals") String goals,
                         @QueryParam("dryrun") boolean dryrun) {
    // Anyone authenticated can rebalance!
    OptimizationResult result = kafkaCruiseControl.rebalance(goals, dryrun);
    return Response.ok(result).build();
}
```

**After (With Authorization):**

```java
@POST
@Path("/rebalance")
public Response rebalance(@Context SecurityContext securityContext,
                         @QueryParam("cluster") String cluster,
                         @QueryParam("goals") String goals,
                         @QueryParam("dryrun") boolean dryrun) {
    // Get authenticated user
    User user = (User) securityContext.getUserPrincipal();

    // Check authorization
    Permission required = new Permission("executions", "execute", cluster);
    authorizationService.requirePermission(user, required);

    // Authorized - proceed with operation
    OptimizationResult result = kafkaCruiseControl.rebalance(
        cluster, goals, dryrun
    );

    return Response.ok(result).build();
}
```

**Error Response (Unauthorized):**

```json
{
  "error": "Unauthorized",
  "message": "User eve@example.com lacks permission: executions:execute:us-east",
  "requiredPermission": "executions:execute:us-east",
  "userRoles": ["viewer"],
  "suggestion": "Contact your administrator to request operator role"
}
```

---

## Component 2: Comprehensive Audit Logging

### Audit Log Structure

**Every operation logged with:**

```java
public class AuditLogEntry {
    private final Instant timestamp;
    private final String requestId;         // Unique ID for correlation
    private final String user;              // Who performed the action
    private final String sourceIP;          // Where they came from
    private final String operation;         // What they did
    private final String resource;          // What they acted on
    private final Map<String, String> parameters;  // How they did it
    private final String result;            // SUCCESS, DENIED, ERROR
    private final String errorMessage;      // If failed, why
    private final long durationMs;          // How long it took
}
```

### Audit Log Examples

#### Example 1: Successful Rebalance

```json
{
  "timestamp": "2025-11-20T14:30:45.123Z",
  "requestId": "req-12345",
  "user": "charlie@example.com",
  "sourceIP": "10.0.1.50",
  "operation": "REBALANCE",
  "resource": "cluster:us-east",
  "parameters": {
    "goals": "RackAwareGoal,ReplicaCapacityGoal,DiskCapacityGoal",
    "dryrun": "false",
    "concurrent_movements": "10"
  },
  "result": "SUCCESS",
  "durationMs": 1800000,
  "metadata": {
    "brokers_affected": 50,
    "partitions_moved": 1200,
    "data_moved_gb": 350
  }
}
```

#### Example 2: Unauthorized Access Attempt

```json
{
  "timestamp": "2025-11-20T15:45:12.456Z",
  "requestId": "req-67890",
  "user": "eve@example.com",
  "sourceIP": "10.0.2.100",
  "operation": "STOP_EXECUTION",
  "resource": "cluster:us-east",
  "parameters": {
    "force": "true"
  },
  "result": "DENIED",
  "errorMessage": "User lacks permission: executions:stop:us-east",
  "durationMs": 5
}
```

#### Example 3: Configuration Change

```json
{
  "timestamp": "2025-11-20T16:20:30.789Z",
  "requestId": "req-11111",
  "user": "alice@example.com",
  "sourceIP": "10.0.1.75",
  "operation": "UPDATE_CONFIG",
  "resource": "config:goals",
  "parameters": {
    "old_value": "RackAwareGoal,ReplicaCapacityGoal",
    "new_value": "RackAwareGoal,ReplicaCapacityGoal,CustomGoal"
  },
  "result": "SUCCESS",
  "durationMs": 150
}
```

### Audit Log Storage

#### Option 1: File-Based (Default)

**Location:** `/var/log/cruise-control/audit.log`

**Format:** JSON Lines (one JSON object per line)

**Rotation:** Daily, keep 90 days

**Configuration:**
```properties
audit.log.enabled=true
audit.log.path=/var/log/cruise-control/audit.log
audit.log.format=json
audit.log.rotation.policy=daily
audit.log.retention.days=90
```

#### Option 2: Kafka Topic

**Topic:** `__CruiseControlAuditLog`

**Benefits:**
- Durable, replicated storage
- Can stream to SIEM/analytics
- Kafka-native solution

**Configuration:**
```properties
audit.log.enabled=true
audit.log.destination=kafka
audit.log.topic=__CruiseControlAuditLog
audit.log.retention.ms=7776000000  # 90 days
```

#### Option 3: External System

**Integrations:**
- Splunk
- Elasticsearch
- Datadog
- AWS CloudWatch
- Azure Monitor

**Configuration:**
```properties
audit.log.enabled=true
audit.log.destination=custom
audit.log.exporter.class=com.example.SplunkAuditExporter
```

### Implementation: Audit Filter

```java
/**
 * Servlet filter that logs all API operations for audit purposes.
 */
@WebFilter("/*")
public class AuditLoggingFilter implements Filter {
    private final AuditLogger _auditLogger;

    @Override
    public void doFilter(ServletRequest request,
                        ServletResponse response,
                        FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // Generate request ID
        String requestId = UUID.randomUUID().toString();

        // Extract user
        Principal principal = httpRequest.getUserPrincipal();
        String user = principal != null ? principal.getName() : "anonymous";

        // Extract source IP
        String sourceIP = httpRequest.getRemoteAddr();

        // Extract operation details
        String method = httpRequest.getMethod();
        String path = httpRequest.getRequestURI();
        Map<String, String> parameters = extractParameters(httpRequest);

        long startTime = System.currentTimeMillis();

        try {
            // Execute the request
            chain.doFilter(request, response);

            long duration = System.currentTimeMillis() - startTime;

            // Log successful operation
            _auditLogger.log(new AuditLogEntry(
                Instant.now(),
                requestId,
                user,
                sourceIP,
                determineOperation(method, path),
                determineResource(path),
                parameters,
                httpResponse.getStatus() < 400 ? "SUCCESS" : "ERROR",
                null,
                duration
            ));

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;

            // Log failed operation
            _auditLogger.log(new AuditLogEntry(
                Instant.now(),
                requestId,
                user,
                sourceIP,
                determineOperation(method, path),
                determineResource(path),
                parameters,
                "ERROR",
                e.getMessage(),
                duration
            ));

            throw e;
        }
    }
}
```

---

## Audit Query API

### New Endpoint: `/audit/logs`

**Query audit logs programmatically:**

```
GET /audit/logs?user=charlie@example.com&start=2025-11-01&end=2025-11-20

Response:
{
  "total": 45,
  "logs": [
    {
      "timestamp": "2025-11-20T14:30:45Z",
      "user": "charlie@example.com",
      "operation": "REBALANCE",
      "resource": "cluster:us-east",
      "result": "SUCCESS"
    },
    ...
  ]
}
```

**Query Parameters:**
- `user` - Filter by user
- `operation` - Filter by operation type
- `resource` - Filter by resource
- `result` - Filter by result (SUCCESS, DENIED, ERROR)
- `start` - Start timestamp
- `end` - End timestamp
- `limit` - Maximum results (default: 100)
- `offset` - Pagination offset

### Audit Reports

**Pre-built reports:**

#### 1. User Activity Report

```
GET /audit/reports/user-activity?user=charlie@example.com&days=30

Response:
{
  "user": "charlie@example.com",
  "period": "2025-10-20 to 2025-11-20",
  "summary": {
    "totalOperations": 127,
    "successfulOperations": 125,
    "deniedOperations": 0,
    "failedOperations": 2
  },
  "operationBreakdown": {
    "REBALANCE": 15,
    "STOP_EXECUTION": 3,
    "VIEW_STATE": 98,
    "UPDATE_PROPOSAL": 11
  },
  "accessedClusters": ["us-east", "us-west"]
}
```

#### 2. Security Events Report

```
GET /audit/reports/security-events?days=7

Response:
{
  "period": "Last 7 days",
  "deniedAccess": [
    {
      "timestamp": "2025-11-20T15:45:12Z",
      "user": "eve@example.com",
      "operation": "STOP_EXECUTION",
      "resource": "cluster:us-east",
      "reason": "User lacks permission"
    }
  ],
  "suspiciousActivity": [
    {
      "user": "unknown@external.com",
      "attempts": 50,
      "reason": "Multiple failed authentication attempts"
    }
  ]
}
```

#### 3. Compliance Report

```
GET /audit/reports/compliance?format=soc2

Response:
{
  "report": "SOC 2 Compliance Report",
  "period": "2025-01-01 to 2025-11-20",
  "findings": {
    "userActivityLogged": true,
    "accessControlEnforced": true,
    "auditLogRetention": "90 days",
    "unauthorizedAccessAttempts": 3,
    "dataModifications": 234,
    "administrativeActions": 45
  },
  "compliance": "COMPLIANT"
}
```

---

## UI Integration

### RBAC Management UI

```
╔══════════════════════════════════════════════════════════════╗
║                  RBAC Management                             ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Users & Roles:                                              ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ alice@example.com         [Admin]            [Edit]    │ ║
║  │ charlie@example.com       [Operator]         [Edit]    │ ║
║  │ eve@example.com           [Viewer]           [Edit]    │ ║
║  │ [+ Add User]                                           │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  Roles:                                                       ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ Admin                          5 users                 │ ║
║  │ ├─ Permissions: admin:*:*                             │ ║
║  │ └─ [View Details] [Edit]                              │ ║
║  │                                                         │ ║
║  │ Operator                       12 users                │ ║
║  │ ├─ Permissions: state:read:*, executions:execute:*    │ ║
║  │ └─ [View Details] [Edit]                              │ ║
║  │                                                         │ ║
║  │ Viewer                         30 users                │ ║
║  │ ├─ Permissions: state:read:*, proposals:read:*        │ ║
║  │ └─ [View Details] [Edit]                              │ ║
║  │                                                         │ ║
║  │ [+ Create Custom Role]                                 │ ║
║  └────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════╝
```

### Audit Log Viewer

```
╔══════════════════════════════════════════════════════════════╗
║                    Audit Logs                                ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Filters:                                                     ║
║  User: [All ▼]  Operation: [All ▼]  Result: [All ▼]         ║
║  Date Range: [2025-11-01] to [2025-11-20]  [Apply]          ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ Time              User          Operation      Result  │ ║
║  ├────────────────────────────────────────────────────────┤ ║
║  │ 14:30:45  charlie@example  REBALANCE       ✓ SUCCESS  │ ║
║  │ └─ Cluster: us-east, Goals: RackAware+5 more          │ ║
║  │    [View Details]                                      │ ║
║  ├────────────────────────────────────────────────────────┤ ║
║  │ 15:45:12  eve@example      STOP_EXECUTION  ❌ DENIED   │ ║
║  │ └─ Reason: User lacks permission                       │ ║
║  │    [View Details]                                      │ ║
║  ├────────────────────────────────────────────────────────┤ ║
║  │ 16:20:30  alice@example    UPDATE_CONFIG   ✓ SUCCESS  │ ║
║  │ └─ Changed: goals configuration                        │ ║
║  │    [View Details]                                      │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  [Export CSV] [Export JSON] [Generate Report]               ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Implementation Plan

### Phase 1: RBAC Foundation (Week 1-2)

**Tasks:**
1. Create permission model (Resource, Action, Permission classes)
2. Create Role and User abstractions
3. Implement file-based RBAC provider
4. Create AuthorizationService
5. Write unit tests

**Deliverables:**
- Working RBAC framework
- Configuration-based role assignment

### Phase 2: REST API Integration (Week 3-4)

**Tasks:**
1. Create AuthorizationFilter for REST API
2. Add authorization checks to all endpoints
3. Add user context to all operations
4. Implement proper error responses
5. Update API documentation

**Deliverables:**
- All endpoints protected by RBAC
- Clear unauthorized error messages

### Phase 3: Audit Logging (Week 5-6)

**Tasks:**
1. Create AuditLogEntry model
2. Implement AuditLogger (file + Kafka options)
3. Create AuditLoggingFilter
4. Add audit log query API
5. Implement audit reports

**Deliverables:**
- Complete audit trail for all operations
- Queryable audit logs

### Phase 4: UI & Documentation (Week 7)

**Tasks:**
1. Create RBAC management UI
2. Create audit log viewer UI
3. Add user/role management
4. Write security documentation
5. Create compliance report templates

**Deliverables:**
- User-friendly RBAC management
- Audit log visualization

---

## Configuration Migration

### Backward Compatibility

**RBAC disabled by default:**

```properties
# Old deployments continue to work unchanged
rbac.enabled=false  # Default

# When disabled:
# - All authenticated users have full access (current behavior)
# - Audit logging still works (recommended to enable)
```

### Gradual Rollout

**Step 1: Enable audit logging only**

```properties
rbac.enabled=false
audit.log.enabled=true
audit.log.path=/var/log/cruise-control/audit.log
```

**Step 2: Enable RBAC with permissive default**

```properties
rbac.enabled=true
rbac.default.role=operator  # Give all users operator access initially
audit.log.enabled=true
```

**Step 3: Restrict to proper roles**

```properties
rbac.enabled=true
rbac.default.role=viewer  # Safe default: read-only
rbac.users.operator=alice@example.com,bob@example.com
rbac.users.admin=admin@example.com
audit.log.enabled=true
```

---

## Security Considerations

### Authentication Requirements

**RBAC requires authentication:**

```properties
# Must enable one of:
webserver.security.enable=true
webserver.auth.method=BASIC  # or JWT, SPNEGO
```

**Without authentication, RBAC cannot identify users!**

### Sensitive Data Protection

**Audit logs may contain sensitive data:**

```properties
# Redact sensitive parameters
audit.log.redact.enabled=true
audit.log.redact.parameters=password,token,api_key
```

**Example:**
```json
{
  "parameters": {
    "username": "alice",
    "password": "***REDACTED***"
  }
}
```

### Audit Log Integrity

**Protect against tampering:**

1. **Write-once storage:** Audit logs in Kafka (append-only)
2. **Cryptographic signing:** Sign each log entry
3. **External archival:** Stream to immutable storage (S3 Glacier)

---

## Testing Strategy

### Unit Tests

```java
@Test
public void testAuthorizationSuccess() {
    User user = new User("alice@example.com", Set.of(Role.ADMIN));
    Permission permission = new Permission("executions", "execute", "us-east");

    assertTrue(authorizationService.isAuthorized(user, permission));
}

@Test
public void testAuthorizationDenied() {
    User user = new User("eve@example.com", Set.of(Role.VIEWER));
    Permission permission = new Permission("executions", "execute", "us-east");

    assertFalse(authorizationService.isAuthorized(user, permission));
}

@Test
public void testAuditLogging() {
    AuditLogger logger = new AuditLogger(config);

    logger.log(new AuditLogEntry(
        Instant.now(),
        "req-123",
        "charlie@example.com",
        "10.0.1.50",
        "REBALANCE",
        "cluster:us-east",
        Map.of("goals", "RackAwareGoal"),
        "SUCCESS",
        null,
        1800000
    ));

    // Verify log written
    List<AuditLogEntry> logs = logger.query(
        Map.of("user", "charlie@example.com")
    );

    assertEquals(1, logs.size());
    assertEquals("REBALANCE", logs.get(0).operation());
}
```

### Integration Tests

```java
@Test
public void testUnauthorizedRebalance() {
    // Viewer role should not be able to rebalance
    Response response = given()
        .auth().preemptive().basic("eve@example.com", "password")
        .queryParam("cluster", "us-east")
        .queryParam("goals", "RackAwareGoal")
        .when()
        .post("/rebalance")
        .then()
        .statusCode(403)  // Forbidden
        .extract().response();

    // Check audit log
    List<AuditLogEntry> logs = auditLogger.query(
        Map.of("user", "eve@example.com")
    );

    assertEquals("DENIED", logs.get(0).result());
}
```

---

## Success Metrics

| Metric | Target |
|--------|--------|
| All operations logged | 100% |
| Audit log retention | 90+ days |
| RBAC adoption | 80% of enterprises |
| Unauthorized access blocked | 100% |
| Compliance certifications | SOC 2, PCI-DSS ready |
| Time to answer "who did what" | <1 minute |

---

## Effort Estimate

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: RBAC foundation | 2 weeks | 10 days |
| Phase 2: API integration | 2 weeks | 10 days |
| Phase 3: Audit logging | 2 weeks | 10 days |
| Phase 4: UI & docs | 1 week | 5 days |
| **Total** | **7 weeks** | **35 days** |

---

## References

- [AWS IAM Policies](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
- [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
- [SOC 2 Audit Requirements](https://www.aicpa.org/interestareas/frc/assuranceadvisoryservices/aicpasoc2report.html)
- [NIST Access Control Guidelines](https://csrc.nist.gov/publications/detail/sp/800-162/final)

---

**Priority:** P0 (Critical for enterprise compliance)
**Effort:** 25-35 dev-days
**Dependencies:** Requires authentication enabled
**Breaking Changes:** None (opt-in feature)
