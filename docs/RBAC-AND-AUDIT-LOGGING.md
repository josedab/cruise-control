# Role-Based Access Control (RBAC) and Audit Logging

## Overview

Cruise Control now supports **Role-Based Access Control (RBAC)** for fine-grained authorization and **comprehensive audit logging** for compliance and security monitoring. These features enable enterprise-grade security and meet regulatory requirements such as SOC 2, PCI-DSS, HIPAA, and GDPR.

## Features

### Role-Based Access Control (RBAC)

- **Fine-grained permissions** based on resource, action, and cluster scope
- **Built-in roles**: Admin, Operator, Viewer, Executor, Auditor
- **Custom roles** with flexible permission configurations
- **File-based** role provider (extensible to LDAP, database, etc.)
- **Backward compatible**: disabled by default

### Audit Logging

- **Comprehensive logging** of all API operations
- **Structured logs** in JSON format with complete operation context
- **Multiple destinations**: file, Kafka topic, or custom integrations
- **Query API** for programmatic access to audit logs
- **Compliance-ready** reports for SOC 2, PCI-DSS, and other frameworks

## Getting Started

### Prerequisites

RBAC requires authentication to be enabled. Configure one of the supported authentication methods:

```properties
# Enable security
webserver.security.enable=true

# Choose authentication method: BASIC, JWT, or SPNEGO
webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider

# Credentials file (for BASIC auth)
webserver.auth.credentials.file=/etc/cruisecontrol/credentials
```

### Enabling RBAC

Add the following to your `cruisecontrol.properties`:

```properties
# Enable RBAC
rbac.enabled=true

# Provider type (file, ldap, database, custom)
rbac.provider=file

# Default role for users without explicit role mappings
rbac.default.role=viewer
```

### Enabling Audit Logging

```properties
# Enable audit logging
audit.log.enabled=true

# Destination (file, kafka, custom)
audit.log.destination=file

# Log file path
audit.log.path=/var/log/cruise-control/audit.log
```

## Configuration

### User-Role Mappings

Define user-role mappings in `cruisecontrol.properties` or a separate RBAC configuration file:

```properties
# Admins (full access)
rbac.users.admin=alice@example.com,bob@example.com

# Operators (can execute operations)
rbac.users.operator=charlie@example.com,diana@example.com

# Viewers (read-only)
rbac.users.viewer=eve@example.com,dev-team@example.com
```

### Built-in Roles

#### Admin
- **Permissions**: `admin:*:*`
- **Use case**: Platform administrators, on-call SREs
- **Can do**: Everything

#### Operator
- **Permissions**: Read all, execute operations, modify proposals
- **Use case**: Kafka operators, day-to-day operations
- **Can do**: View state, create/execute rebalances, stop executions

#### Viewer
- **Permissions**: Read-only access to all resources
- **Use case**: Developers, analysts, monitoring dashboards
- **Can do**: View state, proposals, and execution status

#### Executor
- **Permissions**: Read + execute pre-approved proposals
- **Use case**: Automation systems, scheduled rebalancing
- **Can do**: View state and execute operations (but not create proposals)

#### Auditor
- **Permissions**: Read state + audit logs
- **Use case**: Security auditors, compliance teams
- **Can do**: View state and access audit logs

### Custom Roles

Define custom roles with specific permissions:

```properties
# Production-only operator
rbac.users.prod-operator=grace@example.com
rbac.role.prod-operator=state:read:production-*,executions:execute:production-*

# Cluster-specific viewer
rbac.users.cluster-viewer=iris@example.com
rbac.role.cluster-viewer=state:read:us-east-prod,proposals:read:us-east-prod
```

### Permission Format

Permissions follow the format: `resource:action:cluster`

**Resources:**
- `clusters` - Cluster operations
- `brokers` - Broker operations
- `partitions` - Partition operations
- `proposals` - Optimization proposals
- `executions` - Execution operations (rebalance, broker add/remove)
- `config` - Configuration management
- `state` - Cluster state queries
- `admin` - Administrative operations
- `audit` - Audit log access

**Actions:**
- `read` - View/query operations
- `write` - Modify operations
- `execute` - Trigger operations
- `admin` - Administrative actions (wildcard)

**Cluster scope:**
- `*` - All clusters
- `cluster-name` - Specific cluster
- `pattern-*` - Wildcard pattern (e.g., `production-*`)

**Examples:**
```
state:read:*                    # Read state of all clusters
executions:execute:us-east      # Execute operations on us-east cluster
proposals:write:production-*    # Create proposals for production clusters
admin:*:*                       # Full admin access
```

## Audit Logging

### Audit Log Format

Each audit log entry is a JSON object with the following fields:

```json
{
  "_timestamp": "2025-11-20T14:30:45.123Z",
  "_requestId": "req-12345",
  "_user": "charlie@example.com",
  "_sourceIP": "10.0.1.50",
  "_operation": "REBALANCE",
  "_resource": "cluster:us-east",
  "_parameters": {
    "goals": "RackAwareGoal,ReplicaCapacityGoal",
    "dryrun": "false"
  },
  "_result": "SUCCESS",
  "_durationMs": 1800000,
  "_metadata": {
    "brokers_affected": 50,
    "partitions_moved": 1200
  }
}
```

### Operation Types

Common operations logged:
- `REBALANCE` - Cluster rebalance
- `ADD_BROKER` - Add broker to cluster
- `REMOVE_BROKER` - Remove broker from cluster
- `STOP_EXECUTION` - Stop ongoing execution
- `UPDATE_CONFIG` - Configuration change
- `VIEW_STATE` - Cluster state query
- `CREATE_PROPOSAL` - Create optimization proposal

### Results

- `SUCCESS` - Operation completed successfully
- `DENIED` - Authorization denied
- `ERROR` - Operation failed

### Querying Audit Logs

Audit logs can be queried programmatically or accessed directly from the log file.

**File location:** `/var/log/cruise-control/audit.log` (configurable)

**Query by user:**
```bash
grep '"_user":"charlie@example.com"' /var/log/cruise-control/audit.log
```

**Query by operation:**
```bash
grep '"_operation":"REBALANCE"' /var/log/cruise-control/audit.log
```

**Query denied access attempts:**
```bash
grep '"_result":"DENIED"' /var/log/cruise-control/audit.log
```

## Security Considerations

### Authentication Required

RBAC cannot function without authentication. Ensure one of the following is configured:
- Basic authentication with credentials file
- JWT-based authentication
- SPNEGO/Kerberos authentication

### Audit Log Protection

Protect audit logs from tampering:
1. **File permissions**: Ensure audit log files are readable only by authorized users
2. **Log rotation**: Configure log rotation to prevent disk space issues
3. **Backup**: Regularly backup audit logs to external storage
4. **Immutable storage**: Consider streaming to Kafka or immutable storage (S3 Glacier)

### Role Assignment Best Practices

1. **Principle of least privilege**: Assign minimum necessary permissions
2. **Default to viewer**: Use viewer as default role for safety
3. **Regular audits**: Review role assignments periodically
4. **Separation of duties**: Use different roles for different responsibilities
5. **Test in non-production**: Validate role configurations before production deployment

## Migration Guide

### Backward Compatibility

RBAC and audit logging are **disabled by default** to maintain backward compatibility. Existing deployments continue to work unchanged.

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
rbac.default.role=operator  # Permissive default
audit.log.enabled=true
```

**Step 3: Restrict to proper roles**
```properties
rbac.enabled=true
rbac.default.role=viewer  # Safe default
rbac.users.operator=alice@example.com,bob@example.com
rbac.users.admin=admin@example.com
audit.log.enabled=true
```

## Troubleshooting

### User Cannot Perform Operation

1. **Check authentication**: Ensure user is authenticated correctly
2. **Check role assignment**: Verify user has appropriate role in configuration
3. **Check permissions**: Confirm role grants required permission
4. **Check audit logs**: Review denied access attempts in audit log

Example error:
```
User eve@example.com lacks permission: executions:execute:us-east (current roles: VIEWER)
```

### Audit Logs Not Written

1. **Check configuration**: Verify `audit.log.enabled=true`
2. **Check file path**: Ensure parent directory exists and is writable
3. **Check permissions**: Verify process has write permissions to log directory
4. **Check disk space**: Ensure sufficient disk space available

### RBAC Not Enforcing

1. **Check enabled flag**: Verify `rbac.enabled=true`
2. **Check authentication**: RBAC requires authentication to be enabled
3. **Restart service**: Configuration changes require restart

## Performance Considerations

### RBAC

- Minimal performance impact (microseconds per authorization check)
- Role lookups are cached in memory
- No external calls for file-based provider

### Audit Logging

- Asynchronous logging minimizes request latency
- File-based logging: ~1-2ms per log entry
- Kafka-based logging: depends on Kafka cluster performance
- Consider log rotation for high-volume deployments

## Compliance

### SOC 2

RBAC and audit logging help meet SOC 2 Type II requirements:
- User activity logging (audit logs)
- Access control enforcement (RBAC)
- Audit trail retention (configurable)

### PCI-DSS

Helps meet PCI-DSS requirements:
- Requirement 7: Restrict access to cardholder data (RBAC)
- Requirement 10: Track and monitor all access (audit logging)

### HIPAA

Supports HIPAA compliance:
- Access control (RBAC)
- Audit controls (audit logging)
- Person or entity authentication (authentication required)

### GDPR

Helps with GDPR compliance:
- Data access logging (audit logs show who accessed what)
- Access control (RBAC limits data access)

## Examples

### Example 1: Production Setup

```properties
# Authentication
webserver.security.enable=true
webserver.auth.credentials.file=/etc/cruisecontrol/credentials

# RBAC
rbac.enabled=true
rbac.default.role=viewer
rbac.users.admin=admin@company.com
rbac.users.operator=ops-team@company.com,sre-team@company.com
rbac.users.viewer=dev-team@company.com,qa-team@company.com

# Audit Logging
audit.log.enabled=true
audit.log.destination=file
audit.log.path=/var/log/cruise-control/audit.log
```

### Example 2: Multi-Cluster Setup

```properties
# Production cluster operators
rbac.users.prod-ops=prod-ops@company.com
rbac.role.prod-ops=state:read:prod-*,executions:execute:prod-*

# Staging cluster operators
rbac.users.staging-ops=staging-ops@company.com
rbac.role.staging-ops=state:read:staging-*,executions:execute:staging-*

# Global viewers
rbac.users.viewer=everyone@company.com
rbac.role.viewer=state:read:*,proposals:read:*
```

### Example 3: Audit to Kafka

```properties
# Audit logging to Kafka topic
audit.log.enabled=true
audit.log.destination=kafka
audit.log.topic=__CruiseControlAuditLog
audit.log.retention.ms=7776000000  # 90 days
```

## API Reference

### Permission Class

```java
Permission permission = new Permission(Resource.EXECUTIONS, Action.EXECUTE, "us-east");
```

### Role Class

```java
Role admin = Role.admin();
Role operator = Role.operator();
Role customRole = new Role("CUSTOM", permissions);
```

### AuthorizationService

```java
AuthorizationService authz = new AuthorizationService(rbacProvider, true);
boolean authorized = authz.isAuthorized(user, permission);
authz.requirePermission(user, permission);  // Throws UnauthorizedException if denied
```

### AuditLogger

```java
AuditLogEntry entry = AuditLogEntry.builder()
    .requestId("req-123")
    .user("alice@example.com")
    .operation("REBALANCE")
    .result("SUCCESS")
    .build();

auditLogger.log(entry);
```

## Further Reading

- [RFC-0012: RBAC and Audit Logging](../analysis-output/rfcs/RFC-0012-rbac-audit-logging.md)
- [Cruise Control Security Documentation](./security.md)
- [Configuration Guide](./configuration.md)

## Support

For issues or questions:
- GitHub Issues: https://github.com/linkedin/cruise-control/issues
- Mailing List: cruise-control-users@googlegroups.com
