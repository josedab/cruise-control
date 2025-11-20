# Configuration Validation Framework

## Overview

The Configuration Validation Framework provides comprehensive validation of Cruise Control configuration at startup, catching 90%+ of common misconfigurations before they cause runtime issues.

## Benefits

- **Early Error Detection**: Catch configuration errors at startup instead of hours later during operations
- **Clear Error Messages**: Get actionable suggestions for fixing configuration issues
- **Reduced Downtime**: Prevent production incidents caused by misconfiguration
- **Faster Troubleshooting**: Identify configuration problems in seconds instead of minutes or hours

## Validation Levels

The framework performs validation at four levels:

### 1. Syntax Validation

Checks basic configuration syntax:
- Valid goal class names (classes exist on classpath)
- Proper format for host:port lists (bootstrap.servers, zookeeper.connect)
- Required properties are present

### 2. Range Validation

Validates value ranges and constraints:
- Balance thresholds >= 1.0
- Positive integers where required
- Percentages between 0-100
- Port numbers in valid range (1-65535)

### 3. Semantic Validation

Checks logical relationships between configurations:
- Anomaly detection has notifier or self-healing enabled
- Throttling settings are consistent (cluster max vs broker max)
- Goal ordering follows best practices (RackAwareGoal first, capacity before distribution)

### 4. External System Validation

Tests connectivity to external systems:
- Kafka cluster connectivity
- Metrics topic existence or auto-create capability
- Security configuration validity

## Configuration Options

Control validation behavior with these configuration properties:

```properties
# Enable/disable validation (default: true)
config.validation.enabled=true

# Validation strictness: strict, normal, lenient (default: normal)
# - strict: Warnings also fail startup
# - normal: Only errors fail startup
# - lenient: Log errors but start anyway
config.validation.strictness=normal

# Stop on first error (default: false)
config.validation.fail.fast=false

# Check external connectivity (default: true)
config.validation.check.connectivity=true

# Connectivity check timeout in ms (default: 10000)
config.validation.connectivity.timeout.ms=10000

# Generate detailed reports (default: true)
config.validation.detailed.report=true
```

## Usage

### Automatic Validation at Startup

Validation runs automatically when Cruise Control starts (if enabled):

```bash
# Start Cruise Control - validation runs automatically
./kafka-cruise-control-start.sh config/cruisecontrol.properties
```

If validation fails, you'll see a detailed report:

```
╔═══════════════════════════════════════════════════════════╗
║       Cruise Control Configuration Validation             ║
╚═══════════════════════════════════════════════════════════╝

Status: ❌ FAILED (2 errors, 1 warnings)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ ERROR #1: goals
   Property: goals
   Issue: Invalid goal class: InvalidGoalName
   Fix: Check spelling or remove invalid goal. Valid goals must be
        on the classpath.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ ERROR #2: bootstrap.servers
   Property: bootstrap.servers
   Issue: Cannot connect to Kafka: Connection refused
   Fix: Check that Kafka is running and accessible at: localhost:9092

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️  WARNING #1: max.num.cluster.movements
   Property: max.num.cluster.movements
   Issue: Cluster max (1000) may be too high for broker max (2)
   Suggestion: Consider reducing max.num.cluster.movements or increasing
               num.concurrent.partition.movements.per.broker

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ Configuration validation failed. Fix errors above before starting.
```

### Command-Line Validation Tool

Validate configuration without starting Cruise Control:

```bash
# Validate a configuration file
./kafka-cruise-control-config-validate.sh config/cruisecontrol.properties
```

Exit codes:
- `0` - Configuration is valid
- `1` - Configuration has errors
- `2` - Configuration has warnings only
- `3` - Tool error (file not found, etc.)

### Programmatic Usage

```java
// Create configuration
KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

// Run validation
ConfigurationValidator validator = new ConfigurationValidator(config);
ValidationResult result = validator.validate();

// Check results
if (result.hasErrors()) {
    System.out.println("Errors: " + result.errorCount());
    for (ValidationError error : result.getErrors()) {
        System.out.println(error.getProperty() + ": " + error.getMessage());
    }
}
```

## Common Issues Detected

### 1. Invalid Goal Class Names

**Problem:**
```properties
goals=RackAwareGoal,InvalidGoalName,ReplicaCapacityGoal
```

**Detected:**
```
❌ ERROR: goals
   Issue: Invalid goal class: InvalidGoalName
   Fix: Check spelling or remove invalid goal
```

### 2. Missing Anomaly Action

**Problem:**
```properties
anomaly.detection.goals=RackAwareGoal,ReplicaCapacityGoal
# Missing: self.healing.enabled or anomaly.notifier.class
```

**Detected:**
```
⚠️  WARNING: anomaly.detection.goals
   Issue: Anomaly detection configured but no action enabled
   Fix: Enable self-healing (self.healing.enabled=true) OR
        configure a notifier (anomaly.notifier.class=...)
```

### 3. Inconsistent Throttling

**Problem:**
```properties
max.num.cluster.movements=1000
num.concurrent.partition.movements.per.broker=2
```

**Detected:**
```
⚠️  WARNING: max.num.cluster.movements
   Issue: Cluster max (1000) may be too high for broker max (2)
   Fix: Consider adjusting throttling settings
```

### 4. Invalid Value Ranges

**Problem:**
```properties
cpu.balance.threshold=0.9  # Must be >= 1.0
```

**Detected:**
```
❌ ERROR: cpu.balance.threshold
   Issue: Balance threshold must be >= 1.0, got: 0.9
   Fix: Set cpu.balance.threshold to a value >= 1.0
```

### 5. Connectivity Issues

**Problem:**
```properties
bootstrap.servers=nonexistent-broker:9092
```

**Detected:**
```
❌ ERROR: bootstrap.servers
   Issue: Cannot connect to Kafka: Connection refused
   Fix: Check that Kafka is running and accessible
```

## Disabling Validation

To disable validation (not recommended for production):

```properties
config.validation.enabled=false
```

Or skip connectivity checks only:

```properties
config.validation.check.connectivity=false
```

## Performance Impact

- **Syntax/Range/Semantic validation**: < 100ms
- **Connectivity validation**: 1-3 seconds (configurable timeout)
- **Total overhead**: Typically 1-3 seconds at startup

This is negligible compared to the time saved by catching issues early.

## Best Practices

1. **Always enable validation** in production environments
2. **Use "normal" strictness** for most cases
3. **Review warnings** - they often indicate suboptimal configurations
4. **Test configurations** with the command-line tool before deployment
5. **Set appropriate timeout** for connectivity checks in slow networks

## Extending the Framework

To add custom validation:

```java
public class CustomValidator implements ConfigValidator {
    @Override
    public ValidationResult validate(KafkaCruiseControlConfig config) {
        List<ValidationError> errors = new ArrayList<>();

        // Add your validation logic
        if (someCondition) {
            errors.add(ValidationError.error(
                "property.name",
                "Description of the issue",
                "Suggestion for fixing it"
            ));
        }

        return new ValidationResult(errors);
    }
}
```

Then register it in `ConfigurationValidator`:

```java
_validators.add(new CustomValidator());
```

## Troubleshooting

### Validation Takes Too Long

Reduce connectivity timeout:
```properties
config.validation.connectivity.timeout.ms=5000
```

Or disable connectivity checks:
```properties
config.validation.check.connectivity=false
```

### False Positives

Use lenient mode to log but not fail:
```properties
config.validation.strictness=lenient
```

Then report the issue so we can improve validation logic.

### Validation Fails but Configuration Works

This might indicate:
1. A legitimate warning you should address
2. A bug in the validation framework (please report!)
3. An edge case the framework doesn't handle

You can disable validation as a temporary workaround:
```properties
config.validation.enabled=false
```

## Related Documentation

- [Configuration Guide](https://github.com/linkedin/cruise-control/wiki/Configuration)
- [Goals Documentation](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#goals)
- [Anomaly Detector Documentation](https://github.com/linkedin/cruise-control/wiki/Anomaly-Detector)
