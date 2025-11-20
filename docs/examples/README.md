# Cruise Control Examples

This directory contains code examples demonstrating how to use Cruise Control programmatically.

## Java Examples

### RebalanceExample.java

Demonstrates how to programmatically trigger a cluster rebalance and monitor its progress.

**Features:**
- Submit rebalance request with specific goals
- Poll for execution completion
- Handle errors gracefully

**Usage:**

```bash
# Compile (requires Jackson dependency)
javac -cp jackson-databind-2.13.0.jar:. java/RebalanceExample.java

# Run
java -cp jackson-databind-2.13.0.jar:. RebalanceExample
```

**Dependencies:**

```xml
<!-- Maven -->
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.13.0</version>
</dependency>
```

```gradle
// Gradle
implementation 'com.fasterxml.jackson.core:jackson-databind:2.13.0'
```

## Prerequisites

All examples require:
- Cruise Control running (default: http://localhost:9090)
- Kafka cluster with metrics configured
- At least 1 hour of metrics collected

## Additional Examples

For more complex examples, see:
- **Custom Goals:** [docs/developer-guide/custom-goals.md](../developer-guide/custom-goals.md)
- **Custom Anomaly Detectors:** [docs/developer-guide/custom-anomaly-detectors.md](../developer-guide/custom-anomaly-detectors.md)

## Contributing Examples

We welcome example contributions! Please:
1. Add clear documentation and comments
2. Include prerequisites and dependencies
3. Test the example thoroughly
4. Update this README

See [Contributing Guide](../developer-guide/contributing.md) for details.
