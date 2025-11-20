# Dependency Graph - Cruise Control

**Analysis Commit:** `e30eaf352c31511241f4dfae457fbcb77022a9c6`
**Build System:** Gradle 8.5
**Package Manager:** Maven Central

## Dependency Tree Overview

```
cruise-control (root)
├── cruise-control-core
│   ├── Jetty (9.4.56) - HTTP server
│   ├── Vert.x (4.5.8) - Reactive web framework
│   ├── Apache Commons Math (3.6.1) - Statistics
│   └── SLF4J + Log4j2 - Logging
│
├── cruise-control-metrics-reporter
│   ├── Kafka Server (4.0.0)
│   ├── Kafka Clients (4.0.0)
│   ├── Yammer Metrics (2.2.0)
│   └── SLF4J + Log4j2
│
└── cruise-control (main)
    ├── cruise-control-core (project)
    ├── cruise-control-metrics-reporter (project)
    ├── Kafka (4.0.0) - Cluster management
    ├── Scala (2.13.13) - Language runtime
    ├── Vert.x (4.5.8) - Web framework
    ├── Jetty (9.4.56) - Legacy web server
    ├── Netty (4.1.118) - Network library
    ├── Dropwizard Metrics (4.2.9) - JMX metrics
    ├── Nimbus JOSE JWT (10.0.2) - JWT auth
    ├── Swagger (2.1.16) - API docs
    ├── Gson (2.9.0) - JSON parsing
    ├── Guava (32.1.3) - Google utilities
    ├── Jackson (2.15.2) - JSON processing
    └── TestContainers (1.21.3) - Testing
```

## Core Dependencies

### Apache Kafka Ecosystem

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `kafka_2.13` | 4.0.0 | Core Kafka client | 2024 | Latest major version |
| `kafka-server` | 4.0.0 | Server-side APIs | 2024 | Required for cluster mgmt |
| `kafka-server-common` | 4.0.0 | Common server utilities | 2024 | |
| `kafka-clients` | 4.0.0 | Producer/Consumer APIs | 2024 | Metrics collection |
| `kafka-storage` | 4.0.0 | Storage layer APIs | 2024 | Added in Kafka 3.5+ |

**Compatibility:** Kafka 2.5+ through 4.0+
**Risk:** ✅ Low - Actively maintained by Apache
**License:** Apache 2.0

### Web Frameworks

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `io.vertx:vertx-core` | 4.5.8 | Reactive runtime | 2024-03 | Default web server |
| `io.vertx:vertx-web` | 4.5.8 | Web framework | 2024-03 | REST API routing |
| `io.vertx:vertx-web-openapi` | 4.5.8 | OpenAPI support | 2024-03 | API validation |
| `io.vertx:vertx-junit5` | 4.5.8 | Testing support | 2024-03 | Unit tests |
| `jetty-servlet` | 9.4.56 | Servlet container | 2024-08 | Legacy support |
| `jetty-server` | 9.4.56 | HTTP server | 2024-08 | Being phased out |

**Migration Status:** Gradual shift from Jetty → Vert.x
**Risk:** ⚠️ Medium - Maintaining two web servers
**License:** Apache 2.0 (Vert.x), Eclipse 2.0 (Jetty)

**Trade-off Analysis:**
- **Vert.x (Reactive):** Non-blocking, better throughput, modern async
- **Jetty (Traditional):** Simpler model, proven stability, synchronous
- **Maintaining Both:** Development burden but zero-downtime migration path

### Network Libraries

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `io.netty:netty-handler` | 4.1.118 | Network I/O | 2024-11 | Very recent |
| `io.netty:netty-transport-native-epoll` | 4.1.118 | Linux optimization | 2024-11 | Performance |

**Risk:** ✅ Low - Industry standard
**License:** Apache 2.0
**Why Chosen:** High-performance async I/O, used by Kafka internally

### Language Runtime

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `scala-library` | 2.13.13 | Scala runtime | 2024 | For Kafka integration |

**Java Version:** 17 (required)
**Why Scala:** Kafka broker code is Scala, needed for deep integration
**Trade-off:** Additional language complexity vs. Kafka compatibility

### Logging

| Dependency | Version | Purpose | Last Update | Security |
|-----------|---------|---------|-------------|----------|
| `slf4j-api` | 1.7.36 | Logging facade | 2022 | ✅ Safe |
| `log4j-slf4j-impl` | 2.17.2 | Log4j2 binding | 2022 | ✅ Post-Log4Shell fix |

**Log4Shell (CVE-2021-44228):** ✅ FIXED in 2.17.2
**Risk:** ✅ Low - Updated version
**Pattern:** Multiple logging appenders (console, file, operation, request)

### Metrics & Monitoring

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `metrics-jmx` | 4.2.9 | Dropwizard JMX | 2021 | Operational metrics |
| `metrics-core` (Yammer) | 2.2.0 | Legacy metrics | 2013 | ⚠️ Old, from Kafka |

**Risk:** ⚠️ Medium - Yammer Metrics is unmaintained (last update 2013)
**Why Still Used:** Required by Kafka broker internals
**Alternative:** Dropwizard Metrics (4.x) is the modern replacement

### Security

| Dependency | Version | Purpose | CVEs | Notes |
|-----------|---------|---------|------|-------|
| `nimbus-jose-jwt` | 10.0.2 | JWT tokens | ✅ Fixed CVE-2025-53864 | Recently updated |
| `bcpkix-jdk15on` | 1.70 | Bouncy Castle crypto | ✅ Safe | Test only |
| `kerb-simplekdc` | 2.1.0 | Kerberos testing | N/A | Test only |

**Recent Security Fix:** CVE-2025-53864 in nimbus-jose-jwt fixed in 10.0.2 (updated in commit 4b1520d)
**Risk:** ✅ Low - Active security monitoring

### JSON Processing

| Dependency | Version | Purpose | Vulnerabilities | Notes |
|-----------|---------|---------|-----------------|-------|
| `gson` | 2.9.0 | Google JSON | ✅ Safe | Lightweight |
| `jackson-databind` | 2.15.2 | Jackson JSON | ✅ Pinned for CVE fix | Forced upgrade |
| `json` (org.json) | 20231013 | Simple JSON | ✅ Safe | Minimal usage |

**Multiple JSON Libraries:** ⚠️ Complexity from different libraries
**Jackson Pin:** Explicitly upgraded to 2.15.2 for vulnerability mitigation
**Why Multiple:** Different transitive dependencies require different libraries

### Utilities

| Dependency | Version | Purpose | Last Update | Notes |
|-----------|---------|---------|-------------|-------|
| `guava` | 32.1.3-jre | Google utilities | 2023 | Collections, caching |
| `commons-math3` | 3.6.1 | Apache Math | 2016 | Statistics, variance |
| `httpclient` | 4.5.13 | Apache HTTP | 2020 | REST calls |
| `commons-codec` | 1.15 | Encoding utils | 2020 | Base64, etc. |
| `snappy-java` | 1.1.10.5 | Compression | 2023 | Kafka compatibility |

**Risk:** ⚠️ Some older versions (commons-math3, httpclient)
**Reason:** Stable APIs, no security issues
**Trade-off:** Stability vs. latest features

### API Documentation

| Dependency | Version | Purpose | Notes |
|-----------|---------|---------|-------|
| `swagger-parser-v3` | 2.1.16 | OpenAPI parsing | Current |
| `swagger-annotations` | 2.0.2 | API annotations | Older version |
| `swagger-core` | 2.0.2 | Core Swagger | Older version |

**OpenAPI Spec:** Generated from `resources/yaml/base.yaml`
**Tool:** `org.openapi.generator` plugin (5.3.0)

### Code Quality

| Dependency | Version | Purpose | Notes |
|-----------|---------|---------|-------|
| `spotbugs` | 4.8.6 | Static analysis | Plugin version 6.0.25 |
| `checkstyle` | 10.0 | Code style | Enforced on build |
| `jsr305` (FindBugs) | 3.0.2 | Annotations | Spotbugs successor |

**Enforcement:** All checks run before tests
**Configuration:** `checkstyle/` dir, `gradle/findbugs-exclude.xml`

### Testing

| Dependency | Version | Purpose | Notes |
|-----------|---------|---------|-------|
| `junit` | 4.13.2 | Unit testing | Latest JUnit 4 |
| `easymock` | 5.5.0 | Mocking | Active |
| `powermock` | 2.0.9 | Static mocking | For legacy code |
| `testcontainers` | 1.21.3 | Integration tests | Recently adopted |
| `commons-io` | 2.11.0 | File utilities | Test only |
| `json-path` | 2.7.0 | JSON assertions | Test only |

**Testing Evolution:** Migration from embedded Kafka → TestContainers
**TestContainers Adoption:** Commit e30eaf3 shows recent migration
**Why Not JUnit 5:** Large codebase migration cost vs. benefit

## Dependency Version Strategy

### Version Pinning
```gradle
scalaVersion=2.13.13
kafkaVersion=4.0.0
nettyVersion=4.1.118.Final
jettyVersion=9.4.56.v20240826
vertxVersion=4.5.8
testcontainersVersion=1.21.3
```

**Strategy:** Pin major versions in `gradle.properties`
**Why:** Reproducible builds, controlled upgrades
**Trade-off:** Safety vs. automatic security updates

### Constraint Forcing

```gradle
constraints {
    implementation("commons-beanutils:commons-beanutils:1.11.0") {
        because("version 1.9.4 from kafka 3.9.1 has CVE-2025-48734")
    }
}
```

**Pattern:** Override transitive dependencies for security
**Example:** Forced commons-beanutils upgrade for CVE fix

### Dependency Updates Plugin

```gradle
plugin: "com.github.ben-manes.versions" version "0.42.0"
```

**Purpose:** Check for newer dependency versions
**Command:** `./gradlew dependencyUpdates`
**Filter:** Excludes alpha/beta/rc releases

## Dependency Health Analysis

### 🟢 Healthy (Active Development)
- Kafka ecosystem (4.0.0) - Apache active
- Vert.x (4.5.8) - Eclipse Foundation
- Netty (4.1.118) - Very recent update
- TestContainers (1.21.3) - Modern testing
- Nimbus JOSE JWT (10.0.2) - Security updates
- Guava (32.1.3) - Google maintained

### 🟡 Older But Stable
- Apache Commons Math3 (3.6.1, 2016) - No replacements needed
- HttpClient (4.5.13, 2020) - Consider HttpClient 5.x
- Jetty (9.4.x) - Being phased out, Jetty 10+ uses Jakarta EE
- Gson (2.9.0) - Could upgrade to 2.10+

### 🔴 Concerning
- Yammer Metrics (2.2.0, 2013) - **Abandoned** but required by Kafka internals
  - **Mitigation:** Also uses Dropwizard Metrics 4.x for new code
  - **Future:** Remove when Kafka drops dependency

### Security Vulnerabilities Fixed
1. **CVE-2025-53864** - nimbus-jose-jwt (fixed in 10.0.2)
2. **CVE-2025-48734** - commons-beanutils (forced to 1.11.0)
3. **Log4Shell** - log4j2 (using 2.17.2, post-fix)
4. **Jackson CVEs** - jackson-databind (pinned to 2.15.2)

## License Compatibility

| License | Libraries | Compatibility |
|---------|-----------|---------------|
| Apache 2.0 | Kafka, Vert.x, Netty, Guava, Commons, etc. | ✅ Compatible with BSD 2-Clause |
| Eclipse 2.0 | Jetty | ✅ Compatible |
| MIT | SLF4J | ✅ Compatible |
| BSD | Scala | ✅ Compatible |

**Project License:** BSD 2-Clause
**Risk:** ✅ Low - All dependencies are permissive licenses

## Transitive Dependency Complexity

**Total Dependencies (estimated):** ~150+ including transitive
**Management Strategy:**
- Explicit exclusions of conflicting logging frameworks
- Version conflict resolution via constraints
- BOM (Bill of Materials) from Kafka

```gradle
configurations.all {
    exclude group: 'org.slf4j', module: 'slf4j-log4j12'  // Prevent Log4j 1.x
    exclude group: 'log4j', module: 'log4j'              // Prevent Log4j 1.x
    exclude group: 'ch.qos.logback'                       // Use Log4j2 instead
}
```

## Build Tool Versions

| Tool | Version | Purpose |
|------|---------|---------|
| Gradle | 8.5 | Build system |
| Gradle Wrapper | 8.5 | Reproducible builds |
| Java | 17 | Compilation target |

## Dependency Risk Assessment

### Critical Path Dependencies
1. **Kafka Clients (4.0.0)** - Core functionality
   - Risk: 🟢 Low (Apache active)
   - Impact: Critical (entire system depends on it)

2. **Vert.x/Jetty** - API layer
   - Risk: 🟡 Medium (dual maintenance)
   - Impact: High (all user interactions)

3. **Scala Runtime (2.13.13)** - Language
   - Risk: 🟢 Low (stable language)
   - Impact: High (required for Kafka integration)

### Optional Dependencies
- TestContainers - Testing only
- PowerMock - Testing only
- Swagger - Documentation only

## Recommendations

### Short-term (Quick Wins)
1. ✅ **Update Gson** 2.9.0 → 2.10.1 (bug fixes)
2. ✅ **Update HttpClient** 4.5.13 → 5.3+ (modern API)
3. ✅ **Review Jackson version** - Check for newer patches

### Medium-term (Strategic)
1. 🔄 **Complete Jetty → Vert.x migration** - Remove dual maintenance
2. 🔄 **Migrate JUnit 4 → 5** - Modern testing features
3. 🔄 **Reduce JSON libraries** - Standardize on Jackson or Gson

### Long-term (Architectural)
1. 📋 **Remove Yammer Metrics dependency** - Wait for Kafka to drop it
2. 📋 **Consider Kafka Streams** - For metrics processing?
3. 📋 **Evaluate gRPC** - For internal APIs vs. REST

## Dependency Update Monitoring

**Current Practice:**
- Manual updates via PRs
- `dependencyUpdates` task shows available updates
- Security alerts via GitHub Dependabot

**Improvement Opportunity:**
- Automate security update PRs
- Regular quarterly dependency review
- Track EOL dates for dependencies

---

**Key Insight:** The dependency graph reflects a mature, production-grade system with careful attention to security and version management. The dual web server support is the main complexity burden.
