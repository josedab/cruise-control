# Building Cruise Control

## Prerequisites

- Java 11 or higher
- Gradle (wrapper included, no installation needed)
- Git

## Quick Build

```bash
# Clone the repository
git clone https://github.com/linkedin/cruise-control.git
cd cruise-control

# Build with Gradle
./gradlew build

# Skip tests for faster build
./gradlew build -x test
```

## Build Outputs

After a successful build, you'll find:

```
cruise-control/
├── build/
│   ├── libs/
│   │   ├── cruise-control-*.jar          # Main JAR
│   │   └── cruise-control-*-sources.jar  # Sources JAR
│   ├── distributions/
│   │   ├── cruise-control-*.tar          # Distribution tarball
│   │   └── cruise-control-*.zip          # Distribution zip
│   └── reports/
│       ├── tests/                         # Test reports
│       └── checkstyle/                    # Code style reports
```

## Build Tasks

### Common Tasks

```bash
# Clean build outputs
./gradlew clean

# Compile only (no tests)
./gradlew compileJava compileTestJava

# Run tests
./gradlew test

# Run checkstyle
./gradlew checkstyleMain checkstyleTest

# Generate Javadoc
./gradlew javadoc
# Output: build/docs/javadoc/index.html

# Create distribution
./gradlew distTar
# Output: build/distributions/cruise-control-*.tar
```

### Full Clean Build

```bash
# Clean + build + test + checkstyle
./gradlew clean build
```

## IDE Setup

### IntelliJ IDEA

1. Open IntelliJ IDEA
2. File → Open → Select `cruise-control` directory
3. IntelliJ will auto-detect Gradle and import the project
4. Wait for indexing to complete
5. Set Java 11 SDK: File → Project Structure → Project → SDK

**Code Style:**
- File → Settings → Editor → Code Style → Java
- Scheme → Import Scheme → IntelliJ IDEA code style XML
- Select `config/intellij-java-style.xml`

### Eclipse

```bash
# Generate Eclipse project files
./gradlew eclipse

# Open Eclipse
# File → Import → Existing Projects into Workspace
# Select cruise-control directory
```

### VS Code

1. Install extensions:
   - Java Extension Pack
   - Gradle for Java

2. Open folder: `cruise-control`

3. VS Code will auto-detect Gradle

## Troubleshooting

### Build Fails with "OutOfMemoryError"

Increase Gradle memory:

```bash
# Edit gradle.properties
echo "org.gradle.jvmargs=-Xmx2048m -XX:MaxPermSize=512m" >> gradle.properties

# Or set environment variable
export GRADLE_OPTS="-Xmx2048m"
```

### Checkstyle Errors

Fix automatically where possible:

```bash
./gradlew spotlessApply  # If spotless is configured
```

Or fix manually based on `build/reports/checkstyle/main.html`

### Test Failures

Run specific test to debug:

```bash
./gradlew test --tests ExecutorTest --info
```

View test report: `build/reports/tests/test/index.html`

## Building for Production

```bash
# Clean build with all checks
./gradlew clean build

# Create distribution
./gradlew distTar

# Extract distribution
cd build/distributions
tar -xf cruise-control-*.tar

# Distribution structure:
cruise-control-*/
├── bin/
│   ├── kafka-cruise-control-start.sh
│   └── kafka-cruise-control-stop.sh
├── config/
│   ├── cruisecontrol.properties
│   └── log4j.properties
└── cruise-control-libs/
    └── *.jar
```

## Related Documentation

- [Contributing Guide](contributing.md)
- [Testing Guide](testing.md)
