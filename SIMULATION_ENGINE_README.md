# Simulation and What-If Analysis Engine

This document describes the implementation of RFC-0013: Simulation and What-If Analysis Engine for Cruise Control.

## Overview

The Simulation Engine allows operators to test hypothetical scenarios against the current cluster state without risk. This enables:

- **Capacity Planning**: "Can we handle Black Friday traffic (3× normal)?"
- **Failure Testing**: "What happens if broker 5 fails?"
- **Configuration Testing**: "What if we change goal priorities?"
- **Growth Planning**: "Should we add 10 or 20 brokers?"

## Implementation Status

This is a **Phase 1 implementation** that provides core simulation capabilities. The implementation includes:

### ✅ Completed Components

1. **Data Models** (`com.linkedin.kafka.cruisecontrol.simulator.model`)
   - `Assessment` - Capacity health assessment levels
   - `Severity` - Recommendation severity levels
   - `FailureType` - Types of failures that can be simulated
   - `ResourceUtilization` - Resource usage metrics
   - `CapacityAnalysis` - Cluster capacity analysis results
   - `Recommendation` - Actionable recommendations
   - `ImpactAssessment` - Impact of proposed changes
   - `Failure` - Failure specification
   - `BrokerSpec` - Broker specification for additions
   - `PartitionSpec` - Partition specification for additions
   - `Modifications` - Cluster modifications to apply
   - `Scenario` - Complete scenario definition
   - `ClusterStats` - Basic cluster statistics
   - `SimulationReport` - Complete simulation results

2. **Core Simulation Engine** (`com.linkedin.kafka.cruisecontrol.simulator`)
   - `ScenarioBuilder` - Builds simulated cluster models
   - `SimulationExecutor` - Orchestrates simulation execution
   - `AnalysisEngine` - Analyzes results and generates recommendations

3. **REST API Components** (`com.linkedin.kafka.cruisecontrol.servlet`)
   - `SimulationParameters` - Request parameters
   - `SimulationResult` - Response formatter (JSON and text)

4. **Tests**
   - `AnalysisEngineTest` - Unit tests for analysis engine

## Architecture

```
┌──────────────────────────────────────────────────┐
│            Simulation Engine                      │
│                                                   │
│  ┌────────────────────────────────────────────┐ │
│  │     ScenarioBuilder                        │ │
│  │  - Clone current state                     │ │
│  │  - Apply modifications                     │ │
│  │  - Inject failures                         │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     Simulated ClusterModel                 │ │
│  │  - In-memory cluster state                 │ │
│  │  - Sandboxed (no real cluster impact)     │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     SimulationExecutor                     │ │
│  │  - Orchestrates simulation                 │ │
│  │  - Runs optimization                       │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     AnalysisEngine                         │ │
│  │  - Capacity analysis                       │ │
│  │  - Impact assessment                       │ │
│  │  - Recommendations                         │ │
│  └────────────────────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│           Simulation Report                       │
└──────────────────────────────────────────────────┘
```

## Usage Example

```java
// Create simulation scenario
Modifications modifications = new Modifications(
    List.of(new BrokerSpec(51, "rack-1", capacity)),  // Add broker 51
    Collections.emptyList(),  // No brokers to remove
    Collections.emptyList(),  // No partitions to add
    Map.of("CPU", 1.5)  // 1.5× CPU load
);

Scenario scenario = new Scenario(
    "Black Friday Load Test",
    modifications,
    Collections.emptyList()  // No failures
);

// Execute simulation
SimulationExecutor executor = new SimulationExecutor(
    loadMonitor,
    goalOptimizer,
    config
);

SimulationReport report = executor.execute(
    scenario,
    goals,
    requirements
);

// Review recommendations
for (Recommendation rec : report.recommendations()) {
    System.out.println(rec.severity() + ": " + rec.description());
}
```

## Limitations and Assumptions

### Current Limitations

1. **Cluster Model Cloning**: The current implementation works with the live cluster model rather than creating a deep copy. A production implementation should create isolated copies to avoid any potential side effects.

2. **Load Multipliers**: Applying load multipliers to simulate increased traffic is not fully implemented. This would require modifying replica loads which is complex.

3. **Dynamic Partition Addition**: Adding partitions dynamically to a simulation is logged but not fully implemented. This requires creating replicas and assigning them to brokers which is non-trivial.

4. **Optimizer Integration**: The integration with GoalOptimizer is simplified. Full integration would require:
   - Proper operation progress tracking
   - Complete optimization options support
   - Async execution support

5. **REST Endpoint Integration**: The REST API components are created but not fully integrated into the servlet routing. To complete integration:
   - Add `SIMULATE` to `CruiseControlEndPoint` enum
   - Register the endpoint in the servlet
   - Add proper authentication and authorization

6. **Failure Simulation**: Some failure types (DISK_FULL, SLOW_BROKER) are logged but not fully implemented. Complete implementation would require:
   - Modifying broker disk utilization metrics
   - Simulating latency effects on throughput

### Assumptions Made

1. **Read-Only Operation**: Simulations are completely read-only and do not modify the actual cluster state.

2. **Synchronous Execution**: Simulations run synchronously in the current implementation. For production, async execution with progress tracking would be better.

3. **Default Capacity**: When adding brokers, we use the provided capacity specs. Default capacity values can be configured.

4. **Estimation Formulas**: Impact assessment uses simplified estimation formulas for:
   - Data movement duration (assumes 50 MB/s throughput)
   - CPU impact during rebalancing (~12% increase)
   - Network impact (proportional to data volume)

5. **Healthy Thresholds**: Capacity assessment uses these thresholds:
   - Healthy: >30% headroom
   - Adequate: 15-30% headroom
   - Marginal: 5-15% headroom
   - Insufficient: <5% headroom

## Next Steps

To complete the implementation according to RFC-0013, the following work is needed:

### Phase 2: Complete Core Features

1. **Implement Deep Copy for ClusterModel**
   - Create a proper cloning mechanism to isolate simulations
   - Ensure all cluster state is copied correctly

2. **Complete Failure Simulation**
   - Implement DISK_FULL by modifying disk utilization
   - Implement SLOW_BROKER by simulating latency effects
   - Add validation for failure scenarios

3. **Implement Load Multipliers**
   - Modify replica loads based on multipliers
   - Update aggregated loads at broker/rack/cluster levels
   - Support different multipliers per resource type

4. **Complete Optimizer Integration**
   - Integrate with real goal optimization
   - Support custom optimization options
   - Add progress tracking

### Phase 3: REST API & UI

1. **Complete REST Integration**
   - Add SIMULATE endpoint to CruiseControlEndPoint
   - Create request handler (sync or async)
   - Add proper parameter parsing for complex scenarios
   - Support JSON body for scenario definitions

2. **Add Advanced Endpoints**
   - POST /simulate/compare - Compare multiple scenarios
   - POST /simulate/failure - Dedicated failure testing
   - GET /simulate/scenarios - List saved scenarios
   - POST /simulate/projection - Time-series projections

3. **UI Integration**
   - Create simulation dashboard
   - Add scenario builder UI
   - Visualize results and recommendations
   - Save and load scenarios

### Phase 4: Advanced Features

1. **Scenario Management**
   - Save/load scenarios to persistent storage
   - Scenario templates for common use cases
   - Scenario versioning and history

2. **Monte Carlo Simulation**
   - Probabilistic failure modeling
   - Statistical analysis of outcomes
   - Risk assessment

3. **Time-Series Projections**
   - Model growth over time
   - Capacity planning timeline
   - Cost projections

4. **Performance Optimizations**
   - Caching of simulation results
   - Parallel scenario execution
   - Incremental simulation updates

## Testing

### Running Tests

```bash
./gradlew test --tests "*AnalysisEngineTest"
```

### Test Coverage

Current test coverage includes:
- AnalysisEngine capacity analysis
- Assessment health levels
- Recommendation generation

Additional tests needed:
- ScenarioBuilder modifications
- SimulationExecutor orchestration
- Failure injection
- Integration tests with real cluster models

## Files Created

### Source Files

1. **Model Classes** (13 files)
   - `cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/simulator/model/`
     - Assessment.java
     - Severity.java
     - FailureType.java
     - ResourceUtilization.java
     - CapacityAnalysis.java
     - Recommendation.java
     - ImpactAssessment.java
     - Failure.java
     - BrokerSpec.java
     - PartitionSpec.java
     - Modifications.java
     - Scenario.java
     - ClusterStats.java
     - SimulationReport.java

2. **Core Engine** (3 files)
   - `cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/simulator/`
     - ScenarioBuilder.java
     - SimulationExecutor.java
     - AnalysisEngine.java

3. **REST API** (2 files)
   - `cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/parameters/`
     - SimulationParameters.java
   - `cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/response/`
     - SimulationResult.java

### Test Files

1. **Unit Tests** (1 file)
   - `cruise-control/src/test/java/com/linkedin/kafka/cruisecontrol/simulator/`
     - AnalysisEngineTest.java

### Documentation

1. **This README**: SIMULATION_ENGINE_README.md

## Contributing

To extend this implementation:

1. Review RFC-0013 for complete requirements
2. Check the limitations section for known gaps
3. Add tests for any new functionality
4. Update this README with changes

## References

- RFC-0013: Simulation and What-If Analysis Engine
- [Chaos Engineering Principles](https://principlesofchaos.org/)
- [Netflix Chaos Monkey](https://netflix.github.io/chaosmonkey/)
