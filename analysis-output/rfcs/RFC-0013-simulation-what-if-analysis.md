# RFC-0013: Simulation and What-If Analysis Engine

**Status:** Proposed
**Created:** 2025-11-20
**Authors:** Analysis Team
**Commit Base:** [`e30eaf3`](https://github.com/linkedin/cruise-control/commit/e30eaf352c31511241f4dfae457fbcb77022a9c6)

---

## Executive Summary

Cruise Control currently supports **dry-run mode** (generate proposals without executing), but lacks **scenario simulation** capabilities. Operators cannot answer "what if" questions like: "What happens if broker 5 fails?" or "Can we handle 2× traffic?" or "What if we add 10 more brokers?" This RFC proposes a **Simulation Engine** that allows testing hypothetical scenarios against current or projected cluster states without risk.

**Impact:** HIGH - Enables proactive capacity planning, reduces production risk
**Effort:** 20-30 developer-days (4-6 weeks)
**Risk:** Low (read-only operation, no production impact)

---

## Problem Statement

### Current Limitations

#### 1. **No Scenario Testing**

**Questions Operators Need to Answer:**

| Question | Current Answer | Desired Answer |
|----------|----------------|----------------|
| "What if broker 5 fails?" | Run it and see 🤷 | Simulate failure, see impact |
| "Can we handle Black Friday traffic (3× normal)?" | Hope for the best | Simulate 3× load, verify capacity |
| "Should we add 10 or 20 brokers?" | Guess | Simulate both, compare costs |
| "What if we change goal priorities?" | Try in prod 😱 | Simulate, preview results |
| "What happens if we lose entire rack?" | Disaster recovery drill | Simulate rack failure |

**Current Approach:**
1. Make change in production
2. Hope it works
3. If not, scramble to fix

**Desired Approach:**
1. Simulate change in sandbox
2. Verify it works
3. Execute with confidence

#### 2. **Limited Capacity Planning**

**Scenario:** Planning for growth

```
Current process:
1. Guess we need 10 more brokers
2. Provision hardware ($$$)
3. Add to cluster
4. Discover we actually needed 15 (or only 5!)
```

**Problem:**
- Over-provisioning: Waste money
- Under-provisioning: Performance issues

**Desired:**
```
1. Simulate adding 5, 10, 15, 20 brokers
2. Compare capacity, cost, utilization
3. Pick optimal number
4. Provision exactly what's needed
```

#### 3. **No Failure Mode Analysis**

**Questions:**
- "What's our blast radius if us-east-1a availability zone fails?"
- "Can we survive losing 3 brokers simultaneously?"
- "What if ZooKeeper is slow (100ms latency)?"

**Current:** No way to test without causing real outages

#### 4. **Risky Configuration Changes**

**Scenario:** Changing optimization goals

```
Current process:
1. Update goals configuration
2. Restart Cruise Control
3. Trigger rebalance
4. Realize new goals cause imbalance
5. Rollback, try again
```

**Risk:** Production instability during trial-and-error

---

## Real-World Use Cases

### Use Case 1: Black Friday Capacity Planning

**Scenario:** E-commerce company preparing for Black Friday

```
Current State (November):
- 50 brokers
- 10,000 partitions
- 1TB/hour ingestion
- CPU: 40% average
- Disk: 60% used

Expected Black Friday Load:
- 3× message volume
- 5× partition count (new topics)

Questions:
1. Can current cluster handle it?
2. How many brokers do we need?
3. What's the minimum viable expansion?
```

**Simulation:**

```
POST /simulate

{
  "scenario": {
    "name": "Black Friday 2025",
    "baseCluster": "production",
    "modifications": {
      "messageRateMultiplier": 3.0,
      "partitionCount": 50000,
      "additionalBrokers": [
        {"id": 51, "rack": "rack-1", "capacity": {...}},
        {"id": 52, "rack": "rack-1", "capacity": {...}},
        ...  // Add 20 brokers
      ]
    }
  },
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal", "DiskCapacityGoal"],
  "analysis": ["capacity", "utilization", "hotspots"]
}

Response:
{
  "scenarioName": "Black Friday 2025",
  "clusterStats": {
    "brokers": 70,  # Added 20
    "partitions": 50000,
    "estimatedLoad": "3TB/hour"
  },
  "capacityAnalysis": {
    "cpu": {
      "max": 78.5,
      "average": 65.2,
      "headroom": 21.5,
      "assessment": "SUFFICIENT"
    },
    "disk": {
      "max": 89.3,
      "average": 75.8,
      "headroom": 10.7,
      "assessment": "MARGINAL - Consider adding 5 more brokers"
    }
  },
  "recommendations": [
    "Add 25 brokers instead of 20 for safer disk headroom",
    "Increase disk capacity on existing brokers",
    "Consider additional rack for better fault tolerance"
  ]
}
```

### Use Case 2: Broker Failure Simulation

**Scenario:** Testing resilience

```
POST /simulate

{
  "scenario": {
    "name": "3 Broker Failure",
    "baseCluster": "production",
    "failures": [
      {"broker": 5, "type": "DEAD"},
      {"broker": 12, "type": "DEAD"},
      {"broker": 23, "type": "DEAD"}
    ]
  },
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal"],
  "analysis": ["rebalancing", "data-loss-risk"]
}

Response:
{
  "scenarioName": "3 Broker Failure",
  "impact": {
    "affectedPartitions": 3500,
    "underReplicatedPartitions": 0,
    "dataAtRisk": "0 bytes",
    "assessment": "SAFE - No data loss"
  },
  "rebalanceRequired": {
    "partitionsToMove": 3500,
    "estimatedDuration": "45 minutes",
    "dataToPigMove": "350 GB",
    "cpuImpact": "+15% on remaining brokers"
  },
  "remainingCapacity": {
    "cpu": {
      "max": 82.3,
      "average": 68.5,
      "assessment": "ADEQUATE"
    },
    "disk": {
      "max": 91.7,
      "average": 78.2,
      "assessment": "MARGINAL - Monitor closely"
    }
  },
  "recommendations": [
    "Cluster can survive 3 broker failure",
    "Consider increasing replication factor for critical topics",
    "Add 5 brokers for better headroom"
  ]
}
```

### Use Case 3: Goal Comparison

**Scenario:** Choosing between goal configurations

```
POST /simulate/compare

{
  "baseCluster": "production",
  "scenarios": [
    {
      "name": "Current Goals",
      "goals": ["RackAwareGoal", "ReplicaCapacityGoal", "DiskCapacityGoal"]
    },
    {
      "name": "Add Leader Distribution",
      "goals": ["RackAwareGoal", "ReplicaCapacityGoal", "DiskCapacityGoal", "LeaderBytesInDistributionGoal"]
    },
    {
      "name": "Custom Balance",
      "goals": ["RackAwareGoal", "CustomBalanceGoal"]
    }
  ]
}

Response:
{
  "comparison": [
    {
      "scenarioName": "Current Goals",
      "balance": {
        "cpuVariance": 12.5,
        "diskVariance": 8.3,
        "leaderVariance": 45.2  # High variance!
      },
      "moveCount": 0,
      "score": 7.2
    },
    {
      "scenarioName": "Add Leader Distribution",
      "balance": {
        "cpuVariance": 11.2,
        "diskVariance": 8.1,
        "leaderVariance": 15.3  # Much better!
      },
      "moveCount": 450,
      "score": 8.7
    },
    {
      "scenarioName": "Custom Balance",
      "balance": {
        "cpuVariance": 9.8,
        "diskVariance": 7.5,
        "leaderVariance": 22.1
      },
      "moveCount": 680,
      "score": 8.3
    }
  ],
  "recommendation": "Add Leader Distribution - Best balance with reasonable move count"
}
```

---

## Proposed Solution

### Architecture: Simulation Engine

```
┌──────────────────────────────────────────────────┐
│            Simulation Engine                      │
│                                                   │
│  ┌────────────────────────────────────────────┐ │
│  │     Scenario Builder                        │ │
│  │  - Clone current state                      │ │
│  │  - Apply modifications                      │ │
│  │  - Inject failures                          │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     Simulated ClusterModel                  │ │
│  │  - In-memory cluster state                  │ │
│  │  - Sandboxed (no real cluster impact)      │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     Optimizer (Existing)                    │ │
│  │  - Run goals on simulated cluster          │ │
│  │  - Generate proposals                       │ │
│  └─────────────┬──────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     Analysis Engine                         │ │
│  │  - Capacity analysis                        │ │
│  │  - Impact assessment                        │ │
│  │  - Recommendations                          │ │
│  └────────────────────────────────────────────┘ │
│                │                                  │
│                ▼                                  │
│           Simulation Report                       │
└──────────────────────────────────────────────────┘
```

### Key Components

#### 1. Scenario Builder

**Purpose:** Create modified cluster models for simulation

```java
/**
 * Builds simulated cluster models based on scenarios.
 */
public class ScenarioBuilder {
    /**
     * Creates a simulated cluster model from a scenario definition.
     *
     * @param baseCluster the real cluster to base simulation on
     * @param scenario the scenario modifications to apply
     * @return simulated cluster model
     */
    public ClusterModel buildScenario(String baseCluster, Scenario scenario) {
        // 1. Clone current cluster state
        ClusterModel simulatedModel = cloneClusterModel(baseCluster);

        // 2. Apply modifications
        applyModifications(simulatedModel, scenario.modifications());

        // 3. Inject failures
        applyFailures(simulatedModel, scenario.failures());

        // 4. Adjust load
        applyLoadMultipliers(simulatedModel, scenario.loadMultipliers());

        return simulatedModel;
    }

    private ClusterModel cloneClusterModel(String clusterId) {
        // Deep copy of current cluster state
        ClusterModel current = _coordinator.getContext(clusterId).clusterModel();
        return current.deepCopy();
    }

    private void applyModifications(ClusterModel model, Modifications mods) {
        // Add brokers
        for (BrokerSpec broker : mods.additionalBrokers()) {
            model.addBroker(broker.id(), broker.rack(), broker.capacity());
        }

        // Remove brokers
        for (int brokerId : mods.removedBrokers()) {
            model.removeBroker(brokerId);
        }

        // Add partitions
        for (PartitionSpec partition : mods.additionalPartitions()) {
            model.addPartition(partition.topic(), partition.replicas());
        }
    }

    private void applyFailures(ClusterModel model, List<Failure> failures) {
        for (Failure failure : failures) {
            switch (failure.type()) {
                case BROKER_DEAD:
                    model.markBrokerDead(failure.brokerId());
                    break;
                case DISK_FULL:
                    model.setBrokerDiskUsage(failure.brokerId(), 100.0);
                    break;
                case SLOW_BROKER:
                    model.setBrokerLatency(failure.brokerId(), failure.latency());
                    break;
                case RACK_FAILURE:
                    model.markRackFailed(failure.rack());
                    break;
            }
        }
    }
}
```

#### 2. Simulation Executor

**Purpose:** Run optimization on simulated cluster

```java
/**
 * Executes simulations and generates analysis.
 */
public class SimulationExecutor {
    private final ScenarioBuilder _scenarioBuilder;
    private final GoalOptimizer _optimizer;
    private final AnalysisEngine _analysisEngine;

    /**
     * Executes a simulation scenario.
     *
     * @param request the simulation request
     * @return simulation report
     */
    public SimulationReport execute(SimulationRequest request) {
        // 1. Build simulated cluster
        ClusterModel simulated = _scenarioBuilder.buildScenario(
            request.baseCluster(),
            request.scenario()
        );

        // 2. Run optimization
        OptimizationResult optimization = _optimizer.optimize(
            simulated,
            request.goals(),
            new OptimizationOptions()
        );

        // 3. Analyze results
        CapacityAnalysis capacity = _analysisEngine.analyzeCapacity(simulated);
        ImpactAssessment impact = _analysisEngine.assessImpact(
            simulated,
            optimization
        );

        // 4. Generate recommendations
        List<Recommendation> recommendations =
            _analysisEngine.generateRecommendations(
                simulated,
                capacity,
                impact
            );

        // 5. Create report
        return new SimulationReport(
            request.scenario().name(),
            simulated.stats(),
            optimization,
            capacity,
            impact,
            recommendations
        );
    }
}
```

#### 3. Analysis Engine

**Purpose:** Analyze simulation results and generate insights

```java
/**
 * Analyzes simulation results and generates recommendations.
 */
public class AnalysisEngine {
    /**
     * Analyzes cluster capacity after scenario modifications.
     */
    public CapacityAnalysis analyzeCapacity(ClusterModel model) {
        Map<Resource, ResourceUtilization> utilization = new HashMap<>();

        for (Resource resource : Resource.cachedValues()) {
            double max = model.maxUtilization(resource);
            double avg = model.averageUtilization(resource);
            double p95 = model.percentileUtilization(resource, 95.0);

            // Calculate headroom
            double headroom = 100.0 - max;

            // Assess health
            Assessment assessment;
            if (headroom > 30) {
                assessment = Assessment.HEALTHY;
            } else if (headroom > 15) {
                assessment = Assessment.ADEQUATE;
            } else if (headroom > 5) {
                assessment = Assessment.MARGINAL;
            } else {
                assessment = Assessment.INSUFFICIENT;
            }

            utilization.put(resource, new ResourceUtilization(
                resource,
                max,
                avg,
                p95,
                headroom,
                assessment
            ));
        }

        return new CapacityAnalysis(utilization);
    }

    /**
     * Assesses impact of proposed changes.
     */
    public ImpactAssessment assessImpact(ClusterModel model,
                                        OptimizationResult optimization) {
        return new ImpactAssessment(
            optimization.numPartitionMovements(),
            optimization.estimatedDataToMove(),
            optimization.estimatedDuration(),
            calculateCPUImpact(model, optimization),
            calculateNetworkImpact(model, optimization),
            identifyRisks(model, optimization)
        );
    }

    /**
     * Generates actionable recommendations.
     */
    public List<Recommendation> generateRecommendations(
            ClusterModel model,
            CapacityAnalysis capacity,
            ImpactAssessment impact) {

        List<Recommendation> recommendations = new ArrayList<>();

        // Check CPU capacity
        if (capacity.cpu().assessment() == Assessment.INSUFFICIENT) {
            recommendations.add(new Recommendation(
                Severity.CRITICAL,
                "CPU capacity insufficient",
                String.format("Max CPU: %.1f%%. Add %d brokers",
                    capacity.cpu().max(),
                    calculateBrokersNeeded(model, Resource.CPU)
                )
            ));
        }

        // Check disk capacity
        if (capacity.disk().max() > 90) {
            recommendations.add(new Recommendation(
                Severity.HIGH,
                "Disk capacity critical",
                "Consider increasing disk size or adding brokers"
            ));
        }

        // Check rebalance impact
        if (impact.partitionMovements() > 10000) {
            recommendations.add(new Recommendation(
                Severity.MEDIUM,
                "Large rebalance required",
                String.format("%d partitions to move. Schedule during low traffic",
                    impact.partitionMovements())
            ));
        }

        return recommendations;
    }
}
```

---

## REST API

### Endpoint: POST /simulate

**Execute a single scenario simulation:**

```
POST /simulate

Request:
{
  "scenario": {
    "name": "Add 10 Brokers",
    "baseCluster": "production",
    "modifications": {
      "additionalBrokers": [
        {"id": 51, "rack": "rack-1", "capacity": {...}},
        ...
      ]
    }
  },
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal"],
  "analysis": ["capacity", "rebalancing", "cost"]
}

Response:
{
  "scenarioName": "Add 10 Brokers",
  "simulatedCluster": {
    "brokers": 60,
    "partitions": 10000,
    "totalDataGB": 5000
  },
  "capacityAnalysis": {
    "cpu": {"max": 65.2, "avg": 52.1, "assessment": "HEALTHY"},
    "disk": {"max": 72.5, "avg": 58.3, "assessment": "HEALTHY"}
  },
  "rebalanceAnalysis": {
    "partitionsToMove": 2000,
    "dataToMoveGB": 200,
    "estimatedDurationMinutes": 30
  },
  "costAnalysis": {
    "currentMonthlyCost": 25000,
    "projectedMonthlyCost": 30000,
    "additionalCost": 5000,
    "costPerPartition": 3.00
  },
  "recommendations": [
    "Cluster can handle projected load with 10 additional brokers",
    "Consider using fewer, larger instances to reduce cost"
  ]
}
```

### Endpoint: POST /simulate/compare

**Compare multiple scenarios side-by-side:**

```
POST /simulate/compare

Request:
{
  "baseCluster": "production",
  "scenarios": [
    {
      "name": "Add 5 Brokers",
      "modifications": {"additionalBrokers": [...]}  // 5 brokers
    },
    {
      "name": "Add 10 Brokers",
      "modifications": {"additionalBrokers": [...]}  // 10 brokers
    },
    {
      "name": "Add 20 Brokers",
      "modifications": {"additionalBrokers": [...]}  // 20 brokers
    }
  ],
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal"]
}

Response:
{
  "scenarios": [
    {
      "name": "Add 5 Brokers",
      "score": 6.5,
      "capacity": {"cpu": {"max": 85.3, "assessment": "ADEQUATE"}},
      "cost": {"monthly": 27500},
      "recommendation": "Marginal capacity - risky for growth"
    },
    {
      "name": "Add 10 Brokers",
      "score": 8.7,
      "capacity": {"cpu": {"max": 72.1, "assessment": "HEALTHY"}},
      "cost": {"monthly": 30000},
      "recommendation": "⭐ RECOMMENDED - Good balance of capacity and cost"
    },
    {
      "name": "Add 20 Brokers",
      "score": 7.2,
      "capacity": {"cpu": {"max": 55.3, "assessment": "HEALTHY"}},
      "cost": {"monthly": 35000},
      "recommendation": "Over-provisioned - waste of resources"
    }
  ],
  "bestOption": "Add 10 Brokers"
}
```

### Endpoint: POST /simulate/failure

**Simulate failure scenarios:**

```
POST /simulate/failure

Request:
{
  "baseCluster": "production",
  "failures": [
    {"type": "BROKER_DEAD", "broker": 5},
    {"type": "BROKER_DEAD", "broker": 12},
    {"type": "RACK_FAILURE", "rack": "rack-3"}
  ],
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal"]
}

Response:
{
  "scenarioName": "3 Broker + Rack Failure",
  "impact": {
    "brokersLost": 3,
    "racksFailed": 1,
    "affectedPartitions": 5500,
    "underReplicatedPartitions": 0,
    "dataAtRisk": "0 bytes",
    "assessment": "SAFE"
  },
  "survivability": {
    "clusterOperational": true,
    "additionalFailuresTolerance": 2,
    "remainingCapacity": {"cpu": 82.5, "disk": 88.3}
  },
  "recovery": {
    "autoRecoverable": true,
    "rebalanceRequired": true,
    "estimatedRecoveryTime": "45 minutes"
  },
  "recommendations": [
    "Cluster can survive this failure scenario",
    "Consider adding 5 brokers for better failure tolerance"
  ]
}
```

---

## UI Integration

### Simulation Dashboard

```
╔══════════════════════════════════════════════════════════════╗
║              Simulation & What-If Analysis                   ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  Base Cluster: [Production ▼]                                ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐║
║  │ Scenario Builder                                        │║
║  │                                                          │║
║  │ Scenario Name: [Black Friday 2025____________]          │║
║  │                                                          │║
║  │ Modifications:                                           │║
║  │ ☑ Add Brokers:        [10] brokers                      │║
║  │ ☐ Remove Brokers:     [  ] brokers                      │║
║  │ ☑ Increase Load:      [3.0]× current load               │║
║  │ ☑ Add Partitions:     [40000] new partitions            │║
║  │ ☐ Simulate Failure:   [ ] broker(s)                     │║
║  │                                                          │║
║  │ Goals: [☑] RackAware  [☑] ReplicaCapacity  [☑] Disk    │║
║  │                                                          │║
║  │ Analysis Options:                                        │║
║  │ ☑ Capacity Analysis                                      │║
║  │ ☑ Rebalance Impact                                       │║
║  │ ☑ Cost Projection                                        │║
║  │ ☑ Recommendations                                        │║
║  │                                                          │║
║  │ [Run Simulation]  [Compare Scenarios]  [Save Scenario]  │║
║  └──────────────────────────────────────────────────────────┘║
║                                                               ║
║  Recent Simulations:                                          ║
║  • Black Friday 2025         11/20 14:30  [View Results]    ║
║  • 3 Broker Failure Test     11/19 10:15  [View Results]    ║
║  • Add 20 Brokers Scenario   11/18 16:45  [View Results]    ║
╚══════════════════════════════════════════════════════════════╝
```

### Simulation Results View

```
╔══════════════════════════════════════════════════════════════╗
║           Simulation Results: Black Friday 2025              ║
╠══════════════════════════════════════════════════════════════╣
║                                                               ║
║  ✓ Simulation completed in 2.3 seconds                       ║
║                                                               ║
║  Cluster Configuration:                                       ║
║  • Brokers: 50 → 60 (+10)                                    ║
║  • Partitions: 10,000 → 50,000 (+40,000)                    ║
║  • Load: 1TB/hour → 3TB/hour (×3)                            ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ 📊 Capacity Analysis                                   │ ║
║  │                                                         │ ║
║  │ CPU:        [████████████████░░░░] 78.5% max           │ ║
║  │            Assessment: ✓ ADEQUATE                      │ ║
║  │            Headroom: 21.5%                             │ ║
║  │                                                         │ ║
║  │ Disk:       [██████████████████░░] 89.3% max           │ ║
║  │            Assessment: ⚠ MARGINAL                      │ ║
║  │            Headroom: 10.7%                             │ ║
║  │                                                         │ ║
║  │ Network:    [████████████░░░░░░░░] 65.2% max           │ ║
║  │            Assessment: ✓ HEALTHY                       │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ 🔄 Rebalance Impact                                    │ ║
║  │                                                         │ ║
║  │ Partitions to move:    2,500                           │ ║
║  │ Data to move:          250 GB                          │ ║
║  │ Estimated duration:    45 minutes                      │ ║
║  │ CPU impact:            +12% during rebalance           │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌────────────────────────────────────────────────────────┐ ║
║  │ 💡 Recommendations                                      │ ║
║  │                                                         │ ║
║  │ ⚠️  Add 15 brokers instead of 10 for safer disk        │ ║
║  │     headroom (recommendation: 20% minimum)             │ ║
║  │                                                         │ ║
║  │ ✓  Cluster can handle 3× load with added brokers       │ ║
║  │                                                         │ ║
║  │ 💡 Consider increasing disk capacity per broker to     │ ║
║  │    avoid adding more brokers                           │ ║
║  └────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  [Export Report]  [Save Scenario]  [Execute in Production]  ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Advanced Features

### 1. Time-Series Projection

**Simulate future growth over time:**

```
POST /simulate/projection

Request:
{
  "baseCluster": "production",
  "projections": [
    {"months": 3, "loadGrowth": 1.2},   # +20% in 3 months
    {"months": 6, "loadGrowth": 1.5},   # +50% in 6 months
    {"months": 12, "loadGrowth": 2.0}   # +100% in 12 months
  ],
  "capacityTarget": {
    "cpuHeadroom": 30.0,  # Maintain 30% headroom
    "diskHeadroom": 30.0
  }
}

Response:
{
  "projections": [
    {
      "timeframe": "3 months",
      "load": 1.2,
      "capacityNeeded": {"brokers": 55, "action": "Add 5 brokers"},
      "costImpact": 2500
    },
    {
      "timeframe": "6 months",
      "load": 1.5,
      "capacityNeeded": {"brokers": 65, "action": "Add 15 brokers"},
      "costImpact": 7500
    },
    {
      "timeframe": "12 months",
      "load": 2.0,
      "capacityNeeded": {"brokers": 80, "action": "Add 30 brokers"},
      "costImpact": 15000
    }
  ],
  "recommendations": [
    "Plan hardware procurement now for 12-month timeline",
    "Consider larger instance types to reduce broker count"
  ]
}
```

### 2. Monte Carlo Simulation

**Run probabilistic simulations:**

```
POST /simulate/monte-carlo

Request:
{
  "baseCluster": "production",
  "variables": {
    "brokerFailureRate": {"distribution": "poisson", "lambda": 0.01},
    "loadVariance": {"distribution": "normal", "mean": 1.0, "stddev": 0.2}
  },
  "iterations": 1000,
  "goals": ["RackAwareGoal", "ReplicaCapacityGoal"]
}

Response:
{
  "iterations": 1000,
  "results": {
    "clusterHealthy": {
      "probability": 0.95,
      "description": "95% of simulations had healthy cluster"
    },
    "capacityExceeded": {
      "probability": 0.03,
      "description": "3% of simulations exceeded capacity"
    },
    "dataLoss": {
      "probability": 0.001,
      "description": "0.1% of simulations had data loss risk"
    }
  },
  "recommendations": [
    "Cluster is resilient to typical failure patterns",
    "Consider adding 3 brokers to reduce capacity exceeded probability to <1%"
  ]
}
```

### 3. Saved Scenarios Library

**Save and reuse common scenarios:**

```
GET /simulate/scenarios

Response:
{
  "scenarios": [
    {
      "id": "scenario-123",
      "name": "Black Friday",
      "description": "3× load, 5× partitions",
      "lastRun": "2025-11-20T14:30:45Z",
      "tags": ["capacity-planning", "seasonal"]
    },
    {
      "id": "scenario-456",
      "name": "DR Test",
      "description": "Entire us-east-1a failure",
      "lastRun": "2025-11-15T10:00:00Z",
      "tags": ["disaster-recovery", "testing"]
    }
  ]
}
```

---

## Implementation Plan

### Phase 1: Core Simulation Engine (Week 1-2)

**Tasks:**
1. Create `ScenarioBuilder` class
2. Implement cluster model cloning
3. Add modification/failure injection
4. Create `SimulationExecutor`
5. Write unit tests

**Deliverables:**
- Working simulation engine
- Can create and run scenarios

### Phase 2: Analysis Engine (Week 3-4)

**Tasks:**
1. Implement `CapacityAnalysis`
2. Implement `ImpactAssessment`
3. Create recommendation engine
4. Add cost analysis
5. Integration tests

**Deliverables:**
- Comprehensive analysis of simulation results
- Actionable recommendations

### Phase 3: REST API & UI (Week 5-6)

**Tasks:**
1. Create `/simulate` endpoints
2. Add scenario comparison API
3. Build simulation UI dashboard
4. Add results visualization
5. API documentation

**Deliverables:**
- Complete simulation API
- User-friendly UI

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Capacity planning accuracy | ±10% of simulation |
| Production incidents avoided | 50% reduction |
| Time to capacity decision | <1 hour (from days) |
| Simulation performance | <5 seconds |
| User adoption | 80% of operators |

---

## Effort Estimate

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: Core engine | 2 weeks | 10 days |
| Phase 2: Analysis | 2 weeks | 10 days |
| Phase 3: API & UI | 2 weeks | 10 days |
| **Total** | **6 weeks** | **30 days** |

---

## References

- [Chaos Engineering Principles](https://principlesofchaos.org/)
- [Netflix Chaos Monkey](https://netflix.github.io/chaosmonkey/)
- [Kubernetes Cluster Capacity Planning](https://kubernetes.io/docs/tasks/administer-cluster/cluster-management/)

---

**Priority:** P1 (High impact for capacity planning)
**Effort:** 20-30 dev-days
**Dependencies:** None
**Breaking Changes:** None (additive feature)
