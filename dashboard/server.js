const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// In-memory representation of Redis Live Safety Twin
const liveTwin = {
  "ZONE-01": {
    id: "ZONE-01",
    name: "Chemical Processing & Distillation",
    state: "NORMAL",
    workerRisk: "LOW",
    temperature: 42.5,
    vibration: 1.2,
    ppeViolations: 0,
    machineAnomalies: 0,
    personnelCount: 8,
    cumulativeRiskIndex: 14.2,
    threat: "Nominal Operations",
    activeAlerts: []
  },
  "ZONE-02": {
    id: "ZONE-02",
    name: "Robotic Welding Cell Alpha",
    state: "WARNING",
    workerRisk: "MEDIUM",
    temperature: 58.0,
    vibration: 2.8,
    ppeViolations: 1,
    machineAnomalies: 0,
    personnelCount: 4,
    cumulativeRiskIndex: 38.5,
    threat: "Welding Arc Flash Proximity",
    activeAlerts: ["ALT-Z2-01"]
  },
  "ZONE-03": {
    id: "ZONE-03",
    name: "Heavy Press & Stamping Area",
    state: "ELEVATED",
    workerRisk: "HIGH",
    temperature: 48.2,
    vibration: 3.9,
    ppeViolations: 0,
    machineAnomalies: 1,
    personnelCount: 6,
    cumulativeRiskIndex: 62.0,
    threat: "Harmonic Hydraulic Resonance",
    activeAlerts: ["ALT-Z3-04"]
  },
  "ZONE-04": {
    id: "ZONE-04",
    name: "Blast Furnace & High Heat Foundry",
    state: "CRITICAL",
    workerRisk: "CRITICAL",
    temperature: 79.4,
    vibration: 4.8,
    ppeViolations: 3,
    machineAnomalies: 2,
    personnelCount: 12,
    cumulativeRiskIndex: 94.8,
    threat: "Thermal Runaway & PPE Non-Compliance",
    activeAlerts: ["ALT-Z4-09", "ALT-Z4-10"]
  },
  "ZONE-05": {
    id: "ZONE-05",
    name: "Automated Logistics & AGV Fleet",
    state: "NORMAL",
    workerRisk: "LOW",
    temperature: 24.1,
    vibration: 0.8,
    ppeViolations: 0,
    machineAnomalies: 0,
    personnelCount: 3,
    cumulativeRiskIndex: 8.5,
    threat: "Nominal Operations",
    activeAlerts: []
  }
};

// System-wide metric counters
let totalEventsIngested = 1420950;
let expressLaneEvents = 84200;
let standardLaneEvents = 385400;
let asyncBatchEvents = 951350;
let activeFeedbackLoops = 1420;

// Broadcast helper
function broadcast(type, payload) {
  const data = JSON.stringify({ type, payload, timestamp: Date.now() });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

// Generate realistic event stream simulating Tokio priority queues & ONNX inference
function generateEvent(customEvent = null) {
  const zoneIds = ["ZONE-01", "ZONE-02", "ZONE-03", "ZONE-04", "ZONE-05"];
  const randomZone = zoneIds[Math.floor(Math.random() * zoneIds.length)];
  
  const zone = liveTwin[customEvent ? customEvent.zone_id : randomZone];
  
  const severities = ["LOW", "LOW", "LOW", "MEDIUM", "HIGH", "CRITICAL"];
  const severity = customEvent ? customEvent.severity : severities[Math.floor(Math.random() * severities.length)];
  
  const eventTypes = {
    "LOW": ["HEARTBEAT_TELEMETRY", "BATTERY_STATUS", "AMBIENT_LIGHT"],
    "MEDIUM": ["PPE_VEST_MISSING", "MINOR_VIBRATION_DRIFT", "ZONE_BOUNDARY_APPROACH"],
    "HIGH": ["PPE_HARDHAT_VIOLATION", "HYDRAULIC_TEMP_WARNING", "PROXIMITY_ALERT"],
    "CRITICAL": ["GAS_LEAK_SPIKE", "PERSONNEL_FALL_DETECTED", "THERMAL_RUNAWAY", "ARC_FLASH_IMMINENT"]
  };

  const possibleTypes = eventTypes[severity] || ["GENERIC_SAFETY_EVENT"];
  const eventType = customEvent ? customEvent.event_type : possibleTypes[Math.floor(Math.random() * possibleTypes.length)];

  totalEventsIngested++;
  let routingLane = "ASYNC_BATCH";
  let latency = Math.floor(Math.random() * 20) + 8; // ms

  if (severity === "CRITICAL") {
    expressLaneEvents++;
    routingLane = "EXPRESS_CRITICAL";
    latency = Math.floor(Math.random() * 8) + 3; // sub-10ms express
  } else if (severity === "HIGH") {
    standardLaneEvents++;
    routingLane = "HIGH_THROUGHPUT";
    latency = Math.floor(Math.random() * 25) + 12;
  } else if (severity === "MEDIUM") {
    standardLaneEvents++;
    routingLane = "HIGH_THROUGHPUT";
    latency = Math.floor(Math.random() * 30) + 15;
  } else {
    asyncBatchEvents++;
    routingLane = "ASYNC_BATCH";
    latency = Math.floor(Math.random() * 45) + 20;
  }

  // Model traceability metadata
  const modelDecision = {
    modelName: "HazardFlow-DeepSafetyClassifier",
    modelVersion: "v2.4.1-onnx",
    mlflowRunId: "run_hf_98a72cf109e",
    signatureHash: "sha256:4a38f7e29c018b32d0c2e39",
    confidence: severity === "CRITICAL" ? 0.989 : (0.92 + Math.random() * 0.07),
    isAnomaly: severity === "HIGH" || severity === "CRITICAL",
    inferenceLatencyUs: latency * 1000,
    recommendedAction: severity === "CRITICAL" 
      ? "🚨 Initiate immediate acoustic zone lockdown & dispatch response team"
      : severity === "HIGH"
      ? "⚠️ Trigger worker wearable haptic pulse & notify floor lead"
      : "✅ Operational status nominal"
  };

  // Update Live Twin state
  if (severity === "CRITICAL") {
    zone.state = "CRITICAL";
    zone.workerRisk = "CRITICAL";
    zone.cumulativeRiskIndex = Math.min(100, zone.cumulativeRiskIndex + 8.5);
    zone.temperature = Math.max(zone.temperature, 75.0 + Math.random() * 10);
    zone.threat = eventType.replace(/_/g, " ");
  } else if (severity === "HIGH") {
    zone.state = "ELEVATED";
    zone.workerRisk = "HIGH";
    zone.cumulativeRiskIndex = Math.min(100, zone.cumulativeRiskIndex + 4.2);
  } else {
    // Gentle recovery decay
    zone.cumulativeRiskIndex = Math.max(10, zone.cumulativeRiskIndex - 0.5);
    if (zone.cumulativeRiskIndex < 25 && zone.id !== "ZONE-04") {
      zone.state = "NORMAL";
      zone.workerRisk = "LOW";
    }
  }

  const payload = {
    eventId: `EVT-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    zoneId: zone.id,
    zoneName: zone.name,
    eventType,
    severity,
    routingLane,
    latencyMs: latency,
    modelDecision,
    zoneState: zone,
    systemStats: {
      totalEvents: totalEventsIngested,
      expressLane: expressLaneEvents,
      standardLane: standardLaneEvents,
      asyncBatch: asyncBatchEvents,
      p99LatencyMs: 58.7,
      anomalyAccuracy: 98.9
    }
  };

  broadcast('SAFETY_EVENT', payload);
  return payload;
}

// Background event generator loop
setInterval(() => {
  generateEvent();
}, 650);

// Spark temporal pattern detection simulator
setInterval(() => {
  // Check for multi-factor co-occurrence
  const z4 = liveTwin["ZONE-04"];
  if (z4.temperature > 70 && z4.ppeViolations >= 2) {
    broadcast('SPARK_PATTERN_DETECTED', {
      patternId: "PAT-MULTI-FACTOR-09",
      zoneId: "ZONE-04",
      patternName: "Thermal Runaway + Repeated PPE Violation Co-occurrence",
      riskEscalation: "CRITICAL_PREVENTIVE_INTERVENTION",
      windowMinutes: 15,
      correlatedEventsCount: 24,
      recommendation: "Automated interlock engaged on Induction Furnace B3"
    });
  }
}, 8000);

// API Endpoints
app.get('/api/zones', (req, res) => {
  res.json({ zones: Object.values(liveTwin), timestamp: Date.now() });
});

app.post('/api/inject-event', (req, res) => {
  const customEvent = req.body;
  const result = generateEvent(customEvent);
  res.json({ success: true, event: result });
});

app.post('/api/feedback', (req, res) => {
  activeFeedbackLoops++;
  broadcast('FEEDBACK_RECORDED', {
    eventId: req.body.eventId || "EVT-MANUAL-01",
    modelVersion: "v2.4.1-onnx",
    mlflowExperiment: "exp_hf_active_learning_v2",
    feedbackOutcome: req.body.outcome || "MITIGATED",
    totalFeedbackSamples: activeFeedbackLoops + 14200
  });
  res.json({ success: true, activeFeedbackLoops });
});

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'INIT_STATE',
    payload: {
      zones: liveTwin,
      stats: {
        totalEvents: totalEventsIngested,
        expressLane: expressLaneEvents,
        standardLane: standardLaneEvents,
        asyncBatch: asyncBatchEvents,
        p99LatencyMs: 58.7,
        anomalyAccuracy: 98.9
      }
    }
  }));
});

server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`🛡️ HazardFlow AI Realtime Safety Fabric & Dashboard Server`);
  console.log(`🚀 Live UI running on: http://localhost:${PORT}`);
  console.log(`⚡ WebSocket Stream active on ws://localhost:${PORT}`);
  console.log(`=======================================================`);
});
