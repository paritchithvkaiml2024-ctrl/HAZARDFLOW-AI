// HazardFlow AI - Real-Time Dashboard Client Controller

const zonesContainer = document.getElementById('zones-container');
const eventStream = document.getElementById('event-stream');
const statExpress = document.getElementById('stat-express');
const statHigh = document.getElementById('stat-high');
const statBatch = document.getElementById('stat-batch');
const totalCounter = document.getElementById('total-evts-counter');
const decisionText = document.getElementById('decision-text');
const modelLatency = document.getElementById('model-latency');

const z4Temp = document.getElementById('z4-temp');
const z4Ppe = document.getElementById('z4-ppe');
const z4Mach = document.getElementById('z4-mach');

let localZones = {};
let ws;

function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${protocol}//${window.location.host}`);

  ws.onopen = () => {
    console.log('⚡ HazardFlow Live Safety Twin WebSocket Connected');
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleIncomingMessage(msg);
    } catch (e) {
      console.error('WS Parse Error', e);
    }
  };

  ws.onclose = () => {
    console.warn('WS Disconnected. Reconnecting in 2s...');
    setTimeout(connectWebSocket, 2000);
  };
}

function handleIncomingMessage(msg) {
  if (msg.type === 'INIT_STATE') {
    localZones = msg.payload.zones;
    renderAllZones();
    updateStats(msg.payload.stats);
  } else if (msg.type === 'SAFETY_EVENT') {
    const data = msg.payload;
    // Update zone state
    if (data.zoneState) {
      localZones[data.zoneId] = data.zoneState;
      updateSingleZoneDOM(data.zoneState);
    }
    
    // Add stream entry
    addStreamEntry(data);
    
    // Update decision panel if high or critical
    if (data.severity === 'CRITICAL' || data.severity === 'HIGH') {
      updateDecision(data);
    }

    if (data.systemStats) {
      updateStats(data.systemStats);
    }
  } else if (msg.type === 'SPARK_PATTERN_DETECTED') {
    triggerSparkAlertAnimation(msg.payload);
  } else if (msg.type === 'FEEDBACK_RECORDED') {
    showNotification(`Feedback loop integrated with MLflow (${msg.payload.feedbackOutcome})`);
  }
}

function renderAllZones() {
  zonesContainer.innerHTML = '';
  Object.values(localZones).forEach(zone => {
    const zoneEl = document.createElement('div');
    zoneEl.className = `zone-item ${getZoneClass(zone.state)}`;
    zoneEl.id = `zone-card-${zone.id}`;
    
    zoneEl.innerHTML = `
      <div class="zone-top-row">
        <span class="zone-name-tag">${zone.id} — ${zone.name}</span>
        <span class="zone-state-badge badge-state-${zone.state.toLowerCase()}">${zone.state}</span>
      </div>
      <div class="zone-metrics-row">
        <span>Risk: <strong class="${zone.workerRisk === 'CRITICAL' ? 'highlight-red' : ''}">${zone.workerRisk}</strong></span>
        <span>Temp: <strong>${zone.temperature.toFixed(1)}°C</strong></span>
        <span>PPE: <strong>${zone.ppeViolations}</strong></span>
        <span>Mach: <strong>${zone.machineAnomalies}</strong></span>
      </div>
    `;
    zonesContainer.appendChild(zoneEl);
  });
}

function updateSingleZoneDOM(zone) {
  const el = document.getElementById(`zone-card-${zone.id}`);
  if (!el) {
    renderAllZones();
    return;
  }
  
  el.className = `zone-item ${getZoneClass(zone.state)}`;
  el.querySelector('.zone-state-badge').className = `zone-state-badge badge-state-${zone.state.toLowerCase()}`;
  el.querySelector('.zone-state-badge').textContent = zone.state;
  
  const metrics = el.querySelector('.zone-metrics-row');
  metrics.innerHTML = `
    <span>Risk: <strong class="${zone.workerRisk === 'CRITICAL' ? 'highlight-red' : ''}">${zone.workerRisk}</strong></span>
    <span>Temp: <strong>${zone.temperature.toFixed(1)}°C</strong></span>
    <span>PPE: <strong>${zone.ppeViolations}</strong></span>
    <span>Mach: <strong>${zone.machineAnomalies}</strong></span>
  `;

  // Update Zone 4 special card if relevant
  if (zone.id === 'ZONE-04') {
    z4Temp.textContent = `${zone.temperature.toFixed(1)}°C ↑`;
    z4Ppe.textContent = `${zone.ppeViolations} Active`;
    z4Mach.textContent = `${zone.machineAnomalies} Detected`;
  }
}

function getZoneClass(state) {
  if (state === 'CRITICAL') return 'is-critical';
  if (state === 'WARNING' || state === 'ELEVATED') return 'is-warning';
  return '';
}

function addStreamEntry(data) {
  const entry = document.createElement('div');
  let entryClass = 'entry-batch';
  if (data.severity === 'CRITICAL') entryClass = 'entry-critical';
  else if (data.severity === 'HIGH') entryClass = 'entry-high';

  entry.className = `stream-entry ${entryClass}`;
  entry.innerHTML = `
    <span><strong>[${data.routingLane}]</strong> ${data.zoneId}: ${data.eventType}</span>
    <span class="mono-num">${data.latencyMs}ms</span>
  `;

  eventStream.insertBefore(entry, eventStream.firstChild);
  
  // Keep only top 20 items in live feed
  while (eventStream.children.length > 20) {
    eventStream.removeChild(eventStream.lastChild);
  }
}

function updateDecision(data) {
  if (data.modelDecision) {
    decisionText.textContent = data.modelDecision.recommendedAction;
    modelLatency.textContent = `${data.latencyMs} ms (Target <80ms)`;
  }
}

function updateStats(stats) {
  if (!stats) return;
  if (stats.expressLane) statExpress.textContent = `${stats.expressLane.toLocaleString()} evts`;
  if (stats.standardLane) statHigh.textContent = `${stats.standardLane.toLocaleString()} evts`;
  if (stats.asyncBatch) statBatch.textContent = `${stats.asyncBatch.toLocaleString()} evts`;
  if (stats.totalEvents) totalCounter.textContent = stats.totalEvents.toLocaleString();
}

function triggerSparkAlertAnimation(payload) {
  const alertEl = document.getElementById('spark-pattern-alert');
  alertEl.style.animation = 'pulse-ring 1s ease';
  setTimeout(() => {
    alertEl.style.animation = '';
  }, 1000);
}

function showNotification(text) {
  console.log('HazardFlow Notification:', text);
}

// User Interaction Buttons
document.getElementById('btn-emergency-drill').addEventListener('click', () => {
  fetch('/api/inject-event', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      zone_id: 'ZONE-04',
      event_type: 'THERMAL_RUNAWAY_EXPLOSION_RISK',
      severity: 'CRITICAL'
    })
  });
});

document.getElementById('btn-burst-test').addEventListener('click', () => {
  for (let i = 0; i < 20; i++) {
    setTimeout(() => {
      fetch('/api/inject-event', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          zone_id: `ZONE-0${(i % 5) + 1}`,
          event_type: 'RAPID_BURST_TELEMETRY',
          severity: i % 4 === 0 ? 'HIGH' : 'LOW'
        })
      });
    }, i * 50);
  }
});

document.querySelectorAll('.inject-chip').forEach(chip => {
  chip.addEventListener('click', () => {
    const zone_id = chip.getAttribute('data-zone');
    const severity = chip.getAttribute('data-sev');
    const event_type = chip.getAttribute('data-type');
    fetch('/api/inject-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zone_id, severity, event_type })
    });
  });
});

document.getElementById('btn-mitigate').addEventListener('click', () => {
  fetch('/api/feedback', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      eventId: 'EVT-MANUAL-ACTION',
      outcome: 'TRUE_POSITIVE_MITIGATED'
    })
  }).then(() => {
    // Soften zone 4 risk
    if (localZones['ZONE-04']) {
      localZones['ZONE-04'].state = 'WARNING';
      localZones['ZONE-04'].workerRisk = 'MEDIUM';
      localZones['ZONE-04'].temperature = 52.0;
      updateSingleZoneDOM(localZones['ZONE-04']);
    }
  });
});

// Start WebSocket connection
connectWebSocket();
