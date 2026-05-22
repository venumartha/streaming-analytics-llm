"""
Real-Time API Server
FastAPI with WebSocket support for streaming insights to dashboard
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
from pydantic import BaseModel
from typing import List, Dict, Optional
import json
import asyncio
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Streaming Analytics API",
    description="Real-time telemetry analytics with LLM-powered insights",
    version="1.0.0"
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    kafka_connected: bool


class InsightSummary(BaseModel):
    device_id: str
    severity: str
    summary: str
    timestamp: str


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        dead_connections = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Failed to send to client: {e}")
                dead_connections.append(connection)
        
        # Remove dead connections
        for connection in dead_connections:
            self.active_connections.remove(connection)


manager = ConnectionManager()


# Background task to stream from Kafka
async def stream_insights_from_kafka():
    """Background task that consumes from Kafka and broadcasts to WebSocket clients"""
    logger.info("Starting Kafka consumer for insights stream...")
    
    consumer = KafkaConsumer(
        'telemetry-insights',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='websocket-streamer'
    )
    
    loop = asyncio.get_event_loop()
    
    def consume_messages():
        """Blocking Kafka consumption in thread"""
        for message in consumer:
            insight = message.value
            
            # Create simplified message for WebSocket
            ws_message = {
                'type': 'insight',
                'device_id': insight['device_id'],
                'severity': insight['llm_analysis']['severity'],
                'summary': insight['llm_analysis']['summary'],
                'anomaly_count': insight['anomaly_count'],
                'anomaly_types': insight['anomaly_types'],
                'vitals': insight['vitals_summary'],
                'root_causes': insight['llm_analysis']['root_causes'],
                'actions': insight['llm_analysis']['recommended_actions'],
                'timestamp': insight['window_end']
            }
            
            # Schedule broadcast in event loop
            asyncio.run_coroutine_threadsafe(
                manager.broadcast(ws_message),
                loop
            )
    
    # Run in executor to avoid blocking
    await loop.run_in_executor(None, consume_messages)


@app.on_event("startup")
async def startup_event():
    """Start background Kafka consumer on startup"""
    asyncio.create_task(stream_insights_from_kafka())
    logger.info("API server started")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve simple dashboard HTML"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real-Time Telemetry Dashboard</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background: #1a1a1a;
                color: #fff;
            }
            h1 {
                color: #4CAF50;
            }
            .status {
                padding: 10px;
                margin: 10px 0;
                border-radius: 5px;
                background: #333;
            }
            .insight {
                border: 1px solid #444;
                padding: 15px;
                margin: 10px 0;
                border-radius: 5px;
                background: #2a2a2a;
            }
            .severity-critical { border-left: 5px solid #f44336; }
            .severity-high { border-left: 5px solid #ff9800; }
            .severity-medium { border-left: 5px solid #ffeb3b; }
            .severity-low { border-left: 5px solid #4CAF50; }
            .device-id {
                font-weight: bold;
                color: #2196F3;
            }
            .timestamp {
                color: #888;
                font-size: 0.9em;
            }
            .vitals {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 10px;
                margin: 10px 0;
            }
            .vital-box {
                background: #333;
                padding: 10px;
                border-radius: 5px;
                text-align: center;
            }
            ul {
                margin: 5px 0;
                padding-left: 20px;
            }
        </style>
    </head>
    <body>
        <h1>🏥 Real-Time Telemetry Dashboard</h1>
        <div class="status" id="status">
            <strong>Status:</strong> <span id="connection-status">Connecting...</span>
        </div>
        
        <h2>Recent Insights</h2>
        <div id="insights-container"></div>
        
        <script>
            const ws = new WebSocket('ws://localhost:8000/ws');
            const statusEl = document.getElementById('connection-status');
            const insightsEl = document.getElementById('insights-container');
            
            ws.onopen = () => {
                statusEl.textContent = 'Connected ✅';
                statusEl.style.color = '#4CAF50';
            };
            
            ws.onclose = () => {
                statusEl.textContent = 'Disconnected ❌';
                statusEl.style.color = '#f44336';
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                
                if (data.type === 'insight') {
                    addInsight(data);
                }
            };
            
            function addInsight(insight) {
                const div = document.createElement('div');
                div.className = `insight severity-${insight.severity.toLowerCase()}`;
                
                const vitalsHtml = `
                    <div class="vitals">
                        <div class="vital-box">
                            <div>Temp</div>
                            <div><strong>${insight.vitals.temperature_avg}°F</strong></div>
                        </div>
                        <div class="vital-box">
                            <div>Heart Rate</div>
                            <div><strong>${insight.vitals.heart_rate_avg} bpm</strong></div>
                        </div>
                        <div class="vital-box">
                            <div>BP</div>
                            <div><strong>${insight.vitals.bp_systolic_avg}/${insight.vitals.bp_diastolic_avg}</strong></div>
                        </div>
                        <div class="vital-box">
                            <div>O₂ Sat</div>
                            <div><strong>${insight.vitals.oxygen_avg}%</strong></div>
                        </div>
                    </div>
                `;
                
                div.innerHTML = `
                    <div>
                        <span class="device-id">${insight.device_id}</span>
                        <span class="timestamp">${new Date(insight.timestamp).toLocaleString()}</span>
                    </div>
                    <div style="margin: 10px 0;">
                        <strong>Severity:</strong> <span style="color: ${getSeverityColor(insight.severity)}">${insight.severity}</span>
                    </div>
                    <div style="margin: 10px 0;">
                        <strong>Summary:</strong> ${insight.summary}
                    </div>
                    ${vitalsHtml}
                    <div>
                        <strong>Anomaly Types:</strong> ${insight.anomaly_types.join(', ')}
                        (${insight.anomaly_count} events)
                    </div>
                    <div>
                        <strong>Root Causes:</strong>
                        <ul>
                            ${insight.root_causes.map(c => `<li>${c}</li>`).join('')}
                        </ul>
                    </div>
                    <div>
                        <strong>Recommended Actions:</strong>
                        <ul>
                            ${insight.actions.map(a => `<li>${a}</li>`).join('')}
                        </ul>
                    </div>
                `;
                
                insightsEl.insertBefore(div, insightsEl.firstChild);
                
                // Keep only last 10 insights
                while (insightsEl.children.length > 10) {
                    insightsEl.removeChild(insightsEl.lastChild);
                }
            }
            
            function getSeverityColor(severity) {
                const colors = {
                    'Critical': '#f44336',
                    'High': '#ff9800',
                    'Medium': '#ffeb3b',
                    'Low': '#4CAF50'
                };
                return colors[severity] || '#fff';
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        kafka_connected=True
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time insights"""
    await manager.connect(websocket)
    
    try:
        # Keep connection alive and listen for client messages
        while True:
            # Wait for any client message (ping/pong)
            await websocket.receive_text()
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
