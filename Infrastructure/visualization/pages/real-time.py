import dash
from dash import dcc, html, callback, Input, Output
import os
import dash_leaflet as dl
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timezone
import threading
import time

# Read TIME_SPEED from environment (default to 1000 if not set)
TIME_SPEED = int(os.environ.get("TIME_SPEED", 1000))

# Update interval in milliseconds (update the map every 100ms for smooth visualization)
UPDATE_INTERVAL_MS = 100

# Column names from consumer.go
COLUMNS = [
    "WILDFIRE_TYPE",
    "BURNED_POPULATIONAL_AREA",
    "BURNED_BRUSHLAND_AREA",
    "BURNED_AGRICULTURAL_AREA",
    "BURNED_TOTAL_AREA",
    "HECTARES_PER_HOUR",
    "LATITUDE",
    "LONGITUDE",
    "TEMPERATURE_CELSIUS",
    "RELATIVE_HUMIDITY_PERCENT",
    "WIND_SPEED_MS",
    "PRECIPITATION_MM",
    "FWI",
    "MEAN_ALTITUDE_M",
    "MEAN_SLOPE_DEG",
    "VEGETATION_DENSITY",
    "VEGETATION_VARIETY_INDEX",
    "ALERT_TIMESTAMP",
    "FIRST_INTERVENTION_TIMESTAMP",
    "EXTINCTION_TIMESTAMP",
]


# Global state for real-time monitoring
class WildfireMonitor:
    def __init__(self):
        self.active_fires = {}  # dict: fire_id -> fire_data
        self.simulation_time = None  # Current simulation clock
        self.last_message_time = None  # Timestamp of last received message
        self.last_message_received_at = None  # Real-time when last message was received
        self.consumer_thread = None
        self.is_running = False
        self.lock = threading.Lock()

    def parse_kafka_message(self, message_value):
        try:
            fields = message_value.decode("utf-8").strip().split(",")
            if len(fields) != len(COLUMNS):
                return None

            fire_data = {}
            for i, col_name in enumerate(COLUMNS):
                value = fields[i]
                if value == "":
                    continue

                if col_name in [
                    "LATITUDE",
                    "LONGITUDE",
                    "HECTARES_PER_HOUR",
                ]:
                    try:
                        fire_data[col_name] = float(value)
                    except ValueError:
                        continue
                elif col_name in ["ALERT_TIMESTAMP", "EXTINCTION_TIMESTAMP"]:
                    try:
                        fire_data[col_name] = float(value)
                    except ValueError:
                        continue
                else:
                    fire_data[col_name] = value

            return fire_data
        except Exception as e:
            print(f"[Real-time Monitor] Error parsing message: {e}")
            return None

    def consume_from_kafka(self):
        try:
            # NO CONSUMER GROUP - just read messages
            consumer = KafkaConsumer(
                bootstrap_servers=["kafka:9092"],
                value_deserializer=lambda m: m,
            )

            # Manually assign partition 0 of wildfires topic

            tp = TopicPartition("wildfires", 0)
            consumer.assign([tp])

            # Seek to end - start reading NEW messages only
            consumer.seek_to_end(tp)

            message_count = 0

            while self.is_running:
                # timeout_ms=10 means check 100 times per second
                # max_records=10000 means grab up to 10000 messages at once
                message_batch = consumer.poll(timeout_ms=10, max_records=10000)

                if not message_batch:
                    continue

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        fire_data = self.parse_kafka_message(message.value)
                        if (
                            fire_data
                            and "LATITUDE" in fire_data
                            and "LONGITUDE" in fire_data
                        ):
                            alert_time = fire_data.get("ALERT_TIMESTAMP")

                            with self.lock:
                                if alert_time:
                                    # Update last message time and when it was received
                                    self.last_message_time = alert_time
                                    self.last_message_received_at = time.time()

                                    # Initialize clock on first fire
                                    if self.simulation_time is None:
                                        self.simulation_time = alert_time
                                    else:
                                        # Synchronize: if fire is "in the future", jump clock forward
                                        # Allow small margin (5 minutes = 300 seconds) for fires slightly in the past
                                        time_diff = alert_time - self.simulation_time
                                        if time_diff > -300:
                                            if time_diff > 0:  # Fire is in future
                                                self.simulation_time = alert_time

                                # Add fire to memory
                                fire_id = f"{alert_time}_{fire_data.get('LATITUDE')}_{fire_data.get('LONGITUDE')}"
                                self.active_fires[fire_id] = fire_data
                                message_count += 1
            consumer.close()
        except Exception as e:
            print(f"[Real-time Monitor] Kafka consumer error: {e}")

    def start_consuming(self):
        if not self.is_running:
            self.is_running = True
            self.consumer_thread = threading.Thread(
                target=self.consume_from_kafka, daemon=True
            )
            self.consumer_thread.start()
        else:
            print("[Real-time Monitor] Consumer already running")

    def stop_consuming(self):
        self.is_running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)

    def advance_clock(self, time_delta_seconds):
        with self.lock:
            if self.simulation_time is not None:
                self.simulation_time += time_delta_seconds

    def get_active_fires_at_current_time(self):
        with self.lock:
            if self.simulation_time is None:
                return []

            active = []
            too_early = 0
            already_extinct = 0

            # Define a "display window" - show fires that were active recently
            # This prevents fires from disappearing when clock jumps forward
            DISPLAY_WINDOW = 3600  # Show fires from last 1 hour

            for fire_id, fire_data in self.active_fires.items():
                alert_time = fire_data.get("ALERT_TIMESTAMP", 0)
                extinction_time = fire_data.get("EXTINCTION_TIMESTAMP")

                # Show fire if it's within the display window
                # Fire is visible if: (alert_time <= clock) AND (clock - alert_time <= DISPLAY_WINDOW OR clock <= extinction_time)
                if alert_time > self.simulation_time:
                    too_early += 1
                elif (
                    extinction_time is not None
                    and self.simulation_time > extinction_time
                ):
                    # Check if fire was active recently (within display window)
                    if self.simulation_time - extinction_time <= DISPLAY_WINDOW:
                        active.append(fire_data)  # Show recently extinct fire
                    else:
                        already_extinct += 1
                else:
                    # Fire is currently active
                    active.append(fire_data)

            return active


# Global monitor instance
wildfire_monitor = WildfireMonitor()

dash.register_page(
    __name__,
    path="/real-time",
    name="Real-time Monitoring",
    title="Real-time Monitoring",
)

layout = html.Div(
    [
        html.H1("Real-time Wildfire Monitoring", style={"textAlign": "center"}),
        # Control buttons
        html.Div(
            [
                html.Button(
                    "Start Monitoring",
                    id="start-button",
                    n_clicks=0,
                    className="action-button",
                ),
                html.Div(
                    id="status-message",
                    style={"marginTop": "10px", "fontSize": "14px", "color": "#e0e0e0"},
                ),
            ],
            style={"textAlign": "center", "marginBottom": "20px"},
        ),
        # Map and clock display
        html.Div(
            [
                # Map
                html.Div(
                    [
                        dl.Map(
                            id="realtime-map",
                            center={"lat": 39.5, "lng": -8.5},  # type: ignore
                            zoom=7,
                            style={"width": "600px", "height": "700px"},
                            children=[
                                dl.TileLayer(),
                                dl.LayerGroup(id="realtime-layer"),
                            ],
                            preferCanvas=True,
                        ),
                        # Hectares per hour legend
                        html.Div(
                            [
                                html.H4(
                                    "Fire Intensity (ha/hr)",
                                    style={
                                        "margin": "0 0 10px 0",
                                        "fontSize": "16px",
                                        "color": "#1a1a1a",
                                        "fontWeight": "bold",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#8B0000",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "≥10: Extreme",
                                            style={
                                                "fontSize": "12px",
                                                "color": "#1a1a1a",
                                                "fontWeight": "600",
                                            },
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#ff0000",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "≥1: Very High",
                                            style={
                                                "fontSize": "12px",
                                                "color": "#1a1a1a",
                                                "fontWeight": "600",
                                            },
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#ff4500",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "≥0.5: High",
                                            style={
                                                "fontSize": "12px",
                                                "color": "#1a1a1a",
                                                "fontWeight": "600",
                                            },
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#ffa500",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "≥0.1: Moderate",
                                            style={
                                                "fontSize": "12px",
                                                "color": "#1a1a1a",
                                                "fontWeight": "600",
                                            },
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#ffff00",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "<0.1: Low",
                                            style={
                                                "fontSize": "12px",
                                                "color": "#1a1a1a",
                                                "fontWeight": "600",
                                            },
                                        ),
                                    ]
                                ),
                            ],
                            style={
                                "position": "absolute",
                                "top": "10px",
                                "right": "10px",
                                "backgroundColor": "rgba(255, 255, 255, 0.9)",
                                "padding": "10px",
                                "borderRadius": "5px",
                                "boxShadow": "0 2px 4px rgba(0,0,0,0.3)",
                                "fontSize": "12px",
                                "zIndex": 1000,
                            },
                        ),
                    ],
                    style={
                        "width": "600px",
                        "height": "700px",
                        "position": "relative",
                        "display": "inline-block",
                        "verticalAlign": "top",
                    },
                ),
                # Clock and stats display
                html.Div(
                    [
                        html.H3(
                            "Simulation Clock",
                            style={"marginBottom": "20px", "color": "#1a1a1a"},
                        ),
                        html.Div(
                            id="simulation-clock",
                            style={
                                "fontSize": "32px",
                                "fontWeight": "bold",
                                "marginBottom": "30px",
                                "color": "#ff6600",
                            },
                        ),
                        html.Hr(style={"borderColor": "#ccc"}),
                        html.H4(
                            "Statistics",
                            style={"marginBottom": "15px", "color": "#1a1a1a"},
                        ),
                        html.Div(
                            id="fire-stats",
                            style={
                                "fontSize": "16px",
                                "lineHeight": "1.8",
                                "color": "#2a2a2a",
                            },
                        ),
                        html.Hr(style={"marginTop": "20px", "borderColor": "#ccc"}),
                        html.P(
                            f"Time Speed: {TIME_SPEED}x faster",
                            style={
                                "fontSize": "14px",
                                "color": "#555",
                                "fontWeight": "500",
                            },
                        ),
                    ],
                    style={
                        "width": "300px",
                        "padding": "30px",
                        "backgroundColor": "#f5f5f5",
                        "borderRadius": "12px",
                        "marginLeft": "20px",
                        "display": "inline-block",
                        "verticalAlign": "top",
                        "boxShadow": "0 2px 16px rgba(0,0,0,0.1)",
                    },
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "center",
                "alignItems": "flex-start",
                "marginTop": "20px",
            },
        ),
        # Interval for updating the map (update every 100ms)
        dcc.Interval(
            id="realtime-interval",
            interval=UPDATE_INTERVAL_MS,
            n_intervals=0,
            disabled=True,  # Start disabled, enable when monitoring starts
        ),
    ]
)


# Helper function to get color based on hectares per hour
def get_hectares_per_hour_color(hectares_per_hour: float) -> str:
    """Return color based on fire intensity (hectares per hour)"""
    if hectares_per_hour >= 10:
        return "#8B0000"  # Dark red - Extreme
    elif hectares_per_hour >= 1:
        return "#ff0000"  # Red - Very High
    elif hectares_per_hour >= 0.5:
        return "#ff4500"  # Orange Red - High
    elif hectares_per_hour >= 0.1:
        return "#ffa500"  # Orange - Moderate
    else:
        return "#ffff00"  # Yellow - Low


# Callback to control monitoring start
@callback(
    [
        Output("realtime-interval", "disabled"),
        Output("status-message", "children"),
        Output("start-button", "disabled"),
    ],
    Input("start-button", "n_clicks"),
    prevent_initial_call=True,
)
def control_monitoring(n_clicks):
    if n_clicks > 0:
        wildfire_monitor.start_consuming()
        return False, "Monitoring started", True
    return True, "", False


# Callback to update the map and simulation clock
@callback(
    [
        Output("realtime-layer", "children"),
        Output("simulation-clock", "children"),
        Output("fire-stats", "children"),
    ],
    Input("realtime-interval", "n_intervals"),
)
def update_map(n_intervals):
    # Advance clock slightly to handle gaps between fires
    # With ultra-aggressive polling, messages mostly drive the clock,
    # but we need some advancement for the display window to work
    if wildfire_monitor.simulation_time is not None:
        import time as time_module

        with wildfire_monitor.lock:
            if wildfire_monitor.last_message_received_at:
                seconds_since_last_message = (
                    time_module.time() - wildfire_monitor.last_message_received_at
                )

                # If messages flowing (< 1 second), let messages drive clock mostly
                if seconds_since_last_message < 1:
                    time_delta_seconds = (UPDATE_INTERVAL_MS / 1000.0) * (
                        TIME_SPEED / 100
                    )  # Super slow: 10s per 100ms
                # If gap between messages (1-5 seconds), advance moderately
                elif seconds_since_last_message < 5:
                    time_delta_seconds = (UPDATE_INTERVAL_MS / 1000.0) * (
                        TIME_SPEED / 20
                    )  # Slow: 50s per 100ms
                # If large gap (5+ seconds), advance faster to catch up
                else:
                    time_delta_seconds = (UPDATE_INTERVAL_MS / 1000.0) * (
                        TIME_SPEED / 10
                    )  # Medium: 100s per 100ms
            else:
                # No messages yet, use slow speed
                time_delta_seconds = (UPDATE_INTERVAL_MS / 1000.0) * (TIME_SPEED / 20)

        wildfire_monitor.advance_clock(time_delta_seconds)

    # Get active fires at current simulation time
    active_fires = wildfire_monitor.get_active_fires_at_current_time()

    # Create markers for active fires
    markers = []
    for fire_data in active_fires:
        hectares_per_hour = fire_data.get("HECTARES_PER_HOUR", 0)
        color = get_hectares_per_hour_color(hectares_per_hour)

        latitude = fire_data.get("LATITUDE")
        longitude = fire_data.get("LONGITUDE")

        if latitude is not None and longitude is not None:
            marker = dl.CircleMarker(
                center={"lat": latitude, "lng": longitude},  # type: ignore
                radius=8,
                color=color,
                fillColor=color,
                fillOpacity=0.6,
                children=[
                    dl.Tooltip(
                        html.Div(
                            [
                                html.P(
                                    f"Type: {fire_data.get('WILDFIRE_TYPE', 'N/A')}",
                                    style={"margin": "2px"},
                                ),
                                html.P(
                                    f"Intensity: {hectares_per_hour:.4f} ha/hr",
                                    style={"margin": "2px"},
                                ),
                                html.P(
                                    f"Alert: {datetime.fromtimestamp(fire_data.get('ALERT_TIMESTAMP', 0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
                                    style={"margin": "2px"},
                                ),
                            ]
                        )
                    )
                ],
            )
            markers.append(marker)

    # Format simulation clock
    sim_time = wildfire_monitor.simulation_time
    if sim_time is not None:
        clock_text = (
            datetime.fromtimestamp(sim_time, tz=timezone.utc).strftime(
                "%d/%m/%Y %H:%M:%S"
            )
            + " UTC"
        )
    else:
        clock_text = "Waiting for first alert..."

    # Calculate statistics
    total_fires = len(active_fires)
    if total_fires > 0:
        intensities = [f.get("HECTARES_PER_HOUR", 0) for f in active_fires]
        avg_intensity = sum(intensities) / total_fires
        max_intensity = max(intensities)
        stats = html.Div(
            [
                html.P(f"Active Fires: {total_fires}", style={"marginBottom": "10px"}),
                html.P(
                    f"Avg Intensity: {avg_intensity:.4f} ha/hr",
                    style={"marginBottom": "10px"},
                ),
                html.P(f"Max Intensity: {max_intensity:.4f} ha/hr"),
            ]
        )
    else:
        stats = html.P("No active fires")

    return markers, clock_text, stats
