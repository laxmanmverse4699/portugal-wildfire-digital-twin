import os
import time

import dash
from dash import callback, dcc, html, Input, Output, State
import dash_leaflet as dl
import polars as pl

from DBFetching import DBFetching


dash.register_page(
    __name__,
    path="/data-analytics",
    name="Data Analytics",
    title="Data Analytics",
)

dataset = pl.DataFrame()

layout = html.Div(
    [
        html.H1("Wildfire Analysis", style={"textAlign": "center"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Select Wildfire Types", style={"fontSize": "24px"}),
                        dcc.Checklist(
                            id="data-type-checklist",
                            options=[
                                {
                                    "label": "Reignition",
                                    "value": "Reignition",
                                },
                                {
                                    "label": "Small Fire",
                                    "value": "Small",
                                },
                                {"label": "Wildfire", "value": "Wildfire"},
                                {
                                    "label": "Agricultural Fire",
                                    "value": "Agricultural",
                                },
                                {
                                    "label": "Controlled Fire",
                                    "value": "Controlled",
                                },
                                {
                                    "label": "AI Generated",
                                    "value": "AI_Generated",
                                },
                            ],
                            labelStyle={"display": "block"},
                            style={"margin-bottom": "20px", "margin-top": "10px"},
                        ),
                        html.Label(
                            "Fire duration in minutes", style={"fontSize": "24px"}
                        ),
                        dcc.RangeSlider(
                            id="time-range-slider",
                            min=0,
                            max=50000,
                            step=10,
                            value=[0, 50000],
                            marks={i: f"{i}m" for i in range(0, 50001, 10000)},
                            tooltip={"placement": "bottom", "always_visible": True},
                        ),
                        html.Div(style={"height": "20px"}),
                        html.Label(
                            "Select time range in years of the start dates of the fires",
                            style={"fontSize": "24px"},
                        ),
                        dcc.RangeSlider(
                            id="year-range-slider",
                            min=2013,
                            max=2022,
                            step=1,
                            value=[2013, 2022],
                            marks={i: str(i) for i in range(2013, 2023)},
                            tooltip={"placement": "bottom", "always_visible": True},
                        ),
                        html.Div(style={"height": "20px"}),
                        html.Label(
                            "Select time range in months of the start dates of the fires",
                            style={"fontSize": "24px"},
                        ),
                        dcc.RangeSlider(
                            id="month-range-slider",
                            min=1,
                            max=12,
                            step=1,
                            value=[1, 12],
                            marks={i: str(i) for i in range(1, 13)},
                            tooltip={"placement": "bottom", "always_visible": True},
                        ),
                        html.Div(style={"height": "20px"}),
                        html.Button("Update Map", id="update-map-button", className="action-button"),
                        html.Div(
                            id="query-status",
                            style={
                                "marginTop": "20px",
                                "fontSize": "14px",
                                "color": "#e0e0e0",
                            },
                        ),
                    ],
                    style={
                        "width": "500px",
                        "verticalAlign": "top",
                        "padding": "20px",
                    },
                ),
                html.Div(
                    [
                        dl.Map(
                            id="map",
                            center={"lat": 39.5, "lng": -8.5},  # type: ignore
                            zoom=7,
                            style={"width": "500px", "height": "900px"},
                            children=[dl.TileLayer(), dl.LayerGroup(id="data-layer")],
                            preferCanvas=True,
                        ),
                        # FWI Legend
                        html.Div(
                            [
                                html.H4(
                                    "FWI Legend",
                                    style={"margin": "0 0 10px 0", "fontSize": "16px", "color": "#1a1a1a", "fontWeight": "bold"},
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
                                            "≥90: Very High", style={"fontSize": "12px", "color": "#1a1a1a", "fontWeight": "600"}
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
                                            "≥80: High", style={"fontSize": "12px", "color": "#1a1a1a", "fontWeight": "600"}
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
                                            "≥70: Moderate", style={"fontSize": "12px", "color": "#1a1a1a", "fontWeight": "600"}
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
                                            "≥60: Low", style={"fontSize": "12px", "color": "#1a1a1a", "fontWeight": "600"}
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            style={
                                                "width": "15px",
                                                "height": "15px",
                                                "backgroundColor": "#00ff00",
                                                "display": "inline-block",
                                                "marginRight": "5px",
                                            }
                                        ),
                                        html.Span(
                                            "<60: Very Low", style={"fontSize": "12px", "color": "#1a1a1a", "fontWeight": "600"}
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
                        "width": "500px",
                        "height": "900px",
                        "padding": "20px",
                        "position": "relative",
                    },
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "center",
                "alignItems": "flex-start",
            },
        ),
    ]
)


def create_fire_marker(fire_data):
    """Create a map marker for a fire with FWI-based coloring"""
    lat = fire_data.get("LATITUDE")
    lng = fire_data.get("LONGITUDE")
    FWI = fire_data.get("FWI", 0)
    vegetation_density = fire_data.get("VEGETATION_DENSITY", "Unknown")

    if lat is None or lng is None:
        return None

    # Color based on FWI values (Fire Weather Index)
    if FWI >= 250:
        color = "#ff0000"  # Red - Very High
    elif FWI >= 100:
        color = "#ff4500"  # Orange Red - High
    elif FWI >= 750:
        color = "#ffa500"  # Orange - Moderate
    elif FWI >= 50:
        color = "#ffff00"  # Yellow - Low
    else:
        color = "#00ff00"  # Green - Very Low

    return dl.CircleMarker(
        center={"lat": lat, "lng": lng},
        radius=8,
        color=color,
        fill=True,
        fillOpacity=0.7,
        weight=2,
        children=[
            dl.Tooltip(f"Vegetation Density: {vegetation_density} | FWI: {FWI}")
        ],
    )


@callback(
    [Output("data-layer", "children"), Output("query-status", "children")],
    Input("update-map-button", "n_clicks"),
    State("data-type-checklist", "value"),
    State("time-range-slider", "value"),
    State("year-range-slider", "value"),
    State("month-range-slider", "value"),
)
def update_map(n_clicks, selected_types, time_range, year_range, month_range):
    global dataset

    if not n_clicks:
        return [], "Click 'Update Map' to load data"

    if db_fetching is None:
        return [], "Database connection not available"

    query = db_fetching.query_builder(
        selected_types=selected_types,
        time_range=time_range,
        year_range=year_range,
        month_range=month_range,
        dataset=dataset,
    )

    try:
        # Execute query
        dataset = db_fetching.query_data(query)

        if dataset is None or dataset.height == 0:
            return [], f"No fires found matching the criteria"

        # Create markers for each fire
        markers = []
        for row in dataset.iter_rows(named=True):
            marker = create_fire_marker(row)
            if marker is not None:
                markers.append(marker)

        status_message = f"Found {len(markers)} fires matching criteria"
        if selected_types:
            status_message += f" | Types: {', '.join(selected_types)}"
        status_message += f" | Duration: {time_range[0]}-{time_range[1]} min | Years: {year_range[0]}-{year_range[1]} | Months: {month_range[0]}-{month_range[1]}"

        return markers, status_message

    except Exception as e:
        error_msg = f"Query error: {str(e)}"
        print(error_msg)
        print(f"Query: {query}")
        return [], error_msg


token_path = "/shared/influxdb.token"
for _ in range(120):  # up to 120 seconds
    if os.path.exists(token_path):
        break
    print(f"Waiting for influxdb token file: {token_path}")
    time.sleep(1)

if os.path.exists(token_path):
    with open(token_path, "r", encoding="utf-8") as token_file:
        influx_token = token_file.read().strip()
    db_fetching = DBFetching(
        token=influx_token,
    )
else:
    db_fetching = None
