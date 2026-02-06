import dash
from dash import html, dcc

dash.register_page(__name__, path='/')

layout = html.Div([
    html.Iframe(
        src="/static/html/home.html", 
        style={"width": "100%", "height": "900px", "border": "none"}
    ),
])