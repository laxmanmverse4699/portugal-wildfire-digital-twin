import dash
from dash import Dash, html, dcc
from flask import jsonify, request
from analytics_backend import get_analytics, get_wildfire_map_analytics


app = Dash(__name__, use_pages=True, update_title='')
app.title = "Wildfire Monitoring System"


def serve_layout():
    return html.Div(
        [
            dash.page_container,
        ]
    )

app.layout = serve_layout

if __name__ == "__main__":
    server = app.server

    @server.route("/api/analytics", methods=["GET"])
    def analytics_api():
        start = request.args.get("start")
        stop = request.args.get("stop")
        fire_type = request.args.get("fireType")
        return jsonify(get_analytics(start=start, stop=stop, fire_type=fire_type))
    
    @server.route("/api/wildfire", methods=["GET"])
    def get_wildfire_data():
        types = request.args.get("types", "Wildfire")
        min_time = int(request.args.get("min_time", 0))
        max_time = int(request.args.get("max_time", 50000))
        min_year = int(request.args.get("min_year", 2013))
        max_year = int(request.args.get("max_year", 2022))
        min_month = int(request.args.get("min_month", 1))
        max_month = int(request.args.get("max_month", 12))
        return get_wildfire_map_analytics(types, min_time, max_time, min_year, max_year, min_month, max_month)

    app.run(debug=True, host="0.0.0.0", port=8050)