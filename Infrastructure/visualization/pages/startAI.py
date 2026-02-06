import dash
from dash import html, dcc, Input, Output, State
from kafka import KafkaProducer
import json
import time

dash.register_page(
    __name__,
    path="/start-ai",
    name="Start AI",
    title="Start AI",
)

# Initialize Kafka producer with retry
def create_producer():
    for attempt in range(10):  # Retry up to 10 times
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: str(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka (attempt {attempt+1}/10): {e}")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after 10 attempts")

producer = create_producer()

layout = html.Div([
    html.H1('Start AI Forecasting', style={'textAlign': 'center', 'marginBottom': '30px'}),
    
    html.Div([
        html.H3('Select Number of Wildfire Events to Forecast', style={'marginBottom': '20px'}),
        
        html.Div([
            html.Label('Forecast Count:', style={'fontWeight': 'bold', 'marginRight': '10px'}),
            dcc.Slider(
                id='forecast-slider',
                min=10,
                max=200,
                step=10,
                value=50,
                marks={i: str(i) for i in range(10, 201, 20)},
                tooltip={"placement": "bottom", "always_visible": True}
            ),
        ], style={'marginBottom': '30px'}),
        
        html.Div([
            html.Button(
                'Start AI Forecasting',
                id='start-ai-button',
                n_clicks=0,
                style={
                    'backgroundColor': '#4CAF50',
                    'color': 'white',
                    'border': 'none',
                    'padding': '15px 32px',
                    'textAlign': 'center',
                    'textDecoration': 'none',
                    'display': 'inline-block',
                    'fontSize': '16px',
                    'margin': '4px 2px',
                    'cursor': 'pointer',
                    'borderRadius': '8px'
                }
            )
        ], style={'textAlign': 'center'}),
        
        html.Div(id='ai-status', style={'marginTop': '20px', 'textAlign': 'center'})
    ], style={'maxWidth': '600px', 'margin': '0 auto', 'padding': '20px'})
])

@dash.callback(
    Output('ai-status', 'children'),
    Input('start-ai-button', 'n_clicks'),
    State('forecast-slider', 'value')
)
def start_ai_forecasting(n_clicks, forecast_count):
    if n_clicks > 0:
        try:
            print(f"🎯 Button clicked! Sending {forecast_count} to 'ai' topic...")
            
            # Send the forecast count to the 'ai' topic
            future = producer.send('ai', forecast_count)
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Message sent successfully!")
            print(f"   Topic: {record_metadata.topic}")
            print(f"   Partition: {record_metadata.partition}")
            print(f"   Offset: {record_metadata.offset}")
            
            producer.flush()
            
            return html.Div([
                html.P(f"✅ Successfully sent forecast request for {forecast_count} wildfire events!", 
                      style={'color': 'green', 'fontWeight': 'bold'}),
                html.P(f"Message sent to partition {record_metadata.partition}, offset {record_metadata.offset}", 
                      style={'color': '#666', 'fontSize': '12px'}),
                html.P("The AI model will process this request and generate predictions.", 
                      style={'color': '#666'})
            ])
            
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            import traceback
            traceback.print_exc()
            return html.Div([
                html.P(f"❌ Error sending forecast request: {str(e)}", 
                      style={'color': 'red', 'fontWeight': 'bold'})
            ])
    
    return ""