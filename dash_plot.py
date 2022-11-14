from collections import defaultdict
from typing import Dict
from pymemcache.client import base
from pymemcache import serde
from datetime import datetime

# dash imports
from dash.dependencies import Input, Output
from dash import Dash, html, dcc
import pickle

# kafka-consumer
from kafka import KafkaConsumer
from market_data import MarketData


# dash components
app = Dash(__name__, update_title=None)
app.layout = html.Div([
    html.H1("Testing live features"),
    dcc.Graph(id='graph', figure=dict(data=[{'x': [], 'y': []}])),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])
data: Dict[str, int] = defaultdict(int)

consumer = KafkaConsumer('my_new_topic', key_deserializer=lambda key: key.decode('utf-8'),
                         value_deserializer=pickle.loads)
client = base.Client(('localhost', 11211), serde=serde.pickle_serde)


@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    print(f'handle_stream called at {datetime.now()}')
    # msg: MarketData = consumer.next_v2().value  # blocks until next message is received
    value: int = int(client.get('SBI'))
    print(f'Received value: {value}')
    print(f'Stats: {client.stats()}')

    return {
        "data": [
            {
                "x": ["SBI"],
                "y": [value] if value else 0,
                "type": "bar"
            },
        ],
        "layout": {"title": "Quantity traded for symbols"}
    }


if __name__ == '__main__':
    app.run_server(dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
