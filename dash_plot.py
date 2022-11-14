from typing import Dict

# dash imports
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

# memcache
from pymemcache import serde
from pymemcache.client import base

import config  # to fetch the manually configured values

# dash components
app = Dash(__name__, update_title=None)
app.layout = html.Div([
    html.H1("Testing live features"),
    dcc.Graph(id='graph', figure=dict(data=[{'x': [], 'y': []}])),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])

# memcache client to fetch latest values
client = base.Client(('localhost', 11211), serde=serde.pickle_serde)


# Setting up the callback for refreshing the plot every second
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    received_data: Dict[str, str] = client.get_many(config.TICKERS)
    parsed_data: Dict[str, int] = {str(ticker): int(quantity) if quantity is not None else 0
                                   for ticker, quantity in received_data.items()}

    print(f'Parsed data: {parsed_data}')

    return {
        "data": [
            {
                "x": list(parsed_data.keys()),
                "y": list(parsed_data.values()),
                "type": "bar"
            },
        ],
        "layout": {"title": "Tickers and quantities traded for symbols"}
    }


if __name__ == '__main__':
    app.run_server(dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
