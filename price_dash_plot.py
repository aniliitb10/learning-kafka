from typing import Dict, List, Tuple
from datetime import datetime
import pickle
from sys import argv

import plotly.graph_objects as go

# dash imports
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

# memcache
from pymemcache.client import base

import config  # to fetch the manually configured values

# dash components
app = Dash(__name__, update_title=None)
app.layout = html.Div([
    # html.H1("Live updates"),
    dcc.Graph(id='graph', figure=dict(data=[{'x': [], 'y': []}])),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])

# memcache client to fetch latest values
client = base.Client(('localhost', 11211))


# Setting up the callback for refreshing the plot every second
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    ltp_keys = list(f"{ticker}.ltp" for ticker in config.TICKERS)
    received_data: Dict[str, bytes] = client.get_many(ltp_keys)
    parsed_data: Dict[str, Tuple[List[float], List[datetime]]] = {ticker: pickle.loads(value)
                                                                  if value is not None else([], [])
                                                                  for ticker, value in received_data.items()}
    fig = go.Figure()
    for ticker_ltp, data in parsed_data.items():
        ticker_name = ticker_ltp.split('.')[0]
        print(f'name: {ticker_name}, x: {data[1]}, y:{data[0]}')
        fig.add_trace(go.Scatter(x=data[1], y=data[0], name=ticker_name, mode='lines+markers'))

    fig.update_layout(
        title={'text': '<b>LTP of Tickers</b>', 'x': 0.5},
        xaxis_title="Time (in seconds)",
        yaxis_title="LTP of Tickers",
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        ),
        # autosize=False,
        height=700
    )

    return fig


if __name__ == '__main__':
    port = "8051" if len(argv) == 1 else argv[1]
    app.run_server(port=port, dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
