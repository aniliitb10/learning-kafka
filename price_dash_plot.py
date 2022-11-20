import pickle
from datetime import datetime
from typing import Dict, List, Tuple

import plotly.graph_objects as go
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from redis import Redis

import config  # to fetch the manually configured values

# dash components
app = Dash(__name__, update_title=None)
app.layout = html.Div([
    dcc.Graph(id='graph', figure=dict(data=[{'x': [], 'y': []}])),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0)
])

#  Redis client to fetch latest values
redis_client = Redis(host='localhost', port=config.DEFAULT_REDIS_PORT, db=0)


# Setting up the callback for refreshing the plot every second
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    ltp_keys = list(f"{ticker}{config.LTP_KEY}" for ticker in config.TICKERS)
    received_data: Dict[str, bytes] = {key: redis_client.get(key) for key in ltp_keys}
    parsed_data: Dict[str, Tuple[List[float], List[datetime]]] = {
        ticker: pickle.loads(value) if value is not None else ([], [])
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
    app.run_server(port="8051", dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
