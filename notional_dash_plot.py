from typing import Dict, List

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

# memcache client to fetch latest values
redis_client = Redis(host='localhost', port=config.DEFAULT_REDIS_PORT, db=0)


# Setting up the callback for refreshing the plot every second
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    notional_keys = [f'{ticker}{config.NOTIONAL_KEY}' for ticker in config.TICKERS]
    received_data: Dict[str, bytes] = {ticker: redis_client.get(ticker) for ticker in notional_keys}
    parsed_data: Dict[str, float] = \
        {str(ticker): float(notional) for ticker, notional in received_data.items() if notional}

    x: List[str] = [ticker.split('.')[0] for ticker in parsed_data.keys()]
    y: List[float] = list(parsed_data.values())
    fig = go.Figure(data=[go.Bar(x=x, y=y, text=[int(v) for v in y], textposition='auto')])
    fig.update_layout(
        title={'text': '<b>Tickers vs Total Notional Traded</b>', 'x': 0.5},
        xaxis_title="Tickers",
        yaxis_title="Notional Traded",
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
    app.run_server(port="8052", dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
