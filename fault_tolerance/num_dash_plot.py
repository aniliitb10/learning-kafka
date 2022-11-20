import pickle
from typing import Dict, List, Optional

import plotly.graph_objects as go
# dash imports
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from redis import Redis

from fault_tolerance import config

# dash components
app = Dash(__name__, update_title=None)
app.layout = html.Div([
    dcc.Graph(id='graph'),
    dcc.Interval(id="interval-component", interval=2000, n_intervals=0)
])

# memcache client to fetch latest values
redis_client = Redis(host='localhost', port=config.DEFAULT_REDIS_PORT)


# Setting up the callback for refreshing the plot every second
@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def handle_stream(_):
    received_data: Dict[str, Optional[bytes]] = {consumer_id: redis_client.get(consumer_id)
                                                 for consumer_id in config.CONSUMER_IDs}
    parsed_data: Dict[str, List[int]] = {consumer_id: pickle.loads(numbers)
                                         for consumer_id, numbers in received_data.items() if numbers}
    print(f'Parsed data: {parsed_data}')
    bars: List[go.Bar] = []
    for consumer_id in parsed_data.keys():
        for num in parsed_data[consumer_id]:
            bars.append(go.Bar(x=[consumer_id], y=[1], name=str(num), text=str(num), textposition='auto'))

    fig = go.Figure(data=bars, layout=go.Layout(barmode='stack'))
    fig.update_layout(
        title={'text': '<b>Numbers published on consumers</b>', 'x': 0.5},
        xaxis_title="Consumers",
        yaxis_title="Numbers published",
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        ),
        # autosize=False,
        height=1000
    )

    return fig


if __name__ == '__main__':
    app.run_server(port="8052", dev_tools_ui=True, dev_tools_hot_reload=True, threaded=True)
