from dash import Dash, html, dcc, dash_table, Output, Input, State, no_update,ctx
import requests
import json
from collections import Counter
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server  # For Docker compatibility

# Backend endpoints
BACKEND_ENDPOINTS = {
    'mongodb': 'http://flask_api:5000',
    'supabase': 'http://supabase_api:5050',
    'neo4j': 'http://neo4j_api:6060'
}

# Layout
app.layout = html.Div([
    html.H1("Dashboard", style={'textAlign': 'center'}),

    html.Div([
        dcc.Tabs(id='backend-dropdown', value='mongodb', children=[
            dcc.Tab(label='MongoDB', value='mongodb'),
            dcc.Tab(label='Supabase', value='supabase'),
            dcc.Tab(label='Neo4j', value='neo4j')
        ],
        style={'marginBottom': '30px'}
        ),

        html.Label("Select Query:", style={
            'marginTop': '10px',
            'marginBottom': '5px',
            'fontWeight': 'bold'
        }),
        dcc.Dropdown(
            id='query-type',
            options=[
                {'label': 'Top Rated Movies', 'value': 'top-rated'},
                {'label': 'Most Popular Movies', 'value': 'most-popular'},
                {'label': 'Get All Movies (Limit 10)', 'value': 'movies'},
                {'label': 'Get Movie by ID', 'value': 'movie-by-id'},
                {'label': 'Get Ratings by Movie ID', 'value': 'ratings-by-id'},
                {'label': 'Get Tags by Movie ID', 'value': 'tags-by-id'},
                {'label': 'User Recommendations', 'value': 'recommendations-by-user'},
            ],
            value='top-rated',
            style={'marginBottom': '20px'}
        ),

        dcc.Input(id='extra-param', type='number',min=0, placeholder='Enter ID', style={'marginBottom': '20px', 'display': 'block'}),

        html.Button("Send", id='submit-button', n_clicks=0, style={
            'padding': '10px 20px',
            'fontSize': '16px',
            'backgroundColor': '#007BFF',
            'color': 'white',
            'border': 'none',
            'borderRadius': '5px',
            'cursor': 'pointer',
            'marginTop': '10px'
        }),

        html.H2("Raw JSON Result:", style={'marginTop': '40px'}),
        html.Pre(id='result-output', style={'whiteSpace': 'pre-wrap', 'wordBreak': 'break-word'}),

        dcc.Graph(id='result-graph', style={'marginTop': '40px'}),

        html.H2("Table View:", style={'marginTop': '40px'}),
        dash_table.DataTable(
            id='result-table',
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '5px',
                'whiteSpace': 'normal'
            },
            style_header={
                'backgroundColor': '#f2f2f2',
                'fontWeight': 'bold'
            }
        ),
    ], style={
        'maxWidth': '800px',
        'margin': 'auto',
        'padding': '40px',
        'border': '1px solid #ccc',
        'borderRadius': '10px',
        'boxShadow': '2px 2px 12px rgba(0,0,0,0.1)'
    })
])

# Callback to fetch and display results + graph + table
@app.callback(
    Output('result-output', 'children'),
    Output('result-graph', 'figure'),
    Output('result-table', 'data'),
    Output('result-table', 'columns'),
    Input('submit-button', 'n_clicks'),
    State('backend-dropdown', 'value'),
    State('query-type', 'value'),
    State('extra-param', 'value'),
    prevent_initial_call=True
)
def update_output(n_clicks, backend, query_type, extra_param):
    base_url = BACKEND_ENDPOINTS[backend]

    url = {
        'movie-by-id': f"{base_url}/movies/{extra_param}",
        'ratings-by-id': f"{base_url}/movies/{extra_param}/ratings",
        'tags-by-id': f"{base_url}/movies/{extra_param}/tags",
        'recommendations-by-user': f"{base_url}/user-recommendations/{extra_param}"
    }.get(query_type, f"{base_url}/{query_type}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        raw_data = data.copy() if isinstance(data, dict) else None
        fig = None

        if query_type in ['top-rated', 'most-popular'] and isinstance(data, list):
            labels = [item.get('title', 'N/A') for item in data]
            values = [item.get('avgRating') if query_type == 'top-rated' else item.get('ratingCount') for item in data]
            fig = {
                'data': [{'x': labels, 'y': values, 'type': 'bar'}],
                'layout': {'title': f"{query_type.replace('-', ' ').title()} Chart"}
            }
        elif query_type in ['movie-by-id', 'ratings-by-id']:
            ratings = data.get('ratings') if isinstance(data, dict) else data
            if isinstance(ratings, list):
                rating_values = [r.get('rating') for r in ratings if 'rating' in r]
                if rating_values:
                    counts = Counter(rating_values)
                    fig = {
                        'data': [{'x': list(counts.keys()), 'y': list(counts.values()), 'type': 'bar'}],
                        'layout': {'title': 'Rating Distribution'}
                    }

        # Hide ratings from JSON/table
        if isinstance(data, dict) and 'ratings' in data:
            data = {k: v for k, v in data.items() if k != 'ratings'}
     
        def flatten(record):
            return {
                k: (
                    " | ".join(json.dumps(i) if isinstance(i, dict) else str(i) for i in v)
                    if isinstance(v, list) else
                    json.dumps(v) if isinstance(v, dict) else
                    v
                )
                for k, v in record.items()
            }

        table_data = []
        table_columns = []

        # Handle list of records (e.g. /movies, /top-rated)
        if isinstance(data, list) and all(isinstance(row, dict) for row in data):
            table_data = [flatten(row) for row in data]
        elif isinstance(data, dict):
            table_data = [flatten(data)]

        if table_data:
            table_columns = [{"name": k, "id": k} for k in table_data[0].keys()]

        return json.dumps(data, indent=2), fig or no_update, table_data, table_columns

    except Exception as e:
        return f"Error: {str(e)}", {}, [], []

@app.callback(
    Output('result-graph', 'style'),
    Input('query-type', 'value')
)
def hide_graph_for_certain_queries(query_type):
    no_graph_queries = ['movies', 'tags-by-id', 'recommendations-by-user']
    if query_type in no_graph_queries:
        return {'display': 'none'}
    return {'marginTop': '40px'}
# Show or hide extra input box based on query type
@app.callback(
    Output('extra-param', 'style'),
    Input('query-type', 'value')
)
def toggle_extra_input_visibility(query_type):
    if query_type in ['movie-by-id', 'ratings-by-id', 'tags-by-id', 'recommendations-by-user']:
        return {'display': 'block', 'marginTop': '20px'}
    return {'display': 'none'}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
