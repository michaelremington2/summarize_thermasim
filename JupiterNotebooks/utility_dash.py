import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import numpy as np

def initialize_max_metabolic_state(max_meals, calories_per_gram, expected_prey_body_size):
    return max_meals * calories_per_gram * expected_prey_body_size

def thermal_accuracy_calculator(T_b, T_opt):
    return abs(T_b - T_opt)

def scale_value(value, max_value):
    x = value / max_value
    return min(x, 1.0)

def softmax(x, temperature=1.0):
    x = np.array(x)
    x = x - np.max(x)  # improve numerical stability
    exp_x = np.exp(x / temperature)
    return exp_x / np.sum(exp_x)

def sparsemax(x):
    """
    Sparsemax function: returns sparse probability distribution (some probabilities exactly zero).
    See: https://arxiv.org/abs/1602.02068
    """
    x = np.array(x)
    x = x - np.mean(x)  # optional but improves numerical stability

    z_sorted = np.sort(x)[::-1]
    z_cumsum = np.cumsum(z_sorted)
    k = np.arange(1, len(x) + 1)

    # Determine k_max
    support = z_sorted + (1.0 / k) * (1 - z_cumsum) > 0
    k_max = k[support][-1]
    tau_sum = z_cumsum[support][-1]
    tau = (tau_sum - 1) / k_max

    return np.maximum(x - tau, 0)


app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Behavior Utility Visualizer"),
    html.Div(children='Dynamic variables:'),
    html.Div(children='------------------'),

    html.Label("Metabolic state (M)"),
    html.Div(id='M-value', style={'margin-bottom': '3px'}),
    dcc.Slider(id='M', min=0, max=300, step=1, value=100, marks={0:'0',50:'50',100:'100',150:'150',200:'200',300:'300'}),

    html.Label("Body temperature (T_b)"),
    html.Div(id='T_b-value', style={'margin-bottom': '3px'}),
    dcc.Slider(id='T_b', min=5, max=40, step=0.5, value=30, marks={10:'10',20:'20',30:'30',40:'40'}),

    html.Div(children='Model Input variables:'), 
    html.Div(children='--------------------'),

    html.Label("Max meals (M_max)"),
    html.Div(id='M_max-value', style={'margin-bottom': '3px'}),
    dcc.Slider(id='M_max', min=1, max=10, step=1, value=3, marks={1:'1',3:'3',5:'5',10:'10'}),

    html.Label("Optimal temperature (T_opt)"),
    html.Div(id='T_opt-value', style={'margin-bottom': '3px'}),
    dcc.Slider(id='T_opt', min=20, max=40, step=0.5, value=26, marks={20:'20',30:'30',40:'40'}),

    html.Label("Max temperature difference (T_max)"),
    html.Div(id='T_max-value', style={'margin-bottom': '3px'}),
    dcc.Slider(id='T_max', min=1, max=20, step=0.5, value=5, marks={1:'1',5:'5',10:'10',20:'20'}),

    html.H3(id='preferred-behavior'),

    html.Div([
        dcc.Graph(id='utility-graph', style={'width': '45%', 'margin-right': '10px'}),
        dcc.Graph(id='probability-graph', style={'width': '45%', 'margin-left': '10px'})
    ], style={'display': 'flex', 'flex-direction': 'row'})
])

@app.callback(
    [Output('utility-graph', 'figure'),
     Output('probability-graph', 'figure'),
     Output('preferred-behavior', 'children')],
    [Input('M', 'value'),
     Input('M_max', 'value'),
     Input('T_b', 'value'),
     Input('T_opt', 'value'),
     Input('T_max', 'value')]
)
def update_graph(M, max_meals, T_b, T_opt, T_max, temperature=1.0):
    # Calculate maximum metabolic state
    M_max = initialize_max_metabolic_state(max_meals=max_meals, calories_per_gram=1.38, expected_prey_body_size=65)
    db = thermal_accuracy_calculator(T_b, T_opt)

    U_rest = scale_value(M, M_max)
    U_forage = 1 - U_rest
    U_thermo = scale_value(db, T_max)

    utilities = [U_rest, U_forage, U_thermo]
    labels = ["Rest", "Forage", "Thermoregulate"]

    # Compute behavior probabilities with softmax
    #behavior_probs = softmax(utilities, temperature=temperature)
    behavior_probs = sparsemax(utilities)

    # Randomly choose preferred behavior based on probabilities
    preferred_idx = np.random.choice(len(labels), p=behavior_probs)
    preferred = labels[preferred_idx]

    # Create utilities bar chart
    fig_utilities = go.Figure(data=[
        go.Bar(x=labels, y=utilities, marker_color='blue')
    ])
    fig_utilities.update_layout(yaxis=dict(range=[0,1], title='Utility'),
                                title="Behavior Utilities")

    # Create probabilities bar chart
    fig_probs = go.Figure(data=[
        go.Bar(x=labels, y=behavior_probs, marker_color='lightgreen')
    ])
    fig_probs.update_layout(yaxis=dict(range=[0,1], title='Probability'),
                            title="Behavior Choice Probabilities")

    return fig_utilities, fig_probs, f"Preferred Behavior: {preferred} (P={behavior_probs[preferred_idx]:.2f})"

# Single callback to update all slider value displays
@app.callback(
    [Output('M-value', 'children'),
     Output('T_b-value', 'children'),
     Output('M_max-value', 'children'),
     Output('T_opt-value', 'children'),
     Output('T_max-value', 'children')],
    [Input('M', 'value'),
     Input('T_b', 'value'),
     Input('M_max', 'value'),
     Input('T_opt', 'value'),
     Input('T_max', 'value')]
)
def update_slider_values(M, T_b, M_max, T_opt, T_max):
    return (
        f"M: {M}",
        f"T_b: {T_b:.1f}",
        f"M_max: {M_max}",
        f"T_opt: {T_opt:.1f}",
        f"T_max: {T_max:.1f}"
    )

if __name__ == '__main__':
    app.run(debug=True)
