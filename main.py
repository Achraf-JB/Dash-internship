from utilities.tools import argument_parser
from plots_preparation.generate_reports import ReportGenerator
import dash
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_html_components as html
import dash_bootstrap_components as dbc
import numpy as np


options = [{'label': str(i), 'value': str(i)} for i in range(1,7)]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
args = argument_parser()
report_generator = ReportGenerator(workspace=args.outputPath,)
app.layout = dbc.Container([

    dbc.Row(
        dbc.Col(html.H1("Sanity Check Dashboards",
                        className='text-center text-primary mb-4'),
                width=12)
    ),
    dbc.Row([

        dbc.Col([
            dcc.Dropdown(
                id='dropdown-type',
                options=[
                    {'label': 'Date', 'value': 'date'},
                    {'label': 'Machine', 'value': 'machine'},
                    {'label': 'Branche', 'value': 'branche'},
                    {'label': 'global rapport', 'value': 'global'}
                ],
                value='date',

            ),

        ],
            xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            dcc.Dropdown(id='dropdown-options'),
        ],
            xs=12, sm=12, md=12, lg=5, xl=5
        ),
        dbc.Col([
            dcc.Dropdown(
                id='days-input',
                options=options,
                value='6',
                placeholder="nombre de jour",
            ),
        ],
            xs=12, sm=12, md=12, lg=5, xl=5
        ),

    ], justify='centre'),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='graph'),
        ],
            xs=12, sm=12, md=12, lg=5, xl=5
        ),
    ],
    )

])

@app.callback(
    Output('dropdown-options', 'options'),
    Output('dropdown-options', 'value'),
    Input('dropdown-type', 'value'),

    )
def update_options(selected_type):
    options = []
    default_value = None

    if selected_type == 'date':
        options = [
            {'label': 'pr_label', 'value': 'pr_label'},
            {'label': 'tg_verdict', 'value': 'tg_verdict'},
            {'label': 'bar_plot', 'value': 'bar_plot'}
        ]
        default_value = 'pr_label'

    elif selected_type == 'machine':
        options = report_generator.selection_machine()
        default_value = report_generator.selection_machine()[0]

    elif selected_type == 'branche':
        options = np.append(report_generator.selection_machine() , "bar_chart")
        default_value = report_generator.selection_machine()[0]

    elif selected_type == 'global':
        options = [{'label': 'machine', 'value': 'machine'},
                   {'label': 'branche', 'value': 'branche'}
                   ]
        default_value = 'machine'

    return options, default_value


@app.callback(
    Output('graph', 'figure'),
    [Input('dropdown-type', 'value'),
    Input('dropdown-options', 'value'),
    Input('days-input', 'value')]
)
def update_selected_figure(selected_type, selected_option,nombre):
    if selected_type == 'date':
        figure = report_generator.dash_date(selected_option,nombre)
        return figure
    elif selected_type == 'machine':
        figure = report_generator.dash_machine(selected_option,nombre)
        return figure
    elif selected_type == 'branche':
        if selected_option == "bar_chart":
            figure = report_generator.dash_branche_bar_chart(nombre)
            return figure
        figure = report_generator.dash_branch(selected_option,nombre)
        return figure
    elif selected_type == 'global':
        if selected_option == "machine":
            figure = report_generator.dash_machine_global(nombre)
            return figure
        figure = report_generator.dash_branche_global(nombre)
        return figure


if __name__ == "__main__":
    app.run_server(debug=True)
