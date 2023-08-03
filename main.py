import configparser
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
import plotly.express as px


def load_configuration():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['database'], {key: int(value) for key, value in config['plot'].items()}


def fetch_data_from_database(selector, db_config):
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()
        cursor.execute(selector)
        results = cursor.fetchall()
        cursor.close()  # Close the cursor
        conn.close()
        return results
    except Exception as err:
        print("Unable to connect! Exiting! Error:", err)
        exit()


def main(db_config, plot_config):
    selector = 'SELECT "departmento" FROM public."PERUPositiveRawData"'
    data = fetch_data_from_database(selector, db_config)
    column_names = ['region']
    df_cases = pd.DataFrame(data, columns=column_names)

    selector = 'SELECT "departamento" FROM public."PERUDeathRawData"'
    data = fetch_data_from_database(selector, db_config)
    column_names = ['region']
    df_deaths = pd.DataFrame(data, columns=column_names)

    df_cases.dropna(inplace=True)
    df_deaths.dropna(inplace=True)
    df_cases['cases'] = 1
    df_deaths['deaths'] = 1

    grouped_df_cases = df_cases.groupby(
        'region').agg({'cases': 'sum'}).reset_index()
    grouped_df_deaths = df_deaths.groupby(
        'region').agg({'deaths': 'sum'}).reset_index()
    merged_df = pd.merge(grouped_df_cases, grouped_df_deaths,
                         on='region', suffixes=('_cases', '_deaths'))

    fig_cases = px.bar(grouped_df_cases, x='region', y='cases', title='Confirmed Cases by Region',
                       height=plot_config['height'], width=plot_config['width'])
    fig_deaths = px.bar(grouped_df_deaths, x='region', y='deaths', title='Confirmed deaths by Region',
                        height=plot_config['height'], width=plot_config['width'])
    fig_combined = px.bar(merged_df, x='region', y=[
                          'cases', 'deaths'], title='Comparison of Cases and Deaths by Region', height=plot_config['height'], width=plot_config['width'])

    return fig_cases, fig_deaths, fig_combined


db_config, plot_config = load_configuration()
fig_cases, fig_deaths, fig_combined = main(db_config, plot_config)

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(
        id='graph-selector',
        options=[
            {'label': 'Confirmed Cases by Region', 'value': 'cases'},
            {'label': 'Confirmed Deaths by Region', 'value': 'deaths'},
            {'label': 'Comparison of Cases and Deaths', 'value': 'combined'}
        ],
        value='cases',
    ),
    dcc.Graph(id='graph-display')
])


@app.callback(
    Output('graph-display', 'figure'),
    [Input('graph-selector', 'value')]
)
def update_graph(selected_value):
    if selected_value == 'cases':
        return fig_cases
    elif selected_value == 'deaths':
        return fig_deaths
    elif selected_value == 'combined':
        return fig_combined


if __name__ == '__main__':
    app.run_server(debug=True)
