import pandas as pd
import psycopg2
import plotly.express as px
import configparser

# Load the configuration from the 'config.ini' file


def load_configuration():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

# Fetch data from the PostgreSQL database based on a given SQL query


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
        cursor.close()  # Close the cursor to release resources
        conn.close()
        return results
    except Exception as err:
        print("Unable to connect! Exiting! Error:", err)
        exit()

# Load the cases and deaths data from the database


def load_data(db_config):
    selector_cases = 'SELECT "departmento" FROM public."PERUPositiveRawData"'
    selector_deaths = 'SELECT "departamento" FROM public."PERUDeathRawData"'
    data_cases = fetch_data_from_database(selector_cases, db_config)
    data_deaths = fetch_data_from_database(selector_deaths, db_config)
    return pd.DataFrame(data_cases, columns=['region']), pd.DataFrame(data_deaths, columns=['region'])

# Preprocess the data by handling missing values and adding aggregation columns


def preprocess_data(df_cases, df_deaths):
    df_cases.dropna(inplace=True)
    df_deaths.dropna(inplace=True)
    df_cases['cases'] = 1
    df_deaths['deaths'] = 1
    return df_cases, df_deaths

# Transform the data by grouping and merging cases and deaths data


def transform_data(df_cases, df_deaths):
    grouped_cases = df_cases.groupby('region').agg(
        {'cases': 'sum'}).reset_index()
    grouped_deaths = df_deaths.groupby('region').agg(
        {'deaths': 'sum'}).reset_index()
    return pd.merge(grouped_cases, grouped_deaths, on='region', suffixes=('_cases', '_deaths'))

# Create a bar plot using Plotly Express with specified parameters


def create_plot(df, x, y, title, plot_config):
    return px.bar(df, x=x, y=y, title=title, height=plot_config['height'], width=plot_config['width'])

# Main function to orchestrate the data loading, preprocessing, transformation, and visualization


def main():
    # Load configuration from file
    config = load_configuration()
    # Get database configuration
    db_config = config['database']
    # Get plot configuration
    plot_config = {key: int(value) for key, value in config['plot'].items()}

    # Load cases and deaths data
    df_cases, df_deaths = load_data(db_config)
    df_cases, df_deaths = preprocess_data(
        df_cases, df_deaths)  # Preprocess the data
    # Transform the data
    merged_df = transform_data(df_cases, df_deaths)

    # Create and show the plots
    fig_cases = create_plot(df_cases, 'region', 'cases',
                            'Confirmed Cases by Region', plot_config)
    fig_deaths = create_plot(
        df_deaths, 'region', 'deaths', 'Confirmed deaths by Region', plot_config)
    fig_combined = create_plot(merged_df, 'region', [
                               'cases', 'deaths'], 'Comparison of Cases and Deaths by Region', plot_config)

    fig_cases.show()
    fig_deaths.show()
    fig_combined.show()


if __name__ == "__main__":
    main()
