import pandas as pd
import psycopg2
import plotly.express as px


def main():
    # Load the CSV file for Cases
    selector = 'SELECT "departmento" FROM public."PERUPositiveRawData"'
    data = fetch_data_from_database(selector)
    column_names = ['region']
    df_cases = pd.DataFrame(data, columns=column_names)

    # load the CSV for Deaths
    selector = 'SELECT "departamento" FROM public."PERUDeathRawData"'
    data = fetch_data_from_database(selector)
    column_names = ['region']
    df_deaths = pd.DataFrame(data, columns=column_names)

    # 3. Clean and preprocess the data
    # Handle missing values (if any)
    df_cases.dropna(inplace=True)
    df_deaths.dropna(inplace=True)
    # Add columns for aggregation
    df_cases['cases'] = 1
    df_deaths['deaths'] = 1

    # 4. Transform the data into the desired structure
    # Group by region and calculate summary statistics
    grouped_df_cases = df_cases.groupby('region').agg(
        {'cases': 'sum'}).reset_index()
    grouped_df_deaths = df_deaths.groupby('region').agg(
        {'deaths': 'sum'}).reset_index()

    # merge dataframes
    merged_df = pd.merge(grouped_df_cases, grouped_df_deaths,
                         on='region', suffixes=('_cases', '_deaths'))

    print(merged_df)

    # 7. Validate the prepared data
    fig_cases = px.bar(grouped_df_cases,
                       x='region',
                       y='cases',
                       title='Confirmed Cases by Region',
                       height=400,
                       width=800)
    fig_cases.show()

    fig_deaths = px.bar(grouped_df_deaths,
                        x='region',
                        y='deaths',
                        title='Confirmed deaths by Region',
                        height=400,
                        width=800)
    fig_deaths.show()

    fig = px.bar(merged_df,
                 x='region',
                 y=['cases', 'deaths'],
                 title='Comparison of Cases and Deaths by Region',
                 height=400,
                 width=800)

    fig.show()


def fetch_data_from_database(selector):
    # Try to connect to the database
    try:
        conn = psycopg2.connect(
            host="pixel.ourcloud.ou.edu",
            port=5432,
            database="panviz",
            user="panviz_readonly",
            password="T3u&c7U58V9H"
        )
    except Exception as err:
        # If the connection fails, print the error message and exit the program
        print("Unable to connect! Exiting! Error:", err)
        exit()

    # If the connection is successful, fetch the data
    cursor = conn.cursor()
    cursor.execute(selector)
    results = cursor.fetchall()

    # Close the connection to the database
    conn.close()

    # Return the fetched data
    return results


if __name__ == "__main__":
    main()
