# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging
import requests
import pandas as pd
from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)


def run(job_input: IJobInput):
    """
    Collect COVID-19 historical data for number of cases per day in the US since the start of the pandemic through
    an API call. Ingest this data into a table in a local SQLite database.
    """

    log.info(f"Starting job step {__name__}")

    # Get last_date property/parameter:
    #  - if the this is the first job run, initialize last_date to 2020-01-01 to fetch all rows
    #  - if the data job was run previously, take the property value already stored in the DJ from the previous run
    last_date = job_input.get_property("last_date", "2020-01-01")

    # Initialize URL
    url = "https://covid-api.mmediagroup.fr/v1/history?country=US&status=confirmed"

    # Make a GET request to the COVID-19 API
    response = requests.get(url)
    # Check if the request was successful
    response.raise_for_status()

    dates_cases = response.json()['All']['dates']
    # Reformat the dictionary
    dates_cases_dict = {'obs_date': [], 'number_of_cases': []}

    for key, value in dates_cases.items():
        dates_cases_dict['obs_date'].append(key)
        dates_cases_dict['number_of_cases'].append(value)

    # Convert the dictionary into a DF
    df_covid = pd.DataFrame.from_dict(dates_cases_dict)
    # Keep only the dates which are not present in the table already (based on last_date property)
    df_covid = df_covid[df_covid['obs_date'] > last_date]

    # Ingest the dictionary into a SQLite database using VDK's job_input method (if any results are fetched)
    if len(df_covid) > 0:
        job_input.send_tabular_data_for_ingestion(
            rows=df_covid.values,
            column_names=df_covid.columns.to_list(),
            destination_table="covid_cases_usa_daily",
            method="sqlite"
        )

    log.info(f"Success! {len(df_covid)} rows were inserted in table covid_cases_usa_daily.")
