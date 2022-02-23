# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import pandas as pd
import numpy as np
import logging
import os
import pathlib
import scipy.stats
import matplotlib.pyplot as plt
from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)
os.chdir(pathlib.Path(__file__).parent.absolute())


def run(job_input: IJobInput):
    """
    Calculate the weekly correlation between "no scent" Yankee candle reviews and COVID cases in the US.
    Save the weekly correlations in a table in the DB.
    """

    log.info(f"Starting job step {__name__}")

    # Get last_date property/parameter:
    #  - if the this is the first job run, initialize last_date to 2020-01-01 to fetch all rows
    #  - if the data job was run previously, take the property value already stored in the DJ from the previous run
    last_date = job_input.get_property("last_date", "2020-01-01")

    # Read the candle review data and transform to df
    reviews = job_input.execute_query(
        f"""
        SELECT date, num_no_scent_reviews 
        FROM yankee_candle_reviews_transformed
        WHERE date > '{last_date}'
        """
    )
    reviews_df = pd.DataFrame(reviews, columns=['date', 'num_no_scent_reviews'])
    # Original date format: "2022-02-06T00:00:00". Transform into "2022-02-06"
    #reviews_df['date'] = reviews_df['date'].str[:10]

    # Read the covid data and transform to df
    covid = job_input.execute_query(
        f"""
        SELECT * 
        FROM covid_cases_usa_daily
        WHERE date > '{last_date}'
        """
    )
    covid_df = pd.DataFrame(covid, columns=['date', 'number_of_covid_cases'])

    # Merge the two dataframes and fill missing values with 0. Use right join since reviews_df doesn't contain all dates
    df_merged = reviews_df.merge(covid_df, on=['date'], how='right').fillna(0)

    # If any data is returned, calculate weekly stats
    if len(df_merged) > 0:
        # Calculate new covid cases per day (current numbers are cumulative)
        df_merged['date'] = pd.to_datetime(df_merged['date'], format='%Y-%m-%d')
        df_merged['number_of_covid_cases_daily'] = df_merged['number_of_covid_cases'].diff(periods=-1).fillna(0)

        # Aggregate data on weekly level
        df_merged_weekly = df_merged.copy()
        # This step is necessary so that weekly calculations look 7 days ahead instead of backwards
        # (i.e. on Monday report the numbers for the period Monday-Sunday)
        df_merged_weekly['date'] = pd.to_datetime(df_merged_weekly['date']) - pd.to_timedelta(6, unit='d')
        # Aggregate on week-start level
        df_merged_weekly = df_merged_weekly.resample('W-MON', on='date').sum().reset_index()
        df_merged_weekly = df_merged_weekly.rename(columns={'number_of_covid_cases_daily': 'number_of_covid_cases_weekly'})\
                                           .drop(columns=["number_of_covid_cases"])
        # Sort df values by date
        df_merged_weekly = df_merged_weekly.sort_values('date', ascending=True).reset_index(drop=True)

        # Calculate correlation coefficients for each week in the df_merged_weekly table
        corr_coeff = [np.nan]
        for i in range(1, len(df_merged_weekly)):
            corr_coeff.append(df_merged_weekly['num_no_scent_reviews'][:i]
                              .corr(df_merged_weekly['number_of_covid_cases_weekly'][:i]))
        # Add them as a column in the df
        df_merged_weekly['correlation_coeff'] = corr_coeff

        # Ingest the weekly data and correlation coefficients into a new table using VDK's job_input method
        job_input.send_tabular_data_for_ingestion(
            rows=df_merged_weekly.values,
            column_names=df_merged_weekly.columns.to_list(),
            destination_table="weekly_correlation",
            method="sqlite"
        )

    # Reset the last_date property value to the latest date in the covid source db table
    job_input.set_all_properties({"last_date": max(covid_df['date'])})

    log.info(f"Success! {len(df_merged_weekly)} rows were inserted.")
