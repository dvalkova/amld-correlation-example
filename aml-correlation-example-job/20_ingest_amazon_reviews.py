# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import pandas as pd
import logging
import webscrape
import datefinder
from datetime import datetime
from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)


def run(job_input: IJobInput):
    """
    Scrape critical Amazon Reviews for one of the most popular Yankee candles on Amazon
    and ingest them into a local SQLite DB.
    """

    log.info(f"Starting job step {__name__}")

    # Get last_date property/parameter:
    #  - if the this is the first job run, initialize last_date to 2020-01-01 to fetch all rows
    #  - if the data job was run previously, take the property value already stored in the DJ from the previous run
    last_date = job_input.get_property("last_date", "2020-01-01")

    # Initialize variables
    i = 1
    rev_result = []
    date_result = []
    # Date to start iterating from = current date (in the format "2020-01-01")
    date = datetime.now().strftime("%Y-%m-%d")

    # Go through the review pages and scrape reviews from the beginning of 2020 onwards
    while date > last_date:
        log.info(f'Rendering page {i}...')
        # Parameterize the URL to iterate over the pages
        url = f"https://www.amazon.com/Yankee-Candle-Large-Balsam-Cedar/product-reviews/B000JDGC78/ref=cm_cr_arp_d_\
            viewopt_srt?ie=UTF8&reviewerType=all_reviews&filterByStar=critical&pageNumber={i}&sortBy=recent"

        # Get HTML code into a BeautifulSoup object
        soup = webscrape.html_code(url)
        # Get the reviews and dates for the current page
        rev_page = webscrape.cus_rev(soup)
        date_page = webscrape.rev_date(soup)[2:]

        # Append reviews text into a list removing the empty reviews
        for j in rev_page:
            if j.strip() == "" or j.strip() == "The media could not be loaded.":
                pass
            else:
                rev_result.append(j.strip())
        log.info(len(rev_result))

        # Append review dates into a list by extracting the date from text
        for d in date_page:
            if d.strip() == "":
                pass
            else:
                # Initially, dates are in the format "Reviewed in the United States on February 14, 2022"
                # datefinder package extracts the date from the text and converts it to datetime object
                date_match = datefinder.find_dates(d)
                for date in date_match:
                    # Convert to string
                    date = date.strftime("%Y-%m-%d")
                    date_result.append(date)
        log.info(len(date_result))

        # In each page, check whether there are more dates than reviews (empty reviews with photo only) and remove them
        while len(rev_result) < len(date_result):
            date_result.pop(-1)

        # Go to the next page
        i += 1

    # Create a pandas dataframe with the review text and dates
    df = pd.DataFrame(zip(date_result, rev_result), columns=['Date', 'Review'])
    log.info(f"Shape of the review dataset: {df.shape}")

    # Ingest the dataframe into a SQLite database using VDK's job_input method (if any results are fetched)
    if len(df) > 0:
        job_input.send_tabular_data_for_ingestion(
            rows=df.values,
            column_names=df.columns.to_list(),
            destination_table="yankee_candle_reviews",
            method="sqlite"
        )

    log.info(f"Success! {len(df)} rows were inserted in table yankee_candle_reviews.")

