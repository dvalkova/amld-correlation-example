# AMLD Conference: Correlation between COVID cases in the US and reviews of Yankee candles indicating "no scent" 

To start the mybinder environment with the solution and results, click on the image below:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/dvalkova/amld-correlation-example/HEAD)

### Background

The example investigates the relationship between weekly COVID cases in the US and bad US Amazon reviews containing the words "smell", "fragrance" or "scent" for one of the most popular [Yankee candles on Amazon](https://www.amazon.com/Yankee-Candle-Large-Balsam-Cedar/dp/B000JDGC78/ref=cm_cr_arp_d_product_top?ie=UTF8).

The example uses the functionalities of VDK to create, automate and execute on schedule the program that ingests the raw data into a database, performs transformations and builds a Streamlit dashboard to showcase the results.

The Amazon reviews are fetched through webscraping with the help of the [BeautifulSoup](https://pypi.org/project/beautifulsoup4/) Python package. The daily COVID-19 data for the US is fetched using an [API](https://github.com/M-Media-Group/Covid-19-API).

### Learning outcomes

* Build upon what was already covered in scenario 1 and 2
* Publish extracted data to a configured DB in an incremental fashion (i.e. not ingest records that are already present in the tables)
* Read data from DB
* Correlation calculation
* Present the results in a Streamlit App showing:
  * Weekly Covid cases vs. No Scent complaints over time
  * Recalculate the correlation each week as new data comes in and check how results change over time
* Schedule data job executions once per week


### Results

This repository contains the scripts of the data job and the Streamlit dashboard, as well as the already configured database containing the initially ingested data.
