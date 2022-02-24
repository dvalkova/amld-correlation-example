import os
import pandas as pd
import pathlib
import streamlit as st
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import scipy.stats

# Page title and description
st.title('Correlation analysis: COVID-19 cases in the US and Yankee candle reviews indicating "no scent"')
st.write('The dashboard shows the relationship between weekly COVID cases in the US and bad US Amazon reviews '
         'containing the words "smell", "fragrance" or "scent" for one of the most popular [Yankee candles on Amazon]'
         '(https://www.amazon.com/Yankee-Candle-Large-Balsam-Cedar/dp/B000JDGC78/ref=cm_cr_arp_d_product_top?ie=UTF8).')

# Sub-header
st.header('Number of weekly COVID cases and "no scent" reviews over time')

# Definitions
os.chdir(pathlib.Path(__file__).parent.absolute())
db_file = r'amld-correlation-example-job/correlation-example-db.db'

# Create a connection to the db and a cursor to read the weekly correlation table
con = sqlite3.connect(db_file)
cursor = con.cursor()
# Fetch data
cursor.execute("SELECT * FROM weekly_correlation")
df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda x: x[0], cursor.description)))
# Transform into datetime Series
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

# Plot # COVID cases vs no-scent complaints over time
fig, ax = plt.subplots(figsize=(12, 6))
ax2 = ax.twinx()
ax.set_title('No scent Yankee candle reviews and COVID cases')
ax.plot(df['date'], df['num_no_scent_reviews'], color='green')
ax2.plot(df['date'], df['number_of_covid_cases_weekly'], color='red')
ax.set_ylabel('# "No scent" reviews')
ax2.set_ylabel('# Covid cases weekly (in mln)')
ax.legend(['no scent reviews'])
ax2.legend(['weekly covid cases'], loc='upper center')
ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=range(1,13)))
ax.xaxis.set_minor_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(
    mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
plt.tight_layout()
st.pyplot(fig=plt)

# Sub-header
st.header('Weekly correlation between "no scent" reviews and covid cases')

# Current period correlation
corr_coeff = round(df[df['date']==max(df['date'])]['correlation_coeff'], 3)
st.metric("The current correlation coefficient is:", corr_coeff)

# Plot the correlation coefficients over time
st.write('Correlation coefficient over time:')
df = df.rename(columns={'date': 'index'}).set_index('index')
st.line_chart(data=df[['correlation_coeff']])
# Show data in a table
st.write('Underlying data:')
df = df.reset_index().rename(columns={'index': 'week'}).sort_values('week', ascending=False)
# Convert date to string
df['week'] = df['week'].dt.strftime('%Y-%m-%d')
# Visualize in the dashboard
st.dataframe(df[['week', 'correlation_coeff']])
