import sqlite3
import altair as alt
import pandas as pd
import streamlit as st


sqliteConnection = sqlite3.connect('/tmp/my-db.db')

data = pd.read_sql_query(
    f"select id, count(1) as cnt from hello_table group by id", sqliteConnection
)

chart = (
    alt.Chart(data)
        .mark_bar()
        .encode(
            x="id",
            y=alt.Y("cnt", stack=None),
    )
)
st.altair_chart(chart, use_container_width=True)
