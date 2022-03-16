# streamlit_app.py

import streamlit as st
import snowflake.connector

from streamlit_echarts import st_echarts
import pandas as pd
from vega_datasets import data


st.set_page_config(
       page_title="SnowFlake Connector + Echarts Demo",
       page_icon=":chart_with_upwards_trend:",
    )

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


rows = run_query("SELECT * from MYTABLEECHARTS2;")
df = pd.DataFrame(rows)
df.columns = df.iloc[0]
df = df[1:]

# with st.expander("View the raw data", expanded=False):
# 
#     df

def main():
    df = data.us_employment().set_index("month").drop(columns=["nonfarm_change"])
    means = df.mean(axis=0).map("{:.2f}".format)

    bar_options = {
        "title": {"text": "Mean US employment this past decade"},
        "xAxis": {
            "type": "category",
            "axisTick": {"alignWithLabel": True},
            "data": means.index.values.tolist(),
        },
        "yAxis": {"type": "value"},
        "tooltip": {"trigger": "item"},
        "emphasis": {"itemStyle": {"color": "#a90000"}},
        "series": [{"data": means.tolist(), "type": "bar"}],
    }

    with st.expander("View the raw data", expanded=False):

        df

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv()

    csv = convert_df(df)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name="Dataframe.csv",
        mime="text/csv",
    )

    clicked_label = st_echarts(
        bar_options,
        events={"mouseover": "function(params) {return params.name}"},
        height="500px",
        key="global",
    )

    if clicked_label is None:
        return

    c0, c1, c2, c3 = st.columns([0.5, 1, 1, 1])

    max_value = int(df[clicked_label].max() / 1000)
    min_value = int(df[clicked_label].min() / 1000)
    avg_value = int(df[clicked_label].mean() / 1000)

    with c0:
        st.title("")
    with c1:
        st.metric("Max value", str(max_value) + " K")
    with c2:
        st.metric("Min value", str(min_value) + " K")
    with c3:
        st.metric("Avg value", str(avg_value) + " K")

    st.title("")

    filtered_df = df[clicked_label].sort_index()
    line_options = {
        "title": {"text": f"Breakdown US employment for {clicked_label}"},
        "xAxis": {
            "type": "category",
            "axisTick": {"alignWithLabel": True},
            "data": filtered_df.index.values.tolist(),
        },
        "yAxis": {"type": "value"},
        "tooltip": {"trigger": "axis"},
        "itemStyle": {"color": "#a90000"},
        "lineStyle": {"color": "#a90000"},
        "series": [
            {
                "data": filtered_df.tolist(),
                "type": "line",
                "smooth": True,
            }
        ],
    }
    clicked_label = st_echarts(line_options, key="detail")


if __name__ == "__main__":

    st.image("logo.gif")

    st.header("❄️ SnowFlake Connector & Echarts Demo") 

    st.write(
         """
         This demo app:

 1. Uses the [SnowFlake Connector](https://docs.streamlit.io/knowledge-base/tutorials/databases/snowflake) to connect to a SnowFlake account and query a SnowFlake table
 4. Displays the results back in the Streamlit app
 5. Uses the [Echarts library](https://echarts.apache.org/examples/en/editor.html?c=bubble-gradient) via the new `streamlit-echarts 0.4.0` component to create cross-filtering on mouse-over interaction!
    
             """
     )

    main()