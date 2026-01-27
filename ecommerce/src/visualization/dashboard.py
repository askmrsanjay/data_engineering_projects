import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time

# Set page config
st.set_page_config(
    page_title="E-commerce Real-time BI Dashboard",
    page_icon="📊",
    layout="wide"
)

def main():
    st.title("🚀 E-commerce Real-time Analytics Dashboard")
    st.markdown("---")

    # Sidebar for filters
    st.sidebar.header("Settings")
    refresh_rate = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 10)
    
    # Check if the data file exists
    # We now support Real-Time data from the Silver Streaming job
    csv_path = "realtime_sales_update.csv"
    
    if not os.path.exists(csv_path):
        st.warning("Waiting for Real-Time data stream...")
        st.info("The streaming job (silver_transformation.py) creates this file.")
        time.sleep(5)
        st.rerun()
        return

    # Placeholder for the main metrics
    metric_col1, metric_col2, metric_col3 = st.columns(3)
    
    # Placeholder for charts
    chart_col1, chart_col2 = st.columns(2)

    try:
        # Load Data from CSV (Raw Transactions)
        # Columns: event_timestamp, category, price, user_id
        pd_df = pd.read_csv(csv_path, names=["event_timestamp", "category", "price", "user_id"])

        if pd_df.empty:
            st.warning("Data stream is empty. Waiting for events...")
            time.sleep(5)
            st.rerun()
            return

        # 1. KPI Metrics (Aggregated on the fly)
        total_revenue = pd_df["price"].sum()
        total_transactions = len(pd_df)
        unique_buyers = pd_df["user_id"].nunique()

        with metric_col1:
            st.metric("Live Revenue", f"${total_revenue:,.2f}", delta="Real-Time")
        with metric_col2:
            st.metric("Total Transactions", f"{total_transactions:,}")
        with metric_col3:
            st.metric("Unique Buyers", f"{unique_buyers:,}")

        # 2. Revenue by Category (Bar Chart)
        with chart_col1:
            st.subheader("Live Revenue by Category")
            fig_category = px.bar(
                pd_df.groupby("category")["price"].sum().reset_index(),
                x="category",
                y="price",
                color="category",
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Vivid
            )
            fig_category.update_layout(showlegend=False)
            st.plotly_chart(fig_category, use_container_width=True)

        # 3. Recent Transactions Table (Instead of Trend)
        with chart_col2:
            st.subheader("Latest Transactions")
            st.dataframe(
                pd_df.sort_values("event_timestamp", ascending=False).head(10)[["event_timestamp", "category", "price", "user_id"]],
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")

    # Aggressive auto-refresh for Real-Time feel
    time.sleep(2)
    st.rerun()

if __name__ == "__main__":
    main()
