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
    csv_path = "gold_sales_dashboard.csv"
    
    if not os.path.exists(csv_path):
        st.error(f"Waiting for data... (File '{csv_path}' not found)")
        st.info("The Gold Batch job needs to run once to generate the dashboard data.")
        
        # Add a trigger button for convenience
        if st.button("Trigger Gold Job (via Spark Container)"):
            st.warning("Triggering... please wait.")
            # In a real app, you'd call an API. Here we just give instructions.
            st.code("docker exec ecommerce-spark-master spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 /opt/bitnami/spark/app/src/batch/gold_batch.py")
        
        time.sleep(refresh_rate)
        st.rerun()
        return

    # Placeholder for the main metrics
    metric_col1, metric_col2, metric_col3 = st.columns(3)
    
    # Placeholder for charts
    chart_col1, chart_col2 = st.columns(2)

    try:
        # Load Data from CSV (No Spark needed on host!)
        pd_df = pd.read_csv(csv_path)

        if pd_df.empty:
            st.warning("No data found in the Gold layer yet.")
            return

        # 1. KPI Metrics
        total_revenue = pd_df["total_revenue"].sum()
        total_transactions = pd_df["total_transactions"].sum()
        unique_buyers = pd_df["unique_buyers"].sum()

        with metric_col1:
            st.metric("Total Revenue", f"${total_revenue:,.2f}")
        with metric_col2:
            st.metric("Total Transactions", f"{total_transactions:,}")
        with metric_col3:
            st.metric("Unique Buyers", f"{unique_buyers:,}")

        # 2. Revenue by Category (Bar Chart)
        with chart_col1:
            st.subheader("Revenue by Category")
            fig_category = px.bar(
                pd_df.groupby("category")["total_revenue"].sum().reset_index(),
                x="category",
                y="total_revenue",
                color="category",
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Vivid
            )
            fig_category.update_layout(showlegend=False)
            st.plotly_chart(fig_category, use_container_width=True)

        # 3. Revenue Trend (Line Chart)
        with chart_col2:
            st.subheader("Hourly Revenue Trend")
            pd_df['hour_start'] = pd.to_datetime(pd_df['hour_start'])
            trend_df = pd_df.groupby("hour_start")["total_revenue"].sum().reset_index()
            fig_trend = px.line(
                trend_df.sort_values("hour_start"),
                x="hour_start",
                y="total_revenue",
                markers=True,
                template="plotly_dark"
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        # Raw Data Table
        st.subheader("Recent Sales Aggregates")
        st.dataframe(pd_df.sort_values("hour_start", ascending=False), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")

    # Simple auto-refresh
    time.sleep(refresh_rate)
    st.rerun()

if __name__ == "__main__":
    main()
