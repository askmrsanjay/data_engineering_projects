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
    
    # Tabs for different views
    tab_realtime, tab_batch = st.tabs(["⚡ Real-Time Stream", "🏆 Batch Insights (Gold Layer)"])

    with tab_realtime:
        st.subheader("Live Incoming Data (Silver Stream)")
        csv_path = "realtime_sales_update.csv"
        
        if os.path.exists(csv_path):
            try:
                pd_df = pd.read_csv(csv_path, names=["event_timestamp", "category", "price", "user_id"])
                if not pd_df.empty:
                    # Metrics
                    col1, col2, col3 = st.columns(3)
                    with col1: st.metric("Live Revenue", f"${pd_df['price'].sum():,.2f}")
                    with col2: st.metric("Transactions", f"{len(pd_df):,}")
                    with col3: st.metric("Active Users", f"{pd_df['user_id'].nunique():,}")
                    
                    # Chart
                    fig = px.bar(pd_df.groupby("category")["price"].sum().reset_index(), x="category", y="price", title="Live Revenue by Category", template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Table
                    st.dataframe(pd_df.sort_values("event_timestamp", ascending=False).head(10), use_container_width=True)
                else:
                    st.info("Stream is empty.")
            except Exception as e:
                st.error(f"Error reading stream: {e}")
        else:
            st.warning("Waiting for streaming data (realtime_sales_update.csv)...")

    with tab_batch:
        st.subheader("Historical Aggregates (Gold Batch)")
        
        # Load Batch CSVs
        path_category = "/opt/bitnami/spark/app/gold_category_sales.csv"
        path_products = "/opt/bitnami/spark/app/gold_top_products.csv"
        path_users = "/opt/bitnami/spark/app/gold_user_stats.csv"
        
        # Check if running locally vs docker (paths might differ if not mounted exactly same)
        # For simplicity, we assume the dashboard container sees the same volume at /app or ..
        # Adjusting paths to relative if running from root, or absolute if in docker
        # The docker-compose mounts root to /app. The files are in root.
        
        # Correct Paths for Dashboard Container (mapped to /app)
        # Verify where the files actually are from dashboard perspective. 
        # Dashboard volume: - ..:/app
        # Gold batch output: /opt/bitnami/spark/app/ (which is mapped to root ..)
        # So files should be in /app/gold_category_sales.csv
        
        p1 = "gold_category_sales.csv"
        p2 = "gold_top_products.csv"
        p3 = "gold_user_stats.csv"

        col_b1, col_b2 = st.columns(2)
        
        with col_b1:
            st.markdown("### 📊 Hourly Sales")
            if os.path.exists(p1):
                df_cat = pd.read_csv(p1)
                fig_cat = px.bar(df_cat, x="hour_start", y="total_revenue", color="category", title="Hourly Revenue using Gold Layer", template="plotly_dark")
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Batch data (Sales) not found. Run Gold Batch job.")

        with col_b2:
            st.markdown("### 🛒 Top Products")
            if os.path.exists(p2):
                df_prod = pd.read_csv(p2)
                fig_prod = px.bar(df_prod.head(10), x="total_revenue", y="product_id", orientation='h', title="Top 10 Products", template="plotly_dark")
                fig_prod.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_prod, use_container_width=True)
            else:
                st.info("Batch data (Products) not found.")

        st.markdown("### 👥 Top Users")
        if os.path.exists(p3):
            df_users = pd.read_csv(p3)
            st.dataframe(df_users.head(10), use_container_width=True)
        else:
            st.info("Batch data (Users) not found.")

    # Aggressive auto-refresh for Real-Time feel
    time.sleep(refresh_rate)
    st.rerun()

if __name__ == "__main__":
    main()
