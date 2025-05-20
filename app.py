import streamlit as st
import pandas as pd
from utils import Ch_oi_oi_spurt, most_active_contracts, OI, liquidation_shift, sectorials, sectorial_stock
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import time
from tenacity import retry, wait_exponential, stop_after_attempt

# --- Rate Limiter Class ---
class RateLimiter:
    def __init__(self, max_calls=3, period=1):
        self.max_calls = max_calls
        self.period = timedelta(seconds=period)
        self.timestamps = []

    def __call__(self):
        now = datetime.now()
        self.timestamps = [t for t in self.timestamps if now - t < self.period]
        if len(self.timestamps) >= self.max_calls:
            sleep_time = (self.period - (now - self.timestamps[0])).total_seconds()
            time.sleep(max(sleep_time, 0))
        self.timestamps.append(now)

rate_limiter = RateLimiter(max_calls=3, period=1)

# --- Safe Kite API Wrapper ---
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def safe_kite_call(kite, func, *args, **kwargs):
    rate_limiter()  # Enforce rate limit
    try:
        return func(*args, **kwargs)
    except Exception as e:
        st.error(f"API Error: {str(e)}")
        raise

# Configuration
st.set_page_config(
    page_title="Trade Analyst",
    layout="wide",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Custom CSS (unchanged)
st.markdown("""
<style>
    .stDataFrame {
        width: 100%;
    }
    .metric-box {
        background-color: #0E1117;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 10px;
    }
    .positive-change {
        color: #00FF00;
    }
    .negative-change {
        color: #FF0000;
    }
</style>
""", unsafe_allow_html=True)

# --- Updated Cache Decorators with Rate Limiting ---
@st.cache_data(ttl=300, show_spinner=False)
def cached_oi_spurts():
    return Ch_oi_oi_spurt.get_oi_spurts()

@st.cache_data(ttl=300, show_spinner=False)
def cached_sectorials():
    return sectorials.sectorials()

@st.cache_data(ttl=300, show_spinner=False)
def cached_sector_data(_kite, sector):
    time.sleep(0.34)  # Rate limiting
    json_path = r"kite\data\sector_data.json"
    return sectorial_stock.get_sector_data(_kite, sector, json_path)

@st.cache_data(ttl=300, show_spinner=False)
def cached_active_contracts():
    return most_active_contracts.most_active_eq()

@st.cache_data(ttl=300, show_spinner=False)
def cached_option_data(index):
    return OI.get_data(index)

# --- Helper Functions ---
def gen_ses():
    key = open(r"kite\data\api.txt","r").read().split()
    kite = KiteConnect(api_key=key[0])
    kite.set_access_token(key[2])
    return kite

def display_metric(label, value, delta=None):
    st.metric(label=label, value=value, delta=delta)

def color_value(value):
    """Return colored HTML based on value sign"""
    if value > 0:
        return f'<span class="positive-change">+{value:.2f}%</span>'
    return f'<span class="negative-change">{value:.2f}%</span>'

# Main App
def main():
    kite = gen_ses()
    st.title("📈 Trade Analyst")
    
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    
    with st.sidebar:
        st.header("Navigation")
        menu = st.radio("Select Feature", [
            "Indices", "Overview", "Option Apex", 
            "Intraday Boost", "Market Pulse", "Market Overview"
        ])
        
        if st.button('🔄 Refresh Data', use_container_width=True):
            st.session_state.last_refresh = datetime.now()
            st.rerun()
        
        st.caption(f"Last refreshed: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if menu == "Intraday Boost":
        show_intraday_boost()
    elif menu == "Overview":
        show_overview(kite)
    elif menu == "Indices":
        show_indices(kite)
    elif menu == "Market Pulse":
        show_market_pulse(kite)
    elif menu == "Market Overview":
        show_market_overview(kite)  # Fixed: Removed underscore
    elif menu == "Option Apex":
        show_option_apex()

# Page functions
def show_intraday_boost():
    st.subheader("🔥 OI Spurts in Derivatives")
    with st.spinner("Loading OI spurts data..."):
        try:
            df = cached_oi_spurts()
            st.dataframe(df.style.format({
                '% Change': '{:.2f}%',
                'LTP': '₹{:.2f}'
            }))
        except Exception as e:
            st.error(f"Failed to load OI spurts: {str(e)}")

def show_overview(kite):
    st.subheader("📊 Nifty 50 Overview")
    
    with st.spinner("Loading Nifty 50 data..."):
        try:
            # Rate limited API call
            rate_limiter()
            df = cached_sector_data(kite, "NIFTY 50")
            
            if df.empty:
                st.warning("No data available for NIFTY 50")
                return
            
            # --- Real-time Index Data ---
            nifty_quote = safe_kite_call(kite, kite.quote, ["NSE:NIFTY 50"])
            nifty_data = nifty_quote["NSE:NIFTY 50"]
            
            # Calculate percentage change safely
            nifty_change_pct = 0.0
            if nifty_data['ohlc']['open'] != 0:  # Prevent division by zero
                nifty_change_pct = ((nifty_data['last_price'] - nifty_data['ohlc']['open']) / 
                                  nifty_data['ohlc']['open']) * 100
            
            # Top Metrics Row
            cols = st.columns(5)
            with cols[0]:
                display_metric("Nifty 50", f"{nifty_data['last_price']:,.2f}", 
                             f"{nifty_change_pct:.2f}%")
            with cols[1]:
                display_metric("Stocks Up", f"{len(df[df['% Change'] > 0])}/{len(df)}")
            with cols[2]:
                avg_gain = df[df['% Change'] > 0]['% Change'].mean()
                display_metric("Avg Gain", f"{avg_gain:.2f}%" if not pd.isna(avg_gain) else "0%")
            with cols[3]:
                avg_loss = df[df['% Change'] < 0]['% Change'].mean()
                display_metric("Avg Loss", f"{avg_loss:.2f}%" if not pd.isna(avg_loss) else "0%")
            with cols[4]:
                display_metric("Advance/Decline", 
                             f"{len(df[df['% Change'] > 0])}:{len(df[df['% Change'] < 0])}")
            
            # --- Enhanced Data Display ---
            tab1, tab2 = st.tabs(["Detailed View", "Performance Analysis"])
            
            with tab1:
                # Handle empty volume data case
                max_vol = int(df["Volume"].max()/100000) if not df["Volume"].empty else 100
                
                min_vol, max_vol = st.slider(
                    "Filter by Volume (Lakhs)",
                    min_value=0,
                    max_value=max_vol,
                    value=(0, max_vol))
                
                filtered_df = df[(df["Volume"] >= min_vol*100000) & 
                               (df["Volume"] <= max_vol*100000)]
                
                # Ensure we have columns before trying to display
                required_columns = ["Symbol", "Last Price", "% Change", "Volume", "Prev Close"]
                if all(col in df.columns for col in required_columns):
                    st.dataframe(
                        filtered_df.sort_values("% Change", ascending=False)[required_columns]
                        .style.format({
                            'Last Price': '₹{:.2f}',
                            '% Change': '{:.2f}%',
                            'Prev Close': '₹{:.2f}',
                            'Volume': '{:,}'
                        }).applymap(lambda x: 'color: green' if isinstance(x, (int, float)) and x > 0 else 'color: red', 
                                  subset=['% Change']),
                        height=600,
                        use_container_width=True
                    )
                else:
                    st.warning("Some required columns are missing in the data")
            
            with tab2:
                # Only plot if we have data
                if not df.empty and '% Change' in df.columns and 'Volume' in df.columns:
                    fig = px.scatter(
                        df,
                        x="% Change",
                        y="Volume",
                        color="% Change",
                        color_continuous_scale=["red", "white", "green"],
                        hover_name="Symbol",
                        size="Volume",
                        title="Stock Performance Distribution"
                    )
                    fig.update_layout(
                        plot_bgcolor="#0E1117",
                        paper_bgcolor="#0E1117",
                        font=dict(color="white")
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Insufficient data for visualization")
            
        except Exception as e:
            st.error(f"Failed to load Nifty 50 data: {str(e)}")

def show_market_pulse(kite):  # Removed underscore prefix for consistency
    st.subheader("🏢 Sectorial F&O Stocks Analysis")
    
    sector_indices = [
        "NIFTY AUTO", "NIFTY BANK", "NIFTY FMCG",
        "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
        "NIFTY PSU BANK", "NIFTY PRIVATE BANK", "NIFTY REALTY",
        "NIFTY HEALTHCARE INDEX", "NIFTY OIL & GAS", "NIFTY 50","NIFTY CONSUMER DURABLES" 
    ]
    
    # Two-column layout for better UI
    col1, col2 = st.columns([3, 2])
    
    with col1:
        selected_sector = st.selectbox("Select Sector Index", sector_indices)
        
        with st.spinner(f"Loading {selected_sector} data..."):
            try:
                # Rate limited API call
                rate_limiter()
                df = cached_sector_data(kite, selected_sector)
                
                if df.empty:
                    st.warning(f"No data available for {selected_sector}")
                    return
                
                # --- Enhanced Data Display ---
                st.success(f"Showing {len(df)} F&O stocks in {selected_sector}")
                
                # Sortable dataframe with performance filters
                min_change, max_change = st.slider(
                    "Filter by % Change",
                    min_value=-10.0,
                    max_value=10.0,
                    value=(-5.0, 5.0),
                    key=f"slider_{selected_sector}")
                
                filtered_df = df[(df["% Change"] >= min_change) & 
                               (df["% Change"] <= max_change)]
                
                st.dataframe(
                    filtered_df.style.format({
                        '% Change': '{:.2f}%',
                        'Last Price': '₹{:.2f}',
                        'Prev Close': '₹{:.2f}',
                        'R-Score' : '{:.2f}',
                        'Volume': '{:,}'
                    }).applymap(lambda x: 'color: green' if x > 0 else 'color: red', 
                              subset=['% Change']),
                    height=600,
                    use_container_width=True
                )
                
            except Exception as e:
                st.error(f"Failed to load sector data: {str(e)}")
    
    with col2:
        # --- Sector Performance Snapshot ---
        st.markdown("### 📊 Sector Quick Stats")
        
        try:
            # Get sector index performance
            sector_perf = cached_sectorials()
            if not sector_perf.empty:
                # Safely get current sector performance with error handling
                current_sector_perf = sector_perf[sector_perf["Index"] == selected_sector]
                
                if not current_sector_perf.empty:
                    current_sector_perf = current_sector_perf.iloc[0]
                    
                    st.metric(
                        f"{selected_sector} Performance",
                        f"{current_sector_perf['LTP']:,.2f}",
                        f"{current_sector_perf['% Change']:.2f}%",
                        delta_color="inverse"
                    )
                else:
                    st.warning(f"Performance data not available for {selected_sector}")
                
                # Only show stock stats if we have the sector data
                if not df.empty:
                    # Top 3 gainers stocks in sector
                    top_stocks = df.nlargest(3, "% Change")
                    st.markdown("#### 🚀 Top Gainers")
                    for _, row in top_stocks.iterrows():
                        st.markdown(
                            f"**{row['Symbol']}**: {color_value(row['% Change'])} "
                            f"(₹{row['Last Price']:,.2f})",
                            unsafe_allow_html=True
                        )

                    # Top 3 losers stock
                    low_stocks = df.nsmallest(3, "% Change")
                    st.markdown("#### 🔻 Top Losers")
                    for _, row in low_stocks.iterrows():
                        st.markdown(
                            f"**{row['Symbol']}**: {color_value(row['% Change'])} "
                            f"(₹{row['Last Price']:,.2f})",
                            unsafe_allow_html=True
                        )

                    # Volume leaders
                    vol_leaders = df.nlargest(3, "Volume")
                    st.markdown("#### 💰 Volume Leaders")
                    for _, row in vol_leaders.iterrows():
                        st.markdown(
                            f"**{row['Symbol']}**: {row['Volume']/100000:,.1f}L "
                            f"({color_value(row['% Change'])})",
                            unsafe_allow_html=True
                        )
                    
                    # Highest R-score stocks - NEW SECTION
                    if 'R-Score' in df.columns:
                        rscore_leaders = df.nlargest(3, "R-Score")
                        st.markdown("#### 📈 Highest R-Score")
                        for _, row in rscore_leaders.iterrows():
                            st.markdown(
                                f"**{row['Symbol']}**: {row['R-Score']:.2f} "
                                f"({color_value(row['% Change'])})",
                                unsafe_allow_html=True
                            )
                    else:
                        st.markdown("#### 📈 R-Score Data Not Available")

        except Exception as e:
            st.warning(f"Couldn't load sector stats: {str(e)}")

def show_indices(_kite):  # Note the underscore prefix
    st.subheader("💥 All Sectorial Index Data")
    with st.spinner("Loading sectorial data..."):
        try:
            kite =_kite
            df = cached_sectorials()
            if df.empty:
                st.warning("No sectorial data available")
                return
            
            st.dataframe(df.style.format({
                '% Change': '{:.2f}%',
                'LTP': '{:.2f}',
                'net_change': '{:.2f}',
            }))

            if "% Change" in df.columns:
                df_sort = df.sort_values(by="% Change", ascending=False)

                fig = px.bar(
                    df_sort,
                    x="Index",
                    y="% Change",
                    color="% Change",
                    color_continuous_scale=[(0.0, "red"), (0.5, "lightblue"), (1.0, "blue")],
                    title="📊 Sectorial Performance",
                )

                fig.update_layout(
                    plot_bgcolor="#0E1117",
                    paper_bgcolor="#0E1117",
                    font=dict(color="#FFFFFF"),
                    xaxis=dict(title="Index", tickangle=-45),
                    yaxis=dict(title="% Change"),
                )

                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Failed to load sectorial data: {str(e)}")

def show_market_overview(kite):
    """Real-time market overview with rate-limited API calls"""
    st.subheader("🌐 Live Market Overview")
    
    try:
        # Get index data in one batch
        indices = ["NSE:NIFTY 50", "NSE:NIFTY BANK", "NSE:INDIA VIX"]
        quote_data = safe_kite_call(kite, kite.quote, indices)
        
        # Extract values
        nifty_ltp = quote_data["NSE:NIFTY 50"]["last_price"]
        banknifty_ltp = quote_data["NSE:NIFTY BANK"]["last_price"]
        vix = quote_data["NSE:INDIA VIX"]["last_price"]
        
        # Get index performance
        nifty_change = ((quote_data["NSE:NIFTY 50"]["last_price"] - 
                        quote_data["NSE:NIFTY 50"]["ohlc"]["open"]) / 
                       quote_data["NSE:NIFTY 50"]["ohlc"]["open"]) * 100
        banknifty_change = ((quote_data["NSE:NIFTY BANK"]["last_price"] - 
                           quote_data["NSE:NIFTY BANK"]["ohlc"]["open"]) / 
                          quote_data["NSE:NIFTY BANK"]["ohlc"]["open"]) * 100
        vix_change = ((quote_data["NSE:INDIA VIX"]["last_price"] - 
                      quote_data["NSE:INDIA VIX"]["ohlc"]["open"]) / 
                     quote_data["NSE:INDIA VIX"]["ohlc"]["open"]) * 100
        
        # Display metrics
        cols = st.columns(3)
        with cols[0]:
            st.metric("Nifty 50", f"{nifty_ltp:,.2f}", 
                     f"{nifty_change:.2f}%", delta_color="inverse")
        with cols[1]:
            st.metric("Bank Nifty", f"{banknifty_ltp:,.2f}", 
                     f"{banknifty_change:.2f}%", delta_color="inverse")
        with cols[2]:
            st.metric("India VIX", f"{vix:.2f}", 
                     f"{vix_change:.2f}%", delta_color="inverse")
        
        # Market Breadth (using cached sectorials)
        df_sectors = cached_sectorials()
        advancing = len(df_sectors[df_sectors["% Change"] > 0])
        declining = len(df_sectors[df_sectors["% Change"] < 0])
        
        st.markdown("### 📊 Market Breadth")
        breadth_cols = st.columns(4)
        with breadth_cols[0]:
            st.metric("Advancing Sectors", advancing, 
                     f"{advancing/len(df_sectors)*100:.1f}%")
        with breadth_cols[1]:
            st.metric("Declining Sectors", declining, 
                     f"{declining/len(df_sectors)*100:.1f}%")
        
        # Top 3 Sectors
        top_sectors = df_sectors.nlargest(3, "% Change")
        bottom_sectors = df_sectors.nsmallest(3, "% Change")
        
        with st.expander("🏆 Top Performing Sectors", expanded=True):
            for _, row in top_sectors.iterrows():
                st.markdown(f"**{row['Index']}**: {color_value(row['% Change'])}", 
                           unsafe_allow_html=True)
        
        with st.expander("📉 Weakest Sectors", expanded=False):
            for _, row in bottom_sectors.iterrows():
                st.markdown(f"**{row['Index']}**: {color_value(row['% Change'])}", 
                           unsafe_allow_html=True)
        
        # Visualize sector performance
        st.markdown("### 📈 Sector Performance Heatmap")
        fig = px.imshow(
            df_sectors.set_index("Index")[["% Change"]].T,
            color_continuous_scale="RdYlGn",
            aspect="auto"
        )
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Failed to load market overview: {str(e)}")


def show_option_apex():
    st.subheader("📊 NIFTY Option Chain Overview")
    
    with st.spinner("Loading option chain data..."):
        try:
            df_oi = cached_option_data("NIFTY")
            if df_oi.empty:
                st.warning("No data available for NIFTY options")
                return
                
            results = OI.analyze_option_chain(df_oi)
            
            # Create tabs for better organization
            tab1, tab2, tab3, tab4 = st.tabs(["Overview", "OI Analysis", "IV Analysis", "Signals"])
            
            with tab1:
                st.markdown("#### Key Metrics")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    display_metric(
                        "Underlying", 
                        f"{results['Filtered Data']['CE.underlyingValue'].iloc[0]:.2f}"
                    )
                with col2:
                    display_metric(
                        "Strong Support", 
                        results["Filtered Data"].loc[results["Filtered Data"]["PE.openInterest"].idxmax(), "strikePrice"]
                    )
                with col3:
                    display_metric(
                        "Strong Resistance", 
                        results["Filtered Data"].loc[results["Filtered Data"]["CE.openInterest"].idxmax(), "strikePrice"]
                    )
                with col4:
                    pcr = results["Filtered Data"]["PE.openInterest"].sum() / results["Filtered Data"]["CE.openInterest"].sum()
                    display_metric(
                        "Put-Call Ratio", 
                        f"{pcr:.2f}",
                        "Bullish" if pcr > 1 else "Bearish"
                    )
                
                st.markdown("""
                **PCR Interpretation**:  
                - <1: Bearish sentiment  
                - >1: Bullish sentiment  
                - Extreme values (>1.5 or <0.5) may indicate reversals
                """)
            
            with tab2:
                st.markdown("#### 🔼 Top PE OI Changes")
                pe_latest = results["Top PE OI Change Latest"][["expiryDate", "strikePrice", "PE.openInterest", "PE_OI_Change_%"]]
                pe_overall = results["Top PE OI Change Overall"][["expiryDate", "strikePrice", "PE.openInterest", "PE_OI_Change_%"]]
                
                pe_combined = pd.concat([pe_latest, pe_overall]).drop_duplicates()
                pe_combined["expiryDate"] = pd.to_datetime(pe_combined["expiryDate"]).dt.strftime("%d-%b-%Y")
                
                st.dataframe(
                    pe_combined.style.format({
                        "PE_OI_Change_%": "{:.2f}%",
                        "PE.openInterest": "{:,}"
                    })
                )
                
                st.markdown("#### 🔽 Top CE OI Changes")
                ce_latest = results["Top CE OI Change Latest"][["expiryDate", "strikePrice", "CE.openInterest", "CE_OI_Change_%"]]
                ce_overall = results["Top CE OI Change Overall"][["expiryDate", "strikePrice", "CE.openInterest", "CE_OI_Change_%"]]
                
                ce_combined = pd.concat([ce_latest, ce_overall]).drop_duplicates()
                ce_combined["expiryDate"] = pd.to_datetime(ce_combined["expiryDate"]).dt.strftime("%d-%b-%Y")
                
                st.dataframe(
                    ce_combined.style.format({
                        "CE_OI_Change_%": "{:.2f}%",
                        "CE.openInterest": "{:,}"
                    })
                )
            
            with tab3:
                st.markdown("#### ⚖️ IV Skew Analysis")
                iv_latest = results["Top IV Skew Latest"][["expiryDate", "strikePrice", "IV_Skew"]]
                iv_overall = results["Top IV Skew Overall"][["expiryDate", "strikePrice", "IV_Skew"]]
                
                iv_combined = pd.concat([iv_latest, iv_overall]).drop_duplicates()
                iv_combined["expiryDate"] = pd.to_datetime(iv_combined["expiryDate"]).dt.strftime("%d-%b-%Y")
                
                st.dataframe(
                    iv_combined.style.format({
                        "IV_Skew": "{:.2f}"
                    })
                )
            
            with tab4:
                st.markdown("#### 💡 Option Signals")
                signal_df = liquidation_shift.get_liquidation_zones(df_oi).dropna(subset=['action'])
                
                ce_signals = signal_df[signal_df['type'] == 'CE']
                pe_signals = signal_df[signal_df['type'] == 'PE']
                conflict_signals = signal_df[signal_df['type'] == 'CONFLICT']
                
                if not ce_signals.empty:
                    with st.expander("🔴 CE Zone Signals (Resistance)", expanded=True):
                        st.dataframe(ce_signals.style.format({
                            'Change_in_OI': '{:.2f}%',
                            'strike': '{:.0f}'
                        }))
                
                if not pe_signals.empty:
                    with st.expander("🟢 PE Zone Signals (Support)", expanded=True):
                        st.dataframe(pe_signals.style.format({
                            'Change_in_OI': '{:.2f}%',
                            'strike': '{:.0f}'
                        }))
                
                if not conflict_signals.empty:
                    with st.expander("⚠️ Conflict Zones (Potential Reversals)", expanded=True):
                        st.dataframe(conflict_signals.style.format({
                            'strike': '{:.0f}'
                        }))
                else:
                    st.info("No conflict zones detected")
        
        except Exception as e:
            st.error(f"Failed to analyze option chain: {str(e)}")

if __name__ == "__main__":
    main()