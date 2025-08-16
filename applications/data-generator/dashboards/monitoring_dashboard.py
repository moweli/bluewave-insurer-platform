"""Streamlit monitoring dashboard for insurance data streaming."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import asyncio
import json
from typing import Dict, Any, List
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.streaming.consumer import EventHubConsumer, ClaimEventProcessor
from src.utils.uk_postcode_profiler import UKPostcodeProfiler
from config.settings import get_settings


# Page configuration
st.set_page_config(
    page_title="BlueWave Insurance Monitor",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        margin: 5px;
    }
    .fraud-alert {
        background-color: #ff4b4b;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .success-metric {
        background-color: #00cc88;
        color: white;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)


class DashboardData:
    """Container for dashboard data."""
    
    def __init__(self):
        self.claims = []
        self.policies = []
        self.fraud_cases = []
        self.audit_events = []
        self.metrics = {
            'total_claims': 0,
            'total_policies': 0,
            'total_fraud_cases': 0,
            'total_claim_amount': 0,
            'average_fraud_score': 0,
            'high_risk_claims': 0,
            'claims_by_type': {},
            'claims_by_status': {},
            'geographic_distribution': {},
            'hourly_volume': []
        }
        self.postcode_profiler = UKPostcodeProfiler()


@st.cache_data(ttl=60)
def load_sample_data() -> DashboardData:
    """Load sample data for demonstration."""
    data = DashboardData()
    
    # Generate sample metrics
    data.metrics['total_claims'] = 1247
    data.metrics['total_policies'] = 3892
    data.metrics['total_fraud_cases'] = 62
    data.metrics['total_claim_amount'] = 4_567_890
    data.metrics['average_fraud_score'] = 0.12
    data.metrics['high_risk_claims'] = 89
    
    # Claims by type
    data.metrics['claims_by_type'] = {
        'Collision': 456,
        'Theft': 234,
        'Fire': 123,
        'Flood': 98,
        'Storm': 187,
        'Liability': 149
    }
    
    # Claims by status
    data.metrics['claims_by_status'] = {
        'Submitted': 234,
        'Under Review': 345,
        'Investigation': 89,
        'Approved': 456,
        'Rejected': 67,
        'Paid': 56
    }
    
    # Geographic distribution
    data.metrics['geographic_distribution'] = {
        'London': 456,
        'Birmingham': 234,
        'Manchester': 187,
        'Glasgow': 123,
        'Liverpool': 98,
        'Edinburgh': 76,
        'Bristol': 73
    }
    
    return data


def create_header():
    """Create dashboard header."""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        st.image("https://via.placeholder.com/150x50/4A90E2/FFFFFF?text=BlueWave", width=150)
    
    with col2:
        st.title("🌊 Insurance Data Monitoring Dashboard")
        st.caption(f"Real-time monitoring • Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    with col3:
        status = st.empty()
        if st.session_state.get('streaming_active', False):
            status.success("🟢 Streaming Active")
        else:
            status.error("🔴 Streaming Inactive")


def create_metrics_row(data: DashboardData):
    """Create key metrics row."""
    st.subheader("📊 Key Metrics")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            "Total Claims",
            f"{data.metrics['total_claims']:,}",
            delta="+12% today"
        )
    
    with col2:
        st.metric(
            "Active Policies",
            f"{data.metrics['total_policies']:,}",
            delta="+5% this week"
        )
    
    with col3:
        st.metric(
            "Fraud Cases",
            data.metrics['total_fraud_cases'],
            delta="+3 today",
            delta_color="inverse"
        )
    
    with col4:
        st.metric(
            "Total Claim Value",
            f"£{data.metrics['total_claim_amount']:,.0f}",
            delta="+£234,567"
        )
    
    with col5:
        avg_fraud = data.metrics['average_fraud_score']
        st.metric(
            "Avg Fraud Score",
            f"{avg_fraud:.2%}",
            delta="+0.02%",
            delta_color="inverse" if avg_fraud > 0.15 else "normal"
        )
    
    with col6:
        st.metric(
            "High Risk Claims",
            data.metrics['high_risk_claims'],
            delta="+5",
            delta_color="inverse"
        )


def create_fraud_alerts(data: DashboardData):
    """Create fraud alerts section."""
    st.subheader("🚨 Fraud Alerts")
    
    # Sample fraud alerts
    alerts = [
        {
            'severity': 'HIGH',
            'time': '2 minutes ago',
            'message': 'Fraud ring detected: 4 related claims from postcodes B15-B18',
            'score': 0.92
        },
        {
            'severity': 'MEDIUM',
            'time': '15 minutes ago',
            'message': 'Suspicious claim: £45,000 motor claim filed 5 days after policy start',
            'score': 0.78
        },
        {
            'severity': 'HIGH',
            'time': '1 hour ago',
            'message': 'Repeat offender detected: 3rd claim in 6 months from policyholder #PH-2024-0892',
            'score': 0.85
        }
    ]
    
    for alert in alerts[:3]:  # Show top 3 alerts
        severity_color = "#ff4b4b" if alert['severity'] == 'HIGH' else "#ffa500"
        st.markdown(
            f"""
            <div style="background-color: {severity_color}; color: white; padding: 10px; border-radius: 5px; margin: 5px 0;">
                <strong>{alert['severity']} RISK</strong> • {alert['time']} • Score: {alert['score']:.2f}<br>
                {alert['message']}
            </div>
            """,
            unsafe_allow_html=True
        )


def create_charts_row(data: DashboardData):
    """Create main charts row."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Claims by Type")
        fig_claims = px.pie(
            values=list(data.metrics['claims_by_type'].values()),
            names=list(data.metrics['claims_by_type'].keys()),
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_claims.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_claims, use_container_width=True)
    
    with col2:
        st.subheader("Claims by Status")
        fig_status = px.bar(
            x=list(data.metrics['claims_by_status'].keys()),
            y=list(data.metrics['claims_by_status'].values()),
            color=list(data.metrics['claims_by_status'].keys()),
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_status.update_layout(showlegend=False)
        st.plotly_chart(fig_status, use_container_width=True)


def create_geographic_visualization(data: DashboardData):
    """Create geographic visualization."""
    st.subheader("🗺️ Geographic Distribution")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Create UK map
        m = folium.Map(
            location=[54.5, -3.5],  # Center of UK
            zoom_start=6,
            tiles='OpenStreetMap'
        )
        
        # Add markers for major cities
        city_coords = {
            'London': (51.5074, -0.1278),
            'Birmingham': (52.4862, -1.8904),
            'Manchester': (53.4808, -2.2426),
            'Glasgow': (55.8642, -4.2518),
            'Liverpool': (53.4084, -2.9916),
            'Edinburgh': (55.9533, -3.1883),
            'Bristol': (51.4545, -2.5879)
        }
        
        for city, count in data.metrics['geographic_distribution'].items():
            if city in city_coords:
                lat, lon = city_coords[city]
                radius = min(count / 10, 30)  # Scale radius
                color = '#ff4b4b' if count > 300 else '#ffa500' if count > 150 else '#00cc88'
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    popup=f"{city}: {count} claims",
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.6
                ).add_to(m)
        
        st_folium(m, height=400, width=None, key="uk_map")
    
    with col2:
        st.subheader("Top Risk Areas")
        risk_df = pd.DataFrame({
            'Area': list(data.metrics['geographic_distribution'].keys()),
            'Claims': list(data.metrics['geographic_distribution'].values())
        }).sort_values('Claims', ascending=False)
        
        st.dataframe(risk_df, use_container_width=True, hide_index=True)


def create_time_series_chart(data: DashboardData):
    """Create time series chart."""
    st.subheader("📈 Claims Volume Over Time")
    
    # Generate sample time series data
    hours = pd.date_range(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        freq='H'
    )
    
    claims_volume = [50 + i * 2 + (i % 5) * 10 for i in range(len(hours))]
    fraud_volume = [2 + (i % 7) for i in range(len(hours))]
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Claims Volume', 'Fraud Detections'),
        vertical_spacing=0.1
    )
    
    # Claims volume
    fig.add_trace(
        go.Scatter(
            x=hours,
            y=claims_volume,
            mode='lines+markers',
            name='Claims',
            line=dict(color='#4A90E2', width=2)
        ),
        row=1, col=1
    )
    
    # Fraud detections
    fig.add_trace(
        go.Bar(
            x=hours,
            y=fraud_volume,
            name='Fraud Cases',
            marker_color='#ff4b4b'
        ),
        row=2, col=1
    )
    
    fig.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


def create_recent_claims_table(data: DashboardData):
    """Create recent claims table."""
    st.subheader("📋 Recent Claims")
    
    # Sample recent claims data
    recent_claims = pd.DataFrame({
        'Claim ID': ['CLM-202411-45678', 'CLM-202411-45679', 'CLM-202411-45680', 
                     'CLM-202411-45681', 'CLM-202411-45682'],
        'Type': ['Collision', 'Theft', 'Fire', 'Flood', 'Liability'],
        'Amount': ['£12,500', '£8,900', '£45,000', '£23,400', '£5,600'],
        'Fraud Score': [0.12, 0.78, 0.45, 0.23, 0.89],
        'Status': ['Under Review', 'Investigation', 'Approved', 'Submitted', 'Flagged'],
        'Location': ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool'],
        'Time': ['2 min ago', '5 min ago', '12 min ago', '18 min ago', '25 min ago']
    })
    
    # Style the dataframe
    def color_fraud_score(val):
        if val > 0.7:
            color = '#ff4b4b'
        elif val > 0.4:
            color = '#ffa500'
        else:
            color = '#00cc88'
        return f'color: {color}; font-weight: bold'
    
    styled_df = recent_claims.style.applymap(
        color_fraud_score,
        subset=['Fraud Score']
    )
    
    st.dataframe(styled_df, use_container_width=True, hide_index=True)


def create_sidebar(data: DashboardData):
    """Create sidebar with controls."""
    st.sidebar.header("⚙️ Dashboard Controls")
    
    # Streaming controls
    st.sidebar.subheader("Streaming")
    if st.sidebar.button("Start Streaming", type="primary"):
        st.session_state['streaming_active'] = True
        st.rerun()
    
    if st.sidebar.button("Stop Streaming", type="secondary"):
        st.session_state['streaming_active'] = False
        st.rerun()
    
    # Filters
    st.sidebar.subheader("Filters")
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(datetime.now() - timedelta(days=7), datetime.now()),
        max_value=datetime.now()
    )
    
    claim_types = st.sidebar.multiselect(
        "Claim Types",
        options=['Collision', 'Theft', 'Fire', 'Flood', 'Storm', 'Liability', 'Cyber', 'Other'],
        default=['Collision', 'Theft', 'Fire']
    )
    
    risk_threshold = st.sidebar.slider(
        "Fraud Score Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.7,
        step=0.05
    )
    
    regions = st.sidebar.multiselect(
        "Regions",
        options=['London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool', 'Edinburgh', 'Bristol'],
        default=['London', 'Birmingham', 'Manchester']
    )
    
    # Settings
    st.sidebar.subheader("Settings")
    
    refresh_rate = st.sidebar.selectbox(
        "Refresh Rate",
        options=['Real-time', '5 seconds', '10 seconds', '30 seconds', '1 minute'],
        index=1
    )
    
    show_alerts = st.sidebar.checkbox("Show Fraud Alerts", value=True)
    show_map = st.sidebar.checkbox("Show Geographic Map", value=True)
    
    # Export
    st.sidebar.subheader("Export")
    if st.sidebar.button("📊 Export Dashboard Report"):
        st.sidebar.success("Report exported successfully!")
    
    if st.sidebar.button("📥 Download Raw Data"):
        st.sidebar.info("Preparing data download...")


def main():
    """Main dashboard application."""
    # Initialize session state
    if 'streaming_active' not in st.session_state:
        st.session_state['streaming_active'] = False
    
    # Load data
    data = load_sample_data()
    
    # Create dashboard layout
    create_header()
    
    # Metrics
    create_metrics_row(data)
    
    # Fraud alerts
    if st.session_state.get('show_alerts', True):
        create_fraud_alerts(data)
    
    # Main visualizations
    st.divider()
    create_charts_row(data)
    
    st.divider()
    create_time_series_chart(data)
    
    # Geographic visualization
    if st.session_state.get('show_map', True):
        st.divider()
        create_geographic_visualization(data)
    
    # Recent claims table
    st.divider()
    create_recent_claims_table(data)
    
    # Sidebar
    create_sidebar(data)
    
    # Auto-refresh
    if st.session_state.get('streaming_active', False):
        st.empty()
        import time
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()