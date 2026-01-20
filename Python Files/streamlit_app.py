import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from groq import Groq
import sqlite3
import time

# Page config
st.set_page_config(
    page_title="NYC Taxi Analytics Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with enhanced styling
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background-color: #0E1117;
    }

    /* Header styling */
    .main-header {
        font-size: 3.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 25%, #FF6B6B 50%, #4ECDC4 75%, #45B7D1 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1.5rem;
        margin-bottom: 2rem;
        position: relative;
        letter-spacing: -0.5px;
    }

    .main-header::after {
        content: '';
        position: absolute;
        bottom: 0;
        left: 25%;
        width: 50%;
        height: 4px;
        background: linear-gradient(90deg, #FFD700, #FF6B6B, #4ECDC4);
        border-radius: 2px;
    }

    /* Card styling */
    .metric-card {
        background: linear-gradient(135deg, #1a1d29 0%, #232735 100%);
        padding: 1.5rem;
        border-radius: 16px;
        border: 1px solid #2d3746;
        color: white;
        text-align: center;
        transition: all 0.3s ease;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
    }

    .metric-card:hover {
        transform: translateY(-5px);
        border-color: #FFD700;
        box-shadow: 0 8px 30px rgba(255, 215, 0, 0.2);
    }

    /* Sidebar styling */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #1a1d29 0%, #0E1117 100%);
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background-color: #1a1d29;
        border-radius: 12px;
        padding: 4px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 60px;
        border-radius: 8px;
        padding: 0 24px;
        font-weight: 600;
        color: #94a3b8;
        transition: all 0.3s ease;
    }

    .stTabs [aria-selected="true"] {
        background-color: #FFD700 !important;
        color: #0E1117 !important;
        font-weight: 700;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(255, 215, 0, 0.1);
    }

    /* Button styling */
    .stButton button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 12px 24px;
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.3s ease;
    }

    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.4);
    }

    /* Input fields */
    .stTextArea textarea, .stSelectbox select {
        background-color: #1a1d29 !important;
        border: 1px solid #2d3746 !important;
        color: white !important;
        border-radius: 10px !important;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #1a1d29 !important;
        border-radius: 10px !important;
        border: 1px solid #2d3746 !important;
        color: white !important;
    }

    /* Success/Error/Info/Warning boxes */
    .stAlert {
        border-radius: 12px !important;
        border: none !important;
    }

    .stSuccess {
        background: linear-gradient(135deg, #10B98120 0%, #10B98140 100%) !important;
        border-left: 4px solid #10B981 !important;
    }

    .stError {
        background: linear-gradient(135deg, #EF444420 0%, #EF444440 100%) !important;
        border-left: 4px solid #EF4444 !important;
    }

    .stInfo {
        background: linear-gradient(135deg, #3B82F620 0%, #3B82F640 100%) !important;
        border-left: 4px solid #3B82F6 !important;
    }

    .stWarning {
        background: linear-gradient(135deg, #F59E0B20 0%, #F59E0B40 100%) !important;
        border-left: 4px solid #F59E0B !important;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #FFD700, #FF6B6B) !important;
    }

    /* Dataframe styling */
    .dataframe {
        background-color: #1a1d29 !important;
        border-radius: 12px !important;
        border: 1px solid #2d3746 !important;
    }

    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    ::-webkit-scrollbar-track {
        background: #1a1d29;
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb {
        background: linear-gradient(180deg, #FFD700, #FF6B6B);
        border-radius: 4px;
    }

    /* Card glow effect */
    .glow-card {
        position: relative;
        overflow: hidden;
    }

    .glow-card::before {
        content: '';
        position: absolute;
        top: -2px;
        left: -2px;
        right: -2px;
        bottom: -2px;
        background: linear-gradient(45deg, #FFD700, #FF6B6B, #4ECDC4, #45B7D1);
        z-index: -1;
        filter: blur(10px);
        opacity: 0.3;
        border-radius: 18px;
    }

    /* Badge styling */
    .badge {
        display: inline-block;
        padding: 4px 12px;
        background: linear-gradient(135deg, #FFD700, #FFA500);
        color: #0E1117;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 700;
        margin: 0 4px;
    }

    /* Divider styling */
    .divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, #FFD700, transparent);
        margin: 2rem 0;
    }

    /* Radio button styling */
    [data-testid="stVerticalBlock"] [data-testid="stRadio"] {
        background: #1a1d29;
        padding: 16px;
        border-radius: 12px;
        border: 1px solid #2d3746;
    }

    /* Hover effects for charts */
    .js-plotly-plot .plotly .modebar {
        background: transparent !important;
    }

    /* Animation for loading */
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }

    .pulse {
        animation: pulse 2s infinite;
    }
</style>
""", unsafe_allow_html=True)


# Initialize Groq client
@st.cache_resource
def init_groq():
    return Groq(api_key='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')


client = init_groq()


# SQL Connection
@st.cache_resource
def get_db_connection():
    return sqlite3.connect('taxi_analytics.db', check_same_thread=False)


conn = get_db_connection()


# Load data with caching
@st.cache_data
def load_data():
    df = pd.read_parquet('cleaned_trips.parquet')
    return df


@st.cache_data
def load_kpis():
    df = load_data()
    return {
        'total_trips': len(df),
        'total_revenue': df['total_amount'].sum(),
        'avg_fare': df['fare_amount'].mean(),
        'avg_distance': df['trip_distance'].mean(),
        'avg_tip_pct': df['tip_percentage'].mean(),
        'peak_trips': df[df['is_peak']].shape[0],
        'weekend_trips': df[df['is_weekend']].shape[0]
    }


# Execute SQL query
def execute_query(query):
    try:
        result = pd.read_sql_query(query, conn)
        return result, None
    except Exception as e:
        return None, str(e)


# Header with enhanced design
st.markdown('<div class="main-header">🚕 NYC Taxi Analytics & GenAI Insights Platform</div>', unsafe_allow_html=True)

# Sidebar with enhanced design
with st.sidebar:
    st.markdown("""
    <div style='text-align: center; margin-bottom: 2rem;'>
        <div style='font-size: 2.5rem; margin-bottom: 1rem;'>🚕</div>
        <h1 style='color: #FFD700; font-size: 1.8rem; font-weight: 800;'>NYC Taxi Analytics</h1>
        <p style='color: #94a3b8; font-size: 0.9rem;'>Real-time Insights & AI-Powered Analysis</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    st.markdown("### 🧭 Navigation")
    page = st.radio(
        "",
        ["📊 Dashboard", "🤖 GenAI Assistant", "💾 SQL Query Lab", "📈 Deep Dive Analytics", "💡 Insights"],
        label_visibility="collapsed"
    )

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Dataset info in a beautiful card
    st.markdown("### 📊 Dataset Overview")
    st.markdown("""
    <div class="metric-card" style="text-align: left;">
        <div style="display: flex; align-items: center; margin-bottom: 1rem;">
            <span style="font-size: 1.5rem; margin-right: 10px;">📅</span>
            <div>
                <div style="font-size: 0.9rem; color: #94a3b8;">Period</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: #FFD700;">January 2015</div>
            </div>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 1rem;">
            <span style="font-size: 1.5rem; margin-right: 10px;">📊</span>
            <div>
                <div style="font-size: 0.9rem; color: #94a3b8;">Records</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: #4ECDC4;">12.6M Trips</div>
            </div>
        </div>
        <div style="display: flex; align-items: center;">
            <span style="font-size: 1.5rem; margin-right: 10px;">💰</span>
            <div>
                <div style="font-size: 0.9rem; color: #94a3b8;">Revenue</div>
                <div style="font-size: 1.1rem; font-weight: 600; color: #FF6B6B;">$190.5M</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if page == "💾 SQL Query Lab":
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown("### 📋 Table Schema")
        with st.expander("🔍 View Columns", expanded=False):
            st.code("""
            hour, day_of_week, day
            passenger_count, trip_distance
            fare_amount, tip_amount
            total_amount, tip_percentage
            is_peak, is_weekend
            payment_type, VendorID
            tpep_pickup_datetime
            """)

# Load data
df = load_data()
kpis = load_kpis()

# PAGE 1: DASHBOARD
if page == "📊 Dashboard":
    st.header("📊 Executive Dashboard")
    st.markdown('<p style="color: #94a3b8; font-size: 1.1rem;">January 2015 Performance Overview</p>',
                unsafe_allow_html=True)

    # KPI Cards with enhanced design
    st.markdown("### 🎯 Key Performance Indicators")
    col1, col2, col3, col4, col5 = st.columns(5)

    kpi_icons = ["📊", "💰", "🎯", "📈", "⏰"]
    kpi_colors = ["#FFD700", "#4ECDC4", "#FF6B6B", "#45B7D1", "#FFA500"]

    kpi_data = [
        ("Total Trips", f"{kpis['total_trips']:,}", "+12.6M", kpi_icons[0], kpi_colors[0]),
        ("Total Revenue", f"${kpis['total_revenue'] / 1e6:.1f}M", "+5.2%", kpi_icons[1], kpi_colors[1]),
        ("Avg Fare", f"${kpis['avg_fare']:.2f}", "+$0.45", kpi_icons[2], kpi_colors[2]),
        ("Avg Tip %", f"{kpis['avg_tip_pct']:.1f}%", "+2.1%", kpi_icons[3], kpi_colors[3]),
        ("Peak Trips", f"{kpis['peak_trips'] / 1e6:.1f}M", "25.8%", kpi_icons[4], kpi_colors[4])
    ]

    cols = [col1, col2, col3, col4, col5]
    for i, (title, value, delta, icon, color) in enumerate(kpi_data):
        with cols[i]:
            st.markdown(f"""
            <div class="metric-card glow-card" style="border-color: {color}40;">
                <div style="font-size: 2rem; margin-bottom: 10px;">{icon}</div>
                <div style="font-size: 0.9rem; color: #94a3b8; margin-bottom: 8px;">{title}</div>
                <div style="font-size: 1.8rem; font-weight: 700; margin-bottom: 5px; color: {color};">{value}</div>
                <div style="font-size: 0.85rem; color: #10B981; font-weight: 600;">{delta}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Charts Row 1
    st.markdown("### 📈 Demand & Revenue Analysis")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### 📊 Hourly Demand Pattern")
        hourly_trips = df.groupby('hour').size().reset_index(name='trips')
        fig = px.bar(hourly_trips, x='hour', y='trips',
                     title='',
                     labels={'hour': 'Hour of Day', 'trips': 'Number of Trips'},
                     color='trips', color_continuous_scale='Viridis')
        fig.update_layout(
            height=400,
            plot_bgcolor='#1a1d29',
            paper_bgcolor='#1a1d29',
            font_color='white',
            hovermode='x unified'
        )
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with col2:
        st.markdown("##### 💰 Daily Revenue Trend")
        daily_revenue = df.groupby('day')['total_amount'].sum().reset_index()
        fig = px.area(daily_revenue, x='day', y='total_amount',
                      title='',
                      labels={'day': 'Day of Month', 'total_amount': 'Revenue ($)'},
                      markers=True)
        fig.update_traces(line_color='#00CC96', line_width=3)
        fig.update_layout(
            height=400,
            plot_bgcolor='#1a1d29',
            paper_bgcolor='#1a1d29',
            font_color='white',
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    # Charts Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### 🎯 Peak vs Off-Peak Distribution")
        peak_data = df.groupby('is_peak').size().reset_index(name='trips')
        peak_data['period'] = peak_data['is_peak'].map({True: 'Peak Hours', False: 'Off-Peak'})
        fig = px.pie(peak_data, values='trips', names='period',
                     title='',
                     color_discrete_sequence=['#FF6692', '#19D3F3'],
                     hole=0.4)
        fig.update_layout(
            height=400,
            plot_bgcolor='#1a1d29',
            paper_bgcolor='#1a1d29',
            font_color='white',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with col2:
        st.markdown("##### 📅 Weekday vs Weekend Analysis")
        dow_trips = df.groupby('day_of_week').size().reset_index(name='trips')
        dow_trips['day_name'] = dow_trips['day_of_week'].map({
            1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'
        })
        fig = px.bar(dow_trips, x='day_name', y='trips',
                     title='',
                     color='trips',
                     color_continuous_scale='Plasma')
        fig.update_layout(
            height=400,
            plot_bgcolor='#1a1d29',
            paper_bgcolor='#1a1d29',
            font_color='white',
            hovermode='x unified'
        )
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True, theme=None)

# PAGE 2: GENAI ASSISTANT
elif page == "🤖 GenAI Assistant":
    st.header("🤖 GenAI Urban Mobility Assistant")
    st.markdown(
        '<p style="color: #94a3b8; font-size: 1.1rem;">Ask natural language questions about the NYC taxi data!</p>',
        unsafe_allow_html=True)

    # Create data context
    data_context = f"""
    NYC Yellow Taxi Data - January 2015
    - Total Trips: {kpis['total_trips']:,}
    - Total Revenue: ${kpis['total_revenue']:,.2f}
    - Average Fare: ${kpis['avg_fare']:.2f}
    - Average Tip %: {kpis['avg_tip_pct']:.2f}%
    - Peak Hour Trips: {kpis['peak_trips']:,}
    - Weekend Trips: {kpis['weekend_trips']:,}
    """

    # Predefined questions in beautiful cards
    st.markdown("### 💡 Quick Questions")
    quick_questions = [
        {"icon": "⏰", "text": "What were the busiest hours?", "color": "#FFD700"},
        {"icon": "💰", "text": "When should drivers work to maximize earnings?", "color": "#4ECDC4"},
        {"icon": "💳", "text": "What's the tip percentage pattern?", "color": "#FF6B6B"},
        {"icon": "📅", "text": "Weekend vs weekday demand patterns?", "color": "#45B7D1"},
        {"icon": "🏙️", "text": "Executive summary for city planners?", "color": "#9D4EDD"},
        {"icon": "🎯", "text": "Optimal driver shift patterns?", "color": "#FFA500"}
    ]

    cols = st.columns(3)
    for i, question in enumerate(quick_questions):
        with cols[i % 3]:
            if st.button(
                    f"{question['icon']} {question['text']}",
                    key=f"quick_{i}",
                    use_container_width=True,
                    type="secondary"
            ):
                st.session_state.user_question = question['text']

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Custom question input
    st.markdown("### 💬 Ask Your Own Question")
    user_question = st.text_area(
        "",
        value=st.session_state.get('user_question', ''),
        height=120,
        placeholder="Example: What time of day has the highest revenue per trip?\n\n💡 Tip: Be specific about metrics, time periods, or comparisons you're interested in.",
        label_visibility="collapsed"
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🚀 Ask GenAI", type="primary", use_container_width=True):
            if user_question:
                with st.spinner("🤔 Analyzing data and generating insights..."):
                    try:
                        prompt = f"Context: {data_context}\n\nQuestion: {user_question}\n\nProvide concise, data-driven insights with specific numbers when possible."

                        response = client.chat.completions.create(
                            model="llama-3.3-70b-versatile",
                            messages=[
                                {"role": "system",
                                 "content": "You are an expert urban mobility analyst specializing in NYC taxi data. Provide clear, data-driven insights with specific numbers from the context."},
                                {"role": "user", "content": prompt}
                            ],
                            temperature=0.3,
                            max_tokens=500
                        )

                        answer = response.choices[0].message.content

                        # Display answer in a beautiful card
                        st.markdown("### 💡 AI Insights")
                        st.markdown(f"""
                        <div class="metric-card" style="text-align: left; background: linear-gradient(135deg, #1a1d29 0%, #232735 100%);">
                            <div style="font-size: 1.2rem; color: #FFD700; margin-bottom: 1rem;">📝 Question:</div>
                            <div style="background: rgba(255, 215, 0, 0.1); padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem; border-left: 4px solid #FFD700;">
                                {user_question}
                            </div>
                            <div style="font-size: 1.2rem; color: #4ECDC4; margin-bottom: 1rem;">🤖 Answer:</div>
                            <div style="background: rgba(78, 205, 196, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #4ECDC4;">
                                {answer}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                        # Save to history
                        if 'qa_history' not in st.session_state:
                            st.session_state.qa_history = []
                        st.session_state.qa_history.append({
                            'question': user_question,
                            'answer': answer,
                            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                        })

                    except Exception as e:
                        st.error(f"❌ Error generating insights: {e}")
            else:
                st.warning("Please enter a question!")

    with col2:
        if st.button("🗑️ Clear History", use_container_width=True, type="secondary"):
            if 'qa_history' in st.session_state:
                st.session_state.qa_history = []
            st.session_state.user_question = ''
            st.rerun()

    # Show history in expandable cards
    if 'qa_history' in st.session_state and st.session_state.qa_history:
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown("### 📝 Recent Questions")

        for i, qa in enumerate(reversed(st.session_state.qa_history[-5:])):
            with st.expander(f"📌 {qa['question'][:60]}... ({qa['timestamp']})", expanded=False):
                st.markdown(f"""
                <div style="background: rgba(255, 215, 0, 0.1); padding: 1rem; border-radius: 8px; margin-bottom: 1rem; border-left: 4px solid #FFD700;">
                    <strong>Question:</strong><br>{qa['question']}
                </div>
                """, unsafe_allow_html=True)
                st.markdown(f"""
                <div style="background: rgba(78, 205, 196, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #4ECDC4;">
                    <strong>Answer:</strong><br>{qa['answer']}
                </div>
                """, unsafe_allow_html=True)

# PAGE 3: SQL QUERY LAB
elif page == "💾 SQL Query Lab":
    st.header("💾 SQL Query Lab")
    st.markdown(
        '<p style="color: #94a3b8; font-size: 1.1rem;">Execute SQL queries directly on the NYC taxi database!</p>',
        unsafe_allow_html=True)

    # Sample queries in beautiful cards
    st.markdown("### 📚 Sample Queries")

    sample_queries = {
        "📊 Top 10 Busiest Hours": """SELECT hour, COUNT(*) as total_trips, 
       ROUND(AVG(fare_amount), 2) as avg_fare,
       ROUND(SUM(total_amount), 2) as revenue
FROM trips
GROUP BY hour
ORDER BY total_trips DESC
LIMIT 10;""",

        "💰 Revenue by Day of Week": """SELECT day_of_week,
       COUNT(*) as trips,
       ROUND(SUM(total_amount), 2) as revenue,
       ROUND(AVG(fare_amount), 2) as avg_fare
FROM trips
GROUP BY day_of_week
ORDER BY day_of_week;""",

        "⏰ Peak vs Off-Peak Comparison": """SELECT 
    CASE WHEN is_peak = 1 THEN 'Peak Hours' ELSE 'Off-Peak' END as period,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount), 2) as avg_fare,
    ROUND(SUM(total_amount), 2) as revenue
FROM trips
GROUP BY is_peak;""",

        "💳 Payment Type Analysis": """SELECT payment_type,
       COUNT(*) as trips,
       ROUND(AVG(tip_percentage), 2) as avg_tip_pct,
       ROUND(SUM(total_amount), 2) as revenue
FROM trips
GROUP BY payment_type
ORDER BY trips DESC;""",

        "📅 Top Revenue Days": """SELECT DATE(tpep_pickup_datetime) as date,
       COUNT(*) as trips,
       ROUND(SUM(total_amount), 2) as revenue
FROM trips
GROUP BY date
ORDER BY revenue DESC
LIMIT 10;""",

        "📍 Fare Distribution by Distance": """SELECT 
    CASE 
        WHEN trip_distance < 1 THEN 'Short (< 1mi)'
        WHEN trip_distance < 3 THEN 'Medium (1-3mi)'
        WHEN trip_distance < 10 THEN 'Long (3-10mi)'
        ELSE 'Very Long (10+mi)'
    END as distance_category,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount), 2) as avg_fare
FROM trips
GROUP BY distance_category;"""
    }

    col1, col2, col3 = st.columns(3)
    cols = [col1, col2, col3]

    for i, (name, query) in enumerate(sample_queries.items()):
        with cols[i % 3]:
            if st.button(name, key=f"sample_{i}", use_container_width=True, type="secondary"):
                st.session_state.sql_query = query

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # SQL Editor
    st.markdown("### ✍️ SQL Editor")

    sql_query = st.text_area(
        "",
        value=st.session_state.get('sql_query', ''),
        height=250,
        placeholder="SELECT * FROM trips LIMIT 10;\n\n💡 Tip: Query the 'trips' table with 12.6M rows",
        label_visibility="collapsed"
    )

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        execute_btn = st.button("▶️ Execute Query", type="primary", use_container_width=True)
    with col2:
        clear_btn = st.button("🗑️ Clear Editor", use_container_width=True, type="secondary")

    if clear_btn:
        st.session_state.sql_query = ''
        st.rerun()

    if execute_btn and sql_query:
        with st.spinner("⚡ Executing query..."):
            start_time = time.time()
            result, error = execute_query(sql_query)
            execution_time = time.time() - start_time

            if error:
                st.error(f"❌ SQL Error: {error}")
            else:
                st.success(f"✅ Query executed successfully in {execution_time:.2f} seconds")

                # Display results in a card
                st.markdown("### 📊 Query Results")

                # Stats in beautiful cards
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-card" style="background: linear-gradient(135deg, #1a1d29 0%, #232735 100%);">
                        <div style="font-size: 2rem; color: #FFD700; margin-bottom: 10px;">📊</div>
                        <div style="font-size: 0.9rem; color: #94a3b8;">Rows Returned</div>
                        <div style="font-size: 1.8rem; font-weight: 700; color: #FFD700;">{len(result):,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="metric-card" style="background: linear-gradient(135deg, #1a1d29 0%, #232735 100%);">
                        <div style="font-size: 2rem; color: #4ECDC4; margin-bottom: 10px;">📋</div>
                        <div style="font-size: 0.9rem; color: #94a3b8;">Columns</div>
                        <div style="font-size: 1.8rem; font-weight: 700; color: #4ECDC4;">{len(result.columns)}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-card" style="background: linear-gradient(135deg, #1a1d29 0%, #232735 100%);">
                        <div style="font-size: 2rem; color: #FF6B6B; margin-bottom: 10px;">⚡</div>
                        <div style="font-size: 0.9rem; color: #94a3b8;">Execution Time</div>
                        <div style="font-size: 1.8rem; font-weight: 700; color: #FF6B6B;">{execution_time:.2f}s</div>
                    </div>
                    """, unsafe_allow_html=True)

                # Dataframe
                st.dataframe(
                    result,
                    use_container_width=True,
                    height=400,
                    hide_index=True,
                    column_config={
                        "_index": st.column_config.NumberColumn(
                            "Index",
                            width="small"
                        )
                    }
                )

                # Download button
                csv = result.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv",
                    use_container_width=True
                )

                # Visualization section
                if len(result) > 0 and len(result.columns) >= 2:
                    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
                    st.markdown("### 📈 Quick Visualization")

                    col1, col2 = st.columns(2)
                    numeric_cols = result.select_dtypes(include=['float64', 'int64']).columns.tolist()

                    with col1:
                        x_col = st.selectbox("X-axis", result.columns, key="x_axis")
                    with col2:
                        y_col = st.selectbox("Y-axis", numeric_cols, key="y_axis")

                    if x_col and y_col:
                        chart_type = st.radio("Chart Type", ["Bar", "Line", "Scatter"], horizontal=True,
                                              key="chart_type")

                        if chart_type == "Bar":
                            fig = px.bar(result.head(20), x=x_col, y=y_col, template="plotly_dark")
                        elif chart_type == "Line":
                            fig = px.line(result.head(20), x=x_col, y=y_col, markers=True, template="plotly_dark")
                        else:
                            fig = px.scatter(result.head(100), x=x_col, y=y_col, template="plotly_dark")

                        fig.update_layout(
                            plot_bgcolor='#1a1d29',
                            paper_bgcolor='#1a1d29',
                            font_color='white'
                        )
                        st.plotly_chart(fig, use_container_width=True)

# PAGE 4: DEEP DIVE ANALYTICS
elif page == "📈 Deep Dive Analytics":
    st.header("📈 Deep Dive Analytics")
    st.markdown('<p style="color: #94a3b8; font-size: 1.1rem;">Advanced analysis and detailed insights</p>',
                unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(
        ["🕐 Temporal Analysis", "💵 Financial Analysis", "🚖 Trip Patterns", "💳 Payment Analysis"])

    with tab1:
        st.markdown("### 🕐 Temporal Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Heatmap
            st.markdown("##### 🔥 Demand Heatmap (Day vs Hour)")
            pivot = df.groupby(['day_of_week', 'hour']).size().reset_index(name='trips')
            pivot_table = pivot.pivot(index='day_of_week', columns='hour', values='trips')

            fig = px.imshow(pivot_table,
                            labels=dict(x="Hour", y="Day of Week", color="Trips"),
                            color_continuous_scale='YlOrRd',
                            aspect="auto",
                            template="plotly_dark")
            fig.update_yaxes(tickvals=[0, 1, 2, 3, 4, 5, 6],
                             ticktext=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'])
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with col2:
            st.markdown("##### 📊 Hourly Revenue Distribution")
            hourly_rev = df.groupby('hour')['total_amount'].sum().reset_index()
            fig = px.area(hourly_rev, x='hour', y='total_amount',
                          labels={'hour': 'Hour', 'total_amount': 'Revenue ($)'},
                          color_discrete_sequence=['#636EFA'],
                          template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

    with tab2:
        st.markdown("### 💵 Financial Analysis")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### 📊 Fare Distribution")
            fig = px.histogram(df, x='fare_amount', nbins=50,
                               range_x=[0, 50],
                               labels={'fare_amount': 'Fare ($)'},
                               color_discrete_sequence=['#00CC96'],
                               template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with col2:
            st.markdown("##### 💰 Tip % by Hour")
            tip_by_hour = df.groupby('hour')['tip_percentage'].mean().reset_index()
            fig = px.line(tip_by_hour, x='hour', y='tip_percentage',
                          markers=True,
                          labels={'hour': 'Hour', 'tip_percentage': 'Avg Tip %'},
                          color_discrete_sequence=['#AB63FA'],
                          template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

    with tab3:
        st.markdown("### 🚖 Trip Patterns")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### 📏 Distance Distribution")
            fig = px.histogram(df, x='trip_distance', nbins=50,
                               range_x=[0, 20],
                               labels={'trip_distance': 'Distance (miles)'},
                               color_discrete_sequence=['#FFA15A'],
                               template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with col2:
            st.markdown("##### 👥 Passenger Count Distribution")
            passenger_dist = df['passenger_count'].value_counts().sort_index().reset_index()
            passenger_dist.columns = ['passengers', 'trips']
            fig = px.bar(passenger_dist, x='passengers', y='trips',
                         labels={'passengers': 'Number of Passengers', 'trips': 'Trips'},
                         color='trips',
                         color_continuous_scale='Teal',
                         template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

    with tab4:
        st.markdown("### 💳 Payment Analysis")

        payment_stats = df.groupby('payment_type').agg({
            'fare_amount': 'count',
            'total_amount': 'sum',
            'tip_percentage': 'mean'
        }).reset_index()
        payment_stats.columns = ['payment_type', 'trips', 'revenue', 'avg_tip_pct']

        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(payment_stats, values='trips', names='payment_type',
                         title='Payment Type Distribution',
                         color_discrete_sequence=px.colors.qualitative.Set3,
                         template="plotly_dark",
                         hole=0.4)
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with col2:
            fig = px.bar(payment_stats, x='payment_type', y='avg_tip_pct',
                         title='Average Tip % by Payment Type',
                         color='avg_tip_pct',
                         color_continuous_scale='RdYlGn',
                         template="plotly_dark")
            fig.update_layout(
                height=500,
                plot_bgcolor='#1a1d29',
                paper_bgcolor='#1a1d29',
                font_color='white',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

# PAGE 5: INSIGHTS
elif page == "💡 Insights":
    st.header("💡 Key Insights & Recommendations")
    st.markdown(
        '<p style="color: #94a3b8; font-size: 1.1rem;">Actionable insights for drivers, planners, and businesses</p>',
        unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Business Insights")

        # Peak Demand Card
        st.markdown("""
        <div class="metric-card" style="text-align: left; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <span style="font-size: 2rem; margin-right: 10px; color: #FF6B6B;">⏰</span>
                <div>
                    <div style="font-size: 1.2rem; font-weight: 700; color: #FFD700;">Peak Demand Hours</div>
                    <div style="font-size: 0.9rem; color: #94a3b8;">Highest traffic periods</div>
                </div>
            </div>
            <div style="background: rgba(255, 107, 107, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #FF6B6B;">
                • <strong>7 PM</strong>: 799K trips - Evening rush hour<br>
                • <strong>6 PM</strong>: 793K trips - Post-work commute<br>
                • <strong>8 PM</strong>: 728K trips - Social/dining hours
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Revenue Drivers Card
        st.markdown("""
        <div class="metric-card" style="text-align: left; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <span style="font-size: 2rem; margin-right: 10px; color: #4ECDC4;">💰</span>
                <div>
                    <div style="font-size: 1.2rem; font-weight: 700; color: #FFD700;">Revenue Drivers</div>
                    <div style="font-size: 0.9rem; color: #94a3b8;">Key performance indicators</div>
                </div>
            </div>
            <div style="background: rgba(78, 205, 196, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #4ECDC4;">
                • Peak hours: <strong>25.8%</strong> of total trips<br>
                • Credit card tips: <strong>23.8%</strong> average<br>
                • Weekend volume: <strong>31.1%</strong> of total trips
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Driver Recommendations Card
        st.markdown("""
        <div class="metric-card" style="text-align: left;">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <span style="font-size: 2rem; margin-right: 10px; color: #FFD700;">🚕</span>
                <div>
                    <div style="font-size: 1.2rem; font-weight: 700; color: #FFD700;">Driver Recommendations</div>
                    <div style="font-size: 0.9rem; color: #94a3b8;">Optimize earnings strategy</div>
                </div>
            </div>
            <div style="background: rgba(255, 215, 0, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #FFD700;">
                • <strong>Work 6-10 PM</strong>: Maximum earnings window<br>
                • <strong>Focus on credit card payments</strong>: Higher tip percentages<br>
                • <strong>Target weekend evenings</strong>: Steady high demand
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("### 📊 Data Quality & Scalability")

        # Data Quality Metrics
        st.markdown("""
        <div class="metric-card" style="text-align: left; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <span style="font-size: 2rem; margin-right: 10px; color: #10B981;">📈</span>
                <div>
                    <div style="font-size: 1.2rem; font-weight: 700; color: #FFD700;">Data Quality Metrics</div>
                    <div style="font-size: 0.9rem; color: #94a3b8;">Dataset reliability indicators</div>
                </div>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                <div style="background: rgba(16, 185, 129, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #10B981;">
                    <div style="font-size: 0.9rem; color: #94a3b8;">Completeness</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #10B981;">99.08%</div>
                    <div style="font-size: 0.8rem; color: #10B981;">Excellent</div>
                </div>
                <div style="background: rgba(59, 130, 246, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #3B82F6;">
                    <div style="font-size: 0.9rem; color: #94a3b8;">Records</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #3B82F6;">12.63M</div>
                    <div style="font-size: 0.8rem; color: #3B82F6;">Processed</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Scalability Recommendations
        st.markdown("""
        <div class="metric-card" style="text-align: left;">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <span style="font-size: 2rem; margin-right: 10px; color: #F59E0B;">🚀</span>
                <div>
                    <div style="font-size: 1.2rem; font-weight: 700; color: #FFD700;">Scalability Architecture</div>
                    <div style="font-size: 0.9rem; color: #94a3b8;">For 100GB+ datasets</div>
                </div>
            </div>
            <div style="background: rgba(245, 158, 11, 0.1); padding: 1rem; border-radius: 8px; border-left: 4px solid #F59E0B;">
                <div style="display: flex; align-items: start; margin-bottom: 0.5rem;">
                    <span style="color: #F59E0B; margin-right: 10px;">•</span>
                    <div><strong>Processing:</strong> PySpark for distributed computing</div>
                </div>
                <div style="display: flex; align-items: start; margin-bottom: 0.5rem;">
                    <span style="color: #F59E0B; margin-right: 10px;">•</span>
                    <div><strong>Vector DB:</strong> FAISS for RAG implementations</div>
                </div>
                <div style="display: flex; align-items: start; margin-bottom: 0.5rem;">
                    <span style="color: #F59E0B; margin-right: 10px;">•</span>
                    <div><strong>Deployment:</strong> Azure Databricks cluster</div>
                </div>
                <div style="display: flex; align-items: start;">
                    <span style="color: #F59E0B; margin-right: 10px;">•</span>
                    <div><strong>Storage:</strong> Delta Lake for ACID compliance</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# Footer with enhanced design
st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
st.markdown("""
<div style='text-align: center; color: #94a3b8; padding: 2rem;'>
    <div style='font-size: 1.5rem; margin-bottom: 1rem;'>🚕 💰 📊 🤖</div>
    <p style='font-size: 1.1rem; font-weight: 600; color: #FFD700; margin-bottom: 0.5rem;'>NYC Taxi Analytics Platform</p>
    <p style='font-size: 0.9rem; margin-bottom: 0.5rem;'>Built with Streamlit, PySpark & Groq AI</p>
    <p style='font-size: 0.8rem;'>January 2015 Dataset | Real-time Analytics | AI-Powered Insights</p>
</div>
""", unsafe_allow_html=True)