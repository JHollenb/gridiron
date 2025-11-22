import streamlit as st
import polars as pl
import plotly.graph_objects as go
import numpy as np
import sys
import os
import traceback
from pathlib import Path

# Add parent dir to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.query import GridironQuery

# Page Config (Dark Mode Friendly)
st.set_page_config(
    page_title="Gridiron NGS", 
    layout="wide", 
    page_icon="🏈",
    initial_sidebar_state="expanded"
)

# --- Helper: Reset State on Selection Change ---
def reset_play_state():
    st.session_state.render_triggered = False

# --- Initialize Query Engine ---
@st.cache_resource
def get_query_engine():
    repo_root = Path(__file__).parent.parent
    pool_path = repo_root / "data" / "raw_pool"
    st.sidebar.text(f"Pool: {pool_path.name}")
    try:
        return GridironQuery(str(pool_path))
    except Exception as e:
        st.error(f"Failed to initialize DB: {e}")
        return None

db = get_query_engine()
if db is None: st.stop()

# --- Sidebar: Browser / Filtering ---
st.sidebar.title("🏈 Play Selector")

try:
    # 1. Load Games
    q = db.get_pool()
    # Quick existence check
    try: q.collect().head()
    except: st.stop()

    games = q.select("gameId").unique().collect().to_series().to_list()
    if not games: st.stop()
        
    selected_game = st.sidebar.selectbox("Select Game", sorted(games), on_change=reset_play_state)

    # 2. Load Plays
    plays_in_game = (
        q.filter(pl.col("gameId") == selected_game)
        .select(["playId"]) 
        .unique()
        .collect()
    )
    play_ids = plays_in_game["playId"].sort().to_list()
    selected_play_id = st.sidebar.selectbox("Select Play", play_ids, on_change=reset_play_state)

    # --- Main Area ---
    st.title(f"Game {selected_game} | Play {selected_play_id}")

    # --- STATE MANAGEMENT ---
    # This ensures the chart persists after interaction
    if "render_triggered" not in st.session_state:
        st.session_state.render_triggered = False

    if st.button("Render Play 🎬", type="primary"):
        st.session_state.render_triggered = True

    # Only Run Calculation if Triggered
    if st.session_state.render_triggered:
        with st.spinner("Processing Tracking Data..."):
            # 3. Fetch Data
            play_df = (
                q.filter(
                    (pl.col("gameId") == selected_game) & 
                    (pl.col("playId") == selected_play_id)
                )
                .sort("frameId")
                .collect()
            )

            # --- DASHBOARD STATS CALCULATION ---
            # Calculate these efficiently using Polars before plotting
            total_frames = play_df["frameId"].max()
            
            # Handle speed (s) if it exists, else 0
            max_speed = play_df["s"].max() if "s" in play_df.columns else 0.0
            
            # Count players (unique nflIds excluding ball)
            n_players = play_df.filter(pl.col("nflId").is_not_null())["nflId"].n_unique()
            
            # Get Event Tags (unique events happening this play)
            events = []
            if "event" in play_df.columns:
                events = play_df.filter(pl.col("event").is_not_null())["event"].unique().to_list()

            # --- RENDER DASHBOARD ---
            # We put this ABOVE the chart or below? Let's put it below as requested, 
            # but calculating it here makes the code cleaner.
            
            # --- PLOT LOGIC ---
            # Check side/team columns
            side_col = "playerSide" if "playerSide" in play_df.columns else "team"
            
            # Case-insensitive filtering
            home = play_df.filter(pl.col(side_col).cast(pl.String).str.to_lowercase() == "offense")
            away = play_df.filter(pl.col(side_col).cast(pl.String).str.to_lowercase() == "defense")
            
            # Ball Logic
            ball = play_df.filter(
                (pl.col("nflId").is_null()) | (pl.col("nflId") == 0) | (pl.col(side_col) == "football")
            )
            has_ball = not ball.is_empty()

            frames = sorted(play_df["frameId"].unique().to_list())
            if not frames:
                st.error("No frames found.")
                st.stop()

            # --- PLOTLY FIGURE ---
            fig = go.Figure()

            # Dark Mode Field
            fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=53.3, 
                          line=dict(color="rgba(255,255,255,0.8)", width=2), # White boundary
                          fillcolor="#263c28", # Dark Grass Green
                          layer="below")
            
            # Yard Lines (Simplified)
            for i in range(10, 110, 10):
                fig.add_shape(type="line", x0=i, y0=0, x1=i, y1=53.3,
                              line=dict(color="rgba(255,255,255,0.3)", width=1), layer="below")

            # Helper
            def get_xy(df, frame):
                f_df = df.filter(pl.col("frameId") == frame)
                return f_df["x"], f_df["y"]

            # Initial Traces
            h_x, h_y = get_xy(home, frames[0])
            a_x, a_y = get_xy(away, frames[0])
            
            fig.add_trace(go.Scattergl(x=h_x, y=h_y, mode="markers", marker=dict(size=12, color="#00BFFF", line=dict(color='white', width=1)), name="Offense"))
            fig.add_trace(go.Scattergl(x=a_x, y=a_y, mode="markers", marker=dict(size=12, color="#FF4500", line=dict(color='white', width=1)), name="Defense"))

            if has_ball:
                b_x, b_y = get_xy(ball, frames[0])
                fig.add_trace(go.Scattergl(x=b_x, y=b_y, mode="markers", marker=dict(size=8, color="#FFD700"), name="Ball"))

            # Frames
            animation_frames = []
            for f in frames:
                h_x, h_y = get_xy(home, f)
                a_x, a_y = get_xy(away, f)
                frame_traces = [go.Scattergl(x=h_x, y=h_y), go.Scattergl(x=a_x, y=a_y)]
                if has_ball:
                    b_x, b_y = get_xy(ball, f)
                    frame_traces.append(go.Scattergl(x=b_x, y=b_y))
                animation_frames.append(go.Frame(data=frame_traces, name=str(f)))

            fig.frames = animation_frames

            # Dark Mode Layout
            fig.update_layout(
                height=600,
                paper_bgcolor="black", # Outside the chart
                plot_bgcolor="black",  # Behind the chart (but covered by our rect)
                font=dict(color="white"),
                xaxis=dict(range=[-5, 125], showgrid=False, visible=False, fixedrange=True),
                yaxis=dict(range=[-5, 58], showgrid=False, visible=False, fixedrange=True),
                legend=dict(orientation="h", y=1.05, x=0.5, xanchor="center"),
                updatemenus=[dict(
                    type="buttons",
                    showactive=False,
                    y=0, x=0, xanchor="left",
                    pad=dict(t=50, r=10),
                    buttons=[dict(
                        label="▶ Play",
                        method="animate",
                        args=[None, dict(frame=dict(duration=100, redraw=False), fromcurrent=True)]
                    )]
                )]
            )

            st.plotly_chart(fig, use_container_width=True)

            # --- DASHBOARD ROW ---
            st.markdown("### 📊 Play Telemetry")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Duration", f"{total_frames / 10.0}s", f"{total_frames} frames")
            with col2:
                st.metric("Max Speed (Any Player)", f"{max_speed:.2f} mph")
            with col3:
                st.metric("Active Players", n_players)
            with col4:
                # Display first 2 events found or "None"
                event_str = ", ".join(events[:2]) if events else "None"
                st.metric("Key Events", event_str)

except Exception as e:
    st.error(f"Runtime Error: {e}")
    traceback.print_exc()
