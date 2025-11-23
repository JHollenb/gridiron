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

# Page Config
st.set_page_config(
    page_title="Gridiron NGS", 
    layout="wide", 
    page_icon="🏈",
    initial_sidebar_state="expanded"
)

# --- Helper: Reset State ---
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

# --- Sidebar ---
st.sidebar.title("🏈 Play Selector")

try:
    q = db.get_pool()
    try: q.collect().head()
    except: st.stop()

    games = q.select("gameId").unique().collect().to_series().to_list()
    if not games: st.stop()
        
    selected_game = st.sidebar.selectbox("Select Game", sorted(games), on_change=reset_play_state)

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

    if "render_triggered" not in st.session_state:
        st.session_state.render_triggered = False

    if st.button("Render Play 🎬", type="primary"):
        st.session_state.render_triggered = True

    if st.session_state.render_triggered:
        with st.spinner("Processing Tracking Data..."):
            play_df = (
                q.filter(
                    (pl.col("gameId") == selected_game) & 
                    (pl.col("playId") == selected_play_id)
                )
                .sort("frameId")
                .collect()
            )

            # --- 🔍 DEBUG ---
            with st.expander("🛠 Debug: Inspect Raw Data", expanded=False):
                st.write(f"Rows: {play_df.height}")
                st.write("Columns:", play_df.columns)
                st.dataframe(play_df.head(5))

            # --- PRE-CALC STATS ---
            total_frames = play_df["frameId"].max() if not play_df.is_empty() else 0
            max_speed = play_df["s"].max() if "s" in play_df.columns else 0.0
            n_players = play_df.filter(pl.col("nflId").is_not_null())["nflId"].n_unique()
            events = []
            if "event" in play_df.columns:
                events = play_df.filter(pl.col("event").is_not_null())["event"].unique().to_list()

            # --- DATA SPLIT ---
            side_col = "playerSide" if "playerSide" in play_df.columns else "team"
            player_side = pl.col(side_col).cast(pl.String).str.to_lowercase()

            home = play_df.filter((player_side == "home") | (player_side == "offense")) 
            away = play_df.filter((player_side == "away") | (player_side == "defense")) 
            
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

            # 1. FIELD LAYER (Static)
            fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=53.3, 
                          line=dict(color="rgba(255,255,255,0.8)", width=2),
                          fillcolor="#263c28", layer="below")
            
            # Yard Lines
            for i in range(10, 110, 10):
                fig.add_shape(type="line", x0=i, y0=0, x1=i, y1=53.3,
                              line=dict(color="rgba(255,255,255,0.3)", width=1), layer="below")

            # 2. GHOST TRACES (Static - Future Paths)
            # We iterate through unique players and draw their full lines first
            # These will sit "under" the dots and won't move.
            
            # Colors
            COLOR_HOME = "#00BFFF" # Deep Sky Blue
            COLOR_AWAY = "#FF4500" # Orange Red
            COLOR_BALL = "#FFD700" # Gold

            def add_ghosts(df, color):
                if "nflId" not in df.columns: return
                # Get unique IDs (excluding nulls if any)
                pids = df.filter(pl.col("nflId").is_not_null())["nflId"].unique().to_list()
                for pid in pids:
                    path = df.filter(pl.col("nflId") == pid)
                    fig.add_trace(go.Scatter(
                        x=path["x"], y=path["y"],
                        mode="lines",
                        line=dict(color=color, width=2),
                        opacity=0.15, # Very faint
                        showlegend=False,
                        hoverinfo="skip" # Don't show tooltips for ghosts
                    ))

            add_ghosts(home, COLOR_HOME)
            add_ghosts(away, COLOR_AWAY)
            if has_ball:
                # Ball ghost
                fig.add_trace(go.Scatter(
                    x=ball["x"], y=ball["y"], mode="lines", 
                    line=dict(color=COLOR_BALL, width=2, dash="dot"), opacity=0.3, 
                    showlegend=False, hoverinfo="skip"
                ))


            # 3. ACTIVE PLAYERS (Animated)
            # NOTE: We switched from Scattergl to Scatter (SVG) to fix the disappearing bug.
            
            def get_xy(df, frame):
                f_df = df.filter(pl.col("frameId") == frame)
                return f_df["x"], f_df["y"]

            # Initial Traces (Frame 0)
            h_x, h_y = get_xy(home, frames[0])
            a_x, a_y = get_xy(away, frames[0])
            
            fig.add_trace(go.Scatter(x=h_x, y=h_y, mode="markers", marker=dict(size=12, color=COLOR_HOME, line=dict(color='white', width=1)), name="Offense"))
            fig.add_trace(go.Scatter(x=a_x, y=a_y, mode="markers", marker=dict(size=12, color=COLOR_AWAY, line=dict(color='white', width=1)), name="Defense"))

            if has_ball:
                b_x, b_y = get_xy(ball, frames[0])
                fig.add_trace(go.Scatter(x=b_x, y=b_y, mode="markers", marker=dict(size=8, color=COLOR_BALL), name="Ball"))

            # 4. ANIMATION FRAMES
            animation_frames = []
            for f in frames:
                h_x, h_y = get_xy(home, f)
                a_x, a_y = get_xy(away, f)
                
                # We only update the LAST 3 traces (Home, Away, Ball). 
                # The ghost traces (indexes 0 to N) remain static and are ignored here.
                frame_traces = [go.Scatter(x=h_x, y=h_y), go.Scatter(x=a_x, y=a_y)]
                
                if has_ball:
                    b_x, b_y = get_xy(ball, f)
                    frame_traces.append(go.Scatter(x=b_x, y=b_y))
                
                animation_frames.append(go.Frame(data=frame_traces, name=str(f), traces=list(range(len(fig.data) - len(frame_traces), len(fig.data)))))

            fig.frames = animation_frames

            # 5. LAYOUT CONFIG
            fig.update_layout(
                height=600,
                paper_bgcolor="black",
                plot_bgcolor="black",
                font=dict(color="white"),
                xaxis=dict(range=[-5, 125], showgrid=False, visible=False, fixedrange=True),
                yaxis=dict(range=[-5, 58], showgrid=False, visible=False, fixedrange=True),
                legend=dict(orientation="h", y=1.05, x=0.5, xanchor="center"),
                hovermode="closest",
                updatemenus=[dict(
                    type="buttons",
                    showactive=False,
                    y=0, x=0, xanchor="left",
                    pad=dict(t=50, r=10),
                    buttons=[dict(
                        label="▶ Play",
                        method="animate",
                        # redraw=False is now SAFE because we use SVG (Scatter) not WebGL
                        args=[None, dict(frame=dict(duration=100, redraw=False), fromcurrent=True)]
                    ),
                    dict(
                        label="⏸ Pause",
                        method="animate",
                        args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate", transition=dict(duration=0))]
                    )]
                )]
            )
            
            # Slider
            sliders = [dict(
                steps=[dict(method='animate', 
                            args=[[str(f)], dict(mode='immediate', frame=dict(duration=0, redraw=False), transition=dict(duration=0))],
                            label=str(f)) for f in frames],
                transition=dict(duration=0),
                x=0.1, len=0.9, y=0,
                currentvalue=dict(font=dict(size=15), prefix="Frame: ", visible=True, xanchor="right"),
                font=dict(color="white")
            )]
            fig.update_layout(sliders=sliders)

            st.plotly_chart(fig, use_container_width=True)

            # --- DASHBOARD ---
            st.markdown("### 📊 Play Telemetry")
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Duration", f"{total_frames / 10.0}s", f"{total_frames} frames")
            with col2: st.metric("Max Speed", f"{max_speed:.2f} mph")
            with col3: st.metric("Active Players", n_players)
            with col4:
                event_str = ", ".join(events[:2]) if events else "None"
                st.metric("Key Events", event_str)

except Exception as e:
    st.error(f"Runtime Error: {e}")
    traceback.print_exc()
