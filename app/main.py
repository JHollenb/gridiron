import streamlit as st
import polars as pl
import plotly.graph_objects as go
import numpy as np
import sys
import os
from pathlib import Path

# Add parent dir to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.query import GridironQuery

# Page Config (Must be first)
st.set_page_config(page_title="Gridiron NGS", layout="wide", page_icon="🏈")

# --- Initialize Query Engine ---
@st.cache_resource
def get_query_engine():
    # Use absolute path relative to the Repo Root, not the App folder
    # This assumes main.py is in /app/ and data is in /data/
    repo_root = Path(__file__).parent.parent
    pool_path = repo_root / "data" / "raw_pool"
    
    st.sidebar.text(f"Pool: {pool_path.name}")
    
    try:
        return GridironQuery(str(pool_path))
    except Exception as e:
        st.error(f"Failed to initialize DB: {e}")
        return None

db = get_query_engine()

if db is None:
    st.stop() # Stop execution if DB failed

# --- Sidebar: Browser / Filtering ---
st.sidebar.title("🏈 Play Selector")

try:
    # 1. Load Games
    q = db.get_pool()
    # Check if data exists by trying to fetch 1 row
    try:
        q.collect().head()
    except Exception as e:
        st.error(f"Polars Scan Error: {e}")
        st.stop()

    games = q.select("gameId").unique().collect().to_series().to_list()
    
    if not games:
        st.warning("Database connected but no Games found (0 rows).")
        st.stop()
        
    selected_game = st.sidebar.selectbox("Select Game", sorted(games))

    # 2. Load Plays for that Game
    plays_in_game = (
        q.filter(pl.col("gameId") == selected_game)
        .select(["playId", "playDirection"]) # Grab direction for context
        .unique()
        .collect()
    )
    play_ids = plays_in_game["playId"].sort().to_list()
    selected_play_id = st.sidebar.selectbox("Select Play", play_ids)

    # --- Main Area: Rendering ---
    st.title(f"Game {selected_game} | Play {selected_play_id}")

    if st.button("Render Play 🎬", type="primary"):
        with st.spinner("Fetching Tracking Data..."):
            # 3. Fetch ONLY this play's data
            play_df = (
                q.filter(
                    (pl.col("gameId") == selected_game) & 
                    (pl.col("playId") == selected_play_id)
                )
                .sort("frameId")
                .collect()
            )

        # --- The Optimizer: Plotly WebGL Animation ---
        
        # Split data for easier plotting
        home = play_df.filter(pl.col("playerSide") == "Offense")
        away = play_df.filter(pl.col("playerSide") == "Defense")
        ball = play_df.filter(pl.col("nflId") == 0)
        if ball is None:
            print(f"WARNING - No ball data found")
        
        # Get Frames
        frames = sorted(play_df["frameId"].unique().to_list())
        
        # Create Figure
        fig = go.Figure()

        # Field Setup (Simplified 120x53.3)
        fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=53.3, line=dict(color="white"), fillcolor="green", layer="below")
        
        # Add Initial Traces (Frame 0)
        # We use Scattergl (WebGL) for performance instead of Scatter (SVG)
        
        # Home Team
        fig.add_trace(go.Scattergl(
            x=home.filter(pl.col("frameId") == frames[0])["x"],
            y=home.filter(pl.col("frameId") == frames[0])["y"],
            mode="markers",
            marker=dict(size=12, color="blue"),
            name="Offense"
        ))
        
        # Away Team
        fig.add_trace(go.Scattergl(
            x=away.filter(pl.col("frameId") == frames[0])["x"],
            y=away.filter(pl.col("frameId") == frames[0])["y"],
            mode="markers",
            marker=dict(size=12, color="red"),
            name="Defense"
        ))

        # Ball
        if ball is not None:
            fig.add_trace(go.Scattergl(
                x=ball.filter(pl.col("frameId") == frames[0])["x"],
                y=ball.filter(pl.col("frameId") == frames[0])["y"],
                mode="markers",
                marker=dict(size=8, color="brown"),
                name="Ball"
            ))

        # Construct Frames for Animation
        # This part can be slow in Python loops. For production, optimize this list comp.
        animation_frames = []
        for f in frames:
            frame_data = play_df.filter(pl.col("frameId") == f)
            
            home_f = frame_data.filter(pl.col("playerSide") == "Offense")
            away_f = frame_data.filter(pl.col("playerSide") == "Defense")
            if ball is not None:
                ball_f = frame_data.filter(pl.col("nflId") == 0)
            
                animation_frames.append(go.Frame(
                    data=[
                        go.Scattergl(x=home_f["x"], y=home_f["y"]),
                        go.Scattergl(x=away_f["x"], y=away_f["y"]),
                        go.Scattergl(x=ball_f["x"], y=ball_f["y"])
                    ],
                    name=str(f)
                ))
            else:
                animation_frames.append(go.Frame(
                    data=[
                        go.Scattergl(x=home_f["x"], y=home_f["y"]),
                        go.Scattergl(x=away_f["x"], y=away_f["y"]),
                    ],
                    name=str(f)
                ))


        fig.frames = animation_frames

        # Layout Settings
        fig.update_layout(
            width=1000,
            height=600,
            xaxis=dict(range=[0, 120], showgrid=False, zeroline=False),
            yaxis=dict(range=[0, 53.3], showgrid=False, zeroline=False),
            updatemenus=[dict(
                type="buttons",
                showactive=False,
                buttons=[dict(
                    label="Play",
                    method="animate",
                    args=[None, dict(frame=dict(duration=100, redraw=False), fromcurrent=True)]
                )]
            )]
        )

        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Runtime Error: {e}")
    st.write("Traceback details in terminal.")
    st.stop()
