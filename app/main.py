import streamlit as st
import polars as pl
import plotly.graph_objects as go
import numpy as np
import sys
import os

# Add parent dir to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.query import GridironQuery

# Page Config (Must be first)
st.set_page_config(page_title="Gridiron NGS", layout="wide", page_icon="🏈")

# --- Initialize Query Engine ---
@st.cache_resource
def get_query_engine():
    # Points to your parquet pool
    return GridironQuery("./data/raw_pool")

db = get_query_engine()

# --- Sidebar: Browser / Filtering ---
st.sidebar.title("🏈 Play Selector")

# 1. Load Games (Lazy-ish: We scan unique gameIds)
# Ideally, cache this metadata separately if you have 1000s of games
q = db.get_pool()
games = q.select("gameId").unique().collect().to_series().to_list()
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
    home = play_df.filter(pl.col("team") == "home")
    away = play_df.filter(pl.col("team") == "away")
    ball = play_df.filter(pl.col("team") == "football")
    
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
        name="Home"
    ))
    
    # Away Team
    fig.add_trace(go.Scattergl(
        x=away.filter(pl.col("frameId") == frames[0])["x"],
        y=away.filter(pl.col("frameId") == frames[0])["y"],
        mode="markers",
        marker=dict(size=12, color="red"),
        name="Away"
    ))

    # Ball
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
        
        home_f = frame_data.filter(pl.col("team") == "home")
        away_f = frame_data.filter(pl.col("team") == "away")
        ball_f = frame_data.filter(pl.col("team") == "football")
        
        animation_frames.append(go.Frame(
            data=[
                go.Scattergl(x=home_f["x"], y=home_f["y"]),
                go.Scattergl(x=away_f["x"], y=away_f["y"]),
                go.Scattergl(x=ball_f["x"], y=ball_f["y"])
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
