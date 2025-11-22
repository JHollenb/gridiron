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

    # RENDER PLAY BUTTON
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

            # --- 🔍 DEBUG TOOL ---
            # If you see a green screen, expand this in the UI to see WHY
            with st.expander("Debug: Inspect Raw Data", expanded=False):
                st.write("Columns:", play_df.columns)
                st.write("First 5 rows:", play_df.head(5))
                
                # Check unique values in the column you are filtering on
                # This helps you see if it's "Offense", "OFFENSE", or "home"
                if "playerSide" in play_df.columns:
                    st.write("Unique Sides:", play_df["playerSide"].unique().to_list())
                elif "team" in play_df.columns:
                    st.write("Unique Teams:", play_df["team"].unique().to_list())

            # --- Data Split (Robust) ---
            # We use .str.to_lowercase() to be safe against "OFFENSE" vs "Offense"
            # Adjust the column name "playerSide" if your schema renamed it to "player_side" or "team"
            
            # Check which column exists for the side/team
            side_col = "playerSide" if "playerSide" in play_df.columns else "team"
            
            home = play_df.filter(pl.col(side_col).cast(pl.String).str.to_lowercase() == "offense")
            away = play_df.filter(pl.col(side_col).cast(pl.String).str.to_lowercase() == "defense")
            
            # Robust Ball Check: 
            # Usually ball has nflId=NaN OR team='football'. We check both to be safe.
            ball = play_df.filter(
                (pl.col("nflId").is_null()) | (pl.col("nflId") == 0) | (pl.col(side_col) == "football")
            )
            
            # Check if we actually have a ball
            has_ball = not ball.is_empty()

            # Get Frames
            frames = sorted(play_df["frameId"].unique().to_list())
            if not frames:
                st.error("No frames found for this play!")
                st.stop()

            # Create Figure
            fig = go.Figure()

            # Field Setup
            fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=53.3, line=dict(color="white"), fillcolor="green", layer="below")
            
            # --- INITIAL TRACES (Frame 0) ---
            # We use kwargs to allow empty data without crashing
            
            # Helper to get data safely
            def get_xy(df, frame):
                f_df = df.filter(pl.col("frameId") == frame)
                return f_df["x"], f_df["y"]

            h_x, h_y = get_xy(home, frames[0])
            a_x, a_y = get_xy(away, frames[0])
            
            fig.add_trace(go.Scattergl(x=h_x, y=h_y, mode="markers", marker=dict(size=12, color="blue"), name="Offense"))
            fig.add_trace(go.Scattergl(x=a_x, y=a_y, mode="markers", marker=dict(size=12, color="red"), name="Defense"))

            if has_ball:
                b_x, b_y = get_xy(ball, frames[0])
                fig.add_trace(go.Scattergl(x=b_x, y=b_y, mode="markers", marker=dict(size=8, color="brown"), name="Ball"))

            # --- ANIMATION FRAMES ---
            animation_frames = []
            for f in frames:
                h_x, h_y = get_xy(home, f)
                a_x, a_y = get_xy(away, f)
                
                frame_traces = [
                    go.Scattergl(x=h_x, y=h_y),
                    go.Scattergl(x=a_x, y=a_y)
                ]
                
                if has_ball:
                    b_x, b_y = get_xy(ball, f)
                    frame_traces.append(go.Scattergl(x=b_x, y=b_y))
                    
                animation_frames.append(go.Frame(data=frame_traces, name=str(f)))

            fig.frames = animation_frames

            # Layout Settings
            fig.update_layout(
                height=600,
                xaxis=dict(range=[0, 120], showgrid=False, zeroline=False, visible=False), # Hide axes for cleaner look
                yaxis=dict(range=[0, 53.3], showgrid=False, zeroline=False, visible=False),
                updatemenus=[dict(
                    type="buttons",
                    showactive=False,
                    buttons=[dict(
                        label="▶ Play",
                        method="animate",
                        args=[None, dict(frame=dict(duration=100, redraw=False), fromcurrent=True)]
                    )]
                )]
            )

            st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Runtime Error: {e}")
    traceback.print_exc()
    st.write("Traceback details in terminal.")
    st.stop()
