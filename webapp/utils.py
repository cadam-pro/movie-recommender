import pandas as pd
import streamlit as st


@st.cache_data
def load_movies():
    return pd.read_csv("data/TMDB_all_movies.csv")


@st.cache_data
def chunk_movies(df):
    # Remplacer les valeurs manquantes éventuelles par 0 pour éviter les erreurs de tri
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce").fillna(0)
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce").fillna(0)

    # Trie par popularité puis par nombre de votes
    df_sorted = df.sort_values(
        by=["vote_count", "popularity"], ascending=[False, False]
    )

    # Garde les 1000 premiers
    return df_sorted.head(1000)
