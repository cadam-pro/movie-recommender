import os
import streamlit as st
import pandas as pd
import requests
import json
from dotenv import load_dotenv

st.set_page_config(layout="wide")
st.title("🎬 Recommandation de films")

# --- Chargement des variables d'environnement ---
load_dotenv()

# Utiliser une variable d'environnement avec une valeur par défaut
API_BASE_URL = os.getenv('API_BASE_URL')

print(f"API_BASE_URL: {API_BASE_URL}")

# --- Chargement des films ---
@st.cache_data
def load_movies():
    df = pd.read_csv(
        "data/TMDB_all_movies.csv", usecols=["id", "title"]
    )  # Adapte le nom du fichier
    return df


movies_df = load_movies()

movie_titles = ["--- Sélectionne un film ---"] + movies_df["title"].tolist()

selected_title = st.selectbox(
    "Choisis un film pour obtenir des recommandations",
    movie_titles,
    index=0,  # Par défaut, la première option
)

if selected_title != "--- Sélectionne un film ---":
    # --- Récupérer l'ID correspondant ---
    selected_row = movies_df[movies_df["title"] == selected_title]
    if not selected_row.empty:
        movie_id = selected_row.iloc[0]["id"]

        # --- Appel à l'API ---
        response = requests.get(f"{API_BASE_URL}/recommendations", params={"movie_id": movie_id})

        # response = requests.get(
        #     f"http://backend:8000/recommendations?movie_id={movie_id}"
        # )
        if response.status_code == 200:
            movie = response.json()
            print(movie)

            # --- Fonction d'affichage des infos ---
            def display_movie(movie_json_str, label):
                data = json.loads(movie_json_str)
                st.subheader(label + f" : {data['title']}")
                cols = st.columns([1, 2])
                with cols[0]:
                    st.image(f"https://image.tmdb.org/t/p/w500{data['poster_path']}")
                with cols[1]:
                    st.markdown(f"**Overview** : {data['overview']}")
                    st.markdown(
                        f"**Réalisateur(s)** : {', '.join(data.get('director_array', []))}"
                    )
                    st.markdown(f"**Note moyenne** : {data['vote_average']}")
                    st.markdown(f"**Date de sortie** : {data['release_date']}")
                    st.markdown(f"**Durée** : {int(data['runtime'])} minutes")
                    st.markdown(
                        f"**Langue originale** : {data['original_language_array'][0]}"
                    )
                    st.markdown(
                        f"**Genres** : {', '.join(data.get('genres_array', []))}"
                    )

            # --- Affichage des recommandations ---
            st.markdown("## 📌 Recommandations")
            display_movie(movie["recommendations"]["popular"], "🎯 Le plus populaire")
            display_movie(
                movie["recommendations"]["underground"], "🎭 Le plus underground"
            )
            display_movie(movie["recommendations"]["newest"], "🆕 Le plus récent")

        else:
            st.error("Erreur lors de la récupération des recommandations.")
