import streamlit as st
import requests
from utils import load_movies, chunk_movies

st.set_page_config(layout="wide")
st.title("🎬 Recommandation de films")

df = load_movies()
movies_df = chunk_movies(df)

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
        response = requests.get(
            f"http://127.0.0.1:8000/recommendations?movie_id={movie_id}"
        )
        if response.status_code == 200:
            movie = response.json()

            # --- Fonction d'affichage des infos ---
            def display_movie(movie_json_str, label):
                data = movie_json_str
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
