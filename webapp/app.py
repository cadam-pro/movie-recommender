import streamlit as st
import pandas as pd
import requests
import json
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud

st.set_page_config(layout="wide")
st.title("🎬 Recommandation de films")


# --- Chargement des films ---
@st.cache_data
def load_movies():
    df = pd.read_csv("data/TMDB_all_movies.csv")  # Adapte le nom du fichier
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
        response = requests.get(
            f"http://127.0.0.1:8000/recommendations?movie_id={movie_id}"
        )
        if response.status_code == 200:
            movie = response.json()

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

st.header("🎬 Top 10 des films les plus populaires du moment")
top_rated = movies_df.sort_values(by="popularity", ascending=False).head(10)
st.dataframe(top_rated[["title", "director", "popularity", "release_date"]])

st.header("🎬 Top 10 des films les plus notés")
top_rated = movies_df.sort_values(by="vote_count", ascending=False).head(10)
st.dataframe(top_rated[["title", "director", "vote_count", "release_date"]])

st.header("🌐 Langues les plus représentées")
lang_counts = Counter(
    lang
    for sublist in movies_df["original_language"]
    for lang in ([sublist] if isinstance(sublist, str) else [])
)
st.bar_chart(
    pd.DataFrame(lang_counts.most_common(10), columns=["Langue", "Nombre"]).set_index(
        "Langue"
    )
)

st.header("📈 Nombre de films produits par année")
movies_df["year"] = pd.to_datetime(movies_df["release_date"], errors="coerce").dt.year
films_per_year = movies_df["year"].value_counts().sort_index()
st.line_chart(films_per_year)

st.header("☁️ Nuage de mots des synopsis")
text = " ".join(movies_df["overview"].dropna())
wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)
fig, ax = plt.subplots()
ax.imshow(wordcloud, interpolation="bilinear")
ax.axis("off")
st.pyplot(fig)
