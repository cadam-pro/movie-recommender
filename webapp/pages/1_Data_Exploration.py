import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from utils import load_movies, chunk_movies
from wordcloud import WordCloud

movies_df = load_movies()
chunk_df = chunk_movies(movies_df)

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
films_per_year = films_per_year[films_per_year.index <= 2025]
st.line_chart(films_per_year)

st.header("☁️ Nuage de mots des synopsis pour les 1000 films les plus populaires")
text = " ".join(chunk_df["overview"].dropna())
wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)
fig, ax = plt.subplots()
ax.imshow(wordcloud, interpolation="bilinear")
ax.axis("off")
st.pyplot(fig)
