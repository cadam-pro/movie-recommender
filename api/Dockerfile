# Utilise une image de base Python complète
FROM python:3.10-slim

# Définit le répertoire de travail
WORKDIR /app

# # Copie le code source de l'API
# COPY api/ /app

# Installe Java (version par défaut, souvent Java 11 ou 17) et curl
RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Vérifie la version de Java (optionnel)
RUN java -version

# Installe les dépendances Python
COPY api/requirements.in .
RUN pip install --no-cache-dir -r requirements.in

# # Expose le port de l'API (à adapter si nécessaire)
# EXPOSE 8000
COPY . .

ENV PYTHONPATH=/app
# Commande de lancement de l'API (à adapter selon ton app)
CMD ["uvicorn", "api.webapi:mr_api", "--host", "0.0.0.0", "--port", "8000"]
