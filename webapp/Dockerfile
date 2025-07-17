FROM python:3.10-slim

WORKDIR /app

COPY webapp/requirements.in .

RUN pip install --no-cache-dir -r requirements.in

COPY webapp/ .

EXPOSE 8501

CMD ["streamlit", "run", "Movie_Recommender.py", "--server.port=8501", "--server.address=0.0.0.0"]
