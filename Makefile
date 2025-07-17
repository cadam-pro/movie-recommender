.PHONY: tests
tests :
	@echo "Running tests..."
	export PYTHONPATH=src/movie_recommender  ;\
	echo $(PYTHONPATH) ;\
	pytest tests/

uvicorn :
	@echo "Running uvicorn server..."
	export PYTHONPATH=src/movie_recommender  ;\
	echo $(PYTHONPATH) ;\
	uvicorn api.webapi:mr_api --reload

streamlit :
	@echo "Running streamlit server..."
	streamlit run webapp/Movie_Recommender.py

airflow:
	export PYTHONPATH=src/movie_recommender  ;\
	export PYTHONPATH=src  ;\
	export AIRFLOW__CORE__LOAD_EXAMPLES=False \
	export AIRFLOW_HOME=$(pwd)/.airflow
	airflow standalone
