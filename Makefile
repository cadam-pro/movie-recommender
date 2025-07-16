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
