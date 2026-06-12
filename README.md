Hi, I'm Piotr, I'm an analytics engineer. Here are some of my personal projects:

- [Disaster Risk Predictor](https://github.com/piotr-malek/my_python_projects/tree/main/disaster_predictor)
  - Helps monitor multi-hazard disaster risk (flood, fire, drought, landslide) across 250+ regions around the world. Trained per-hazard ML models score risk from Earth Engine and OpenMeteo-style inputs into BigQuery. Airflow automates the full daily chain with no manual steps: ingestion → ML risk assessment → LLM interpretation of weather outlooks, plus a Streamlit app for users to monitor risks.
  - Python, ML (trained hazard models), BigQuery, Airflow (ingest → assess → interpret), Earth Engine, Streamlit, LLMs

- [Bird Migration Analysis](https://github.com/piotr-malek/my_python_projects/tree/main/birding)
  - Explores whether migration timing and routes line up with weather so birding and forecasting questions can be grounded in data. Ingests eBird and OpenMeteo, models and transforms in BigQuery with dbt, and surfaces patterns relevant to predicting migration windows.
  - APIs, dbt, BigQuery, SQL, data modeling

- [Garmin AI Training Coach](https://github.com/piotr-malek/my_python_projects/tree/main/garmin_ai)
  - Turns disconnected Garmin metrics into coaching-style guidance: what workouts meant for fitness, how recovery looks, and how to tweak plans. Uses the Garmin API plus Gemini over activity, sleep, and health history, with optional Google Sheets as a scratchpad.
  - APIs, LLMs (Gemini), Google Sheets

- [Daily Health Monitor](https://github.com/piotr-malek/my_python_projects/tree/main/daily_health_monitor)
  - Sends a morning email digest on sleep, stress, recovery, and how you're likely to feel today — with conservative, evidence-bound suggestions. Pulls wellness from Garmin Connect, optionally layers in training from Strava (handy when you mix devices), stores history in BigQuery, and writes the narrative with a local Ollama model. Built around daily wellbeing first; training load is context, not the headline.
  - Python, Garmin Connect API, Strava API, BigQuery, Ollama, SMTP

- [Trello Card Automation](https://github.com/piotr-malek/my_python_projects/tree/main/trello_fetching_cards)
  - Reliable batch retrieval of Trello cards for reporting or downstream automation, with explicit error handling and tests so flaky networks do not silently drop work.
  - APIs, error handling, unit testing