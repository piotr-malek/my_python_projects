Hi, I'm Piotr, I'm an analytics engineer. Here are some of my personal projects:

- [Disaster Risk Predictor](https://github.com/piotr-malek/my_python_projects/tree/main/disaster_predictor)
  - Helps monitor multi-hazard disaster risk (flood, fire, drought, landslide) across 250+ regions around the world. Trained per-hazard ML models score risk from Earth Engine and OpenMeteo-style inputs into BigQuery. Airflow automates the full daily chain with no manual steps: ingestion → ML risk assessment → LLM interpretation of weather outlooks, plus a Streamlit app for users to monitor risks.
  - Python, ML (trained hazard models), BigQuery, Airflow (ingest → assess → interpret), Earth Engine, Streamlit, LLMs

- [Strava Personal Dashboard](https://github.com/piotr-malek/my_python_projects/blob/main/strava_activities.ipynb)
  - Gives a single, always-updated view of personal training history instead of juggling the Strava app alone. Pulls activities via the Strava API, shapes the data, loads Google Sheets, and feeds a Looker Studio dashboard with automated refresh.
  - OAuth, APIs, Google Sheets automation, Looker Studio

- [Bird Migration Analysis](https://github.com/piotr-malek/my_python_projects/tree/main/birding)
  - Explores whether migration timing and routes line up with weather so birding and forecasting questions can be grounded in data. Ingests eBird and OpenMeteo, models and transforms in BigQuery with dbt, and surfaces patterns relevant to predicting migration windows.
  - APIs, dbt, BigQuery, SQL, data modeling

- [Garmin AI Training Coach](https://github.com/piotr-malek/my_python_projects/tree/main/garmin_ai)
  - Turns disconnected Garmin metrics into coaching-style guidance: what workouts meant for fitness, how recovery looks, and how to tweak plans. Uses the Garmin API plus Gemini over activity, sleep, and health history, with optional Google Sheets as a scratchpad.
  - APIs, LLMs (Gemini), Google Sheets

- [Trello Card Automation](https://github.com/piotr-malek/my_python_projects/tree/main/trello_fetching_cards)
  - Reliable batch retrieval of Trello cards for reporting or downstream automation, with explicit error handling and tests so flaky networks do not silently drop work.
  - APIs, error handling, unit testing

- [Urban Heat Island (Singapore)](https://github.com/piotr-malek/my_python_projects/blob/main/uhi_singapore.ipynb)
  - Surfaces where and how urban areas run hotter than their surroundings using API-sourced environmental inputs, then explores and charts Singapore-specific heat patterns for insight rather than a single headline number.
  - APIs, data wrangling, visualization
