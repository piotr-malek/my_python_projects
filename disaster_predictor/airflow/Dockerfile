FROM astrocrpublic.azurecr.io/runtime:3.1-10

# For astro dev start: Mounted volumes handle file access, symlinks work
# For astro build/deploy: Run 'make sync' first to convert symlinks to files
# Add include and airflow root to PYTHONPATH so modules are importable
ENV PYTHONPATH="/usr/local/airflow:/usr/local/airflow/include:${PYTHONPATH}"
