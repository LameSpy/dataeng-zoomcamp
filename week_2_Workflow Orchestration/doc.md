<!-- 
For installation Prefect i used requirements.txt file
in main folder also have two file - ingest_data_old.py and ingest_data_prefect.py. It is the same things but in second file we used Prefect
 -->


# Prefect
# registrate blocks in Prefect
prefect block register -m prefect_gcp

# Launch UI of prefect
prefect orion start

# Deploiment Prefect flow with CLI
# it will create deploiment yaml file
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
# in will deploy in prefect API
prefect deployment apply etl_parent_flow-deployment.yaml