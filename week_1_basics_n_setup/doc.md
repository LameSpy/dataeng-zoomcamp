Запустить локальный Postgres
Для запуска многострочного кода используется `
docker run -it `
    -e POSTGRES_USER="root" `
    -e POSTGRES_PASSWORD="root" `
    -e POSTGRES_DB="ny_taxi" `
    -v C:\Users\Дмитрий\WorkFolder\Программирование\GitHub\dataeng-zoomcamp\week_1_basics_n_setup\ny_taxi_postgres_data:/var/lib/postgresql/data `
    -p 5432:5432 `
    postgres:13 `

For testing database i use pgcli. it is a python library. Fow windows also need to install comand line tools for postgres in this site https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

For connection to database i am use this
pgcli -h localhost -p 5432 -u root -d ny_taxi 

For download file i am use thit
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz

Command for pgcli
\dt - show all tables
\d - show all columns of table

------Create docker containers in general network. It is just example
1) create network
docker network create pg-network

2) set created network to config of docker containers
docker run -it `
    -e POSTGRES_USER="root" `
    -e POSTGRES_PASSWORD="root" `
    -e POSTGRES_DB="ny_taxi" `
    -v C:\Users\Дмитрий\WorkFolder\Программирование\GitHub\dataeng-zoomcamp\week_1_basics_n_setup\ny_taxi_postgres_data:/var/lib/postgresql/data `
    -p 5432:5432 `
    --network=pg-network `
    --name pg-database `
    postgres:13 `

create container pgadmin in same network
docker run -it `
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" `
    -e PGADMIN_DEFAULT_PASSWORD="root" `
    -p 8080:80 `
    -- network = pg-network `
    -- name pgadmin `
    dpage/pgadmin4`



URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz'

# call python script with parameters
python ingest_data.py `
 --host=localhost `
 --port='5432' `
 --db=ny_taxi `
 --user=root `
 --password=root `
 --url_csv=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz `
 --table='yellow_taxi_trips' `
    
# docker 
docker build -t taxi_ingest:v001 .       

# run docker with python script
docker run -it `
--network=pg-network `
taxi_ingest:v003 `
    --host=pg-database `
    --port='5432' `
    --db=ny_taxi `
    --user=root `
    --password=root `
    --url_csv=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz `
    --table='yellow_taxi_trips' `


# GCP config
# for connet to gcp from cli you need to use these comand
gcloud auth activate-service-account --key-file %GOOGLE_APPLICATION_CREDENTIALS%
# after that you can check your servises account in GCP
gcloud auth list
# for choose some of account, use this command
gcloud config set account dc-service@datacamp-378414.iam.gserviceaccount.com

# command for terraform
datacamp-378414
terraform plan `
-var 'BQ_DATASET=datatolk' `
-var 'storage_class=dsf' `