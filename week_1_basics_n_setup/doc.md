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