# When will exception. launch will stop
set -e

# Give var for var it can allow you call this bash scripts with paramrters
TAXI_TYPE=$1
YEAR=$2

# Example of URL
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


for MONTH in {1..12}; do
    FMONTH=`printf "%02d" $MONTH`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    LOCAL_PREFIX="/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX}
    wget $URL -O ${LOCAL_PATH}

    echo "download finish"    
    # use GZIP if there were csc files, but i have alredy gz files
    # gzip ${LOCAL_PATH}
done