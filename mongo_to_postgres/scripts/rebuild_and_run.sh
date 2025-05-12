docker-compose stop db_conversion
docker-compose rm -f db_conversion
docker-compose build db_conversion
docker-compose up -d db_conversion

