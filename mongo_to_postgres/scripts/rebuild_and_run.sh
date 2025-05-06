docker-compose stop mongo_to_postgres
docker-compose rm -f mongo_to_postgres
docker-compose build mongo_to_postgres
docker-compose up -d mongo_to_postgres

