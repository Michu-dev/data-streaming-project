docker run --name postgresdb \
-p 8432:5432 \
-e POSTGRES_PASSWORD=secret \
-d postgres

export PGPASSWORD='secret'
psql -h localhost -p 8432 -U postgres -d postgres -f ddl.sql