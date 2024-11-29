#!/usr/bin/env just --justfile
set dotenv-load := true

watch_test:
    @just db_run &>/dev/null&
    @just wait-for-db
    watchexec --restart --verbose --clear --wrap-process=session --stop-signal SIGTERM --exts gleam --watch ./ -- "gleam test"

test:
    @just db_run &>/dev/null&
    @just wait-for-db
    gleam test


# DB
squirrel:
    gleam run -m squirrel

db_run:
    docker run \
     --rm \
     -p $DB_HOST_PORT:5432 \
     -e POSTGRES_PASSWORD=$DB_PASSWORD \
     -e POSTGRES_USER=$DB_USER \
     --name $DB_CONTAINER_NAME \
     postgres:14.1

db_inspect *ARGS:
    psql $DATABASE_URL {{ARGS}}

wait-for-db:
    until psql $DATABASE_URL -c '\q' 2>/dev/null; do echo "Waiting for database..."; sleep 2; done; echo "Database is up!"

