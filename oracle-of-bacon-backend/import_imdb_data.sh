#!/usr/bin/env bash

# imports movies, actors and relationships between them
# needs to specify database that corresponds to configuration of neo4j
neo4j-admin import \
    --database=imdb.db \
    --nodes:Movie ./src/main/resources/imdb-data/movies.csv \
    --nodes:Actor ./src/main/resources/imdb-data/actors.csv \
    --relationships ./src/main/resources/imdb-data/roles.csv