# Oracle of Bacon
This application is an Oracle of Bacon implementation based on NoSQL data stores :
* ElasticSearch (http) - localhost:9200
* Redis - localhost:6379
* Mongo - localhost:27017
* Neo4J (bolt) - locahost:7687

To build :
```
./gradlew build
```

To Run, execute class *com.serli.oracle.of.bacon.Application*.


## Import data
Data should be located in folder `./src/main/resources/imdb-data`. There should be 3 files : 

- `actors.csv`
- `movies.csv`
- `roles.csv`

To import, run `./import_imdb_data.sh`.
