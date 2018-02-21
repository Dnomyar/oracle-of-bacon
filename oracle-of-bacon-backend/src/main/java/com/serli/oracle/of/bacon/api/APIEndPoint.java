package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.broker.BroadcastBroker;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;
import net.codestory.http.convert.TypeConvert;
import net.codestory.http.errors.NotFoundException;
import org.bson.Document;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    private final BroadcastBroker baconToBroker;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();

        baconToBroker = new BroadcastBroker();
        setUpConsumers();
    }

    private void setUpConsumers(){
        baconToBroker.addConsumer(baconToEvent -> {
            redisRepository.addLastSearch(baconToEvent.param);
        });
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {

        baconToBroker.publishEvent(new BroadcastBroker.Event("BACON_TO", actorName));

        List<Neo4JRepository.GraphItem> connectionsToKevinBacon = neo4JRepository.getConnectionsToKevinBacon(actorName);

        return TypeConvert.toJson(
                connectionsToKevinBacon.stream()
                    .map(this::wrapGraphItemToDataObject)
                    .collect(Collectors.toList())
        );

    }


    /**
     * Basically turns : {...}
     *
     * to : {
     *     data: {...}
     * }
     */
    private HashMap<String, Neo4JRepository.GraphItem> wrapGraphItemToDataObject(Neo4JRepository.GraphItem graphItem) {
        HashMap<String, Neo4JRepository.GraphItem> stringGraphItemHashMap = new HashMap<>();
        stringGraphItemHashMap.put("data", graphItem);
        return stringGraphItemHashMap;
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return elasticSearchRepository.getActorsSuggests(searchQuery);
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        return mongoDbRepository.getActorByName(actorName)
                .map(Document::toJson)
                .orElseThrow(NotFoundException::new);
    }
}
