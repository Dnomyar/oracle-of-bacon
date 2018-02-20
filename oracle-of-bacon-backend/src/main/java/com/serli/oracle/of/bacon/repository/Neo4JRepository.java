package com.serli.oracle.of.bacon.repository;


import com.google.common.base.Function;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.util.*;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4jneo4j"));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String targetActorName) {

        List<GraphItem> resultsList = new ArrayList<>();

        try (Session session = driver.session()){

            Map<String, Object> params = new HashMap<>();
            params.put("sourceActor", "Bacon, Kevin (I)");
            params.put("targetActor", targetActorName);

            String request =
                    "MATCH (sourceActor:Actor {name: {sourceActor}}), " +
                            "(targetActor:Actor {name: {targetActor}}), " +
                            "p = shortestPath((sourceActor)-[*]-(targetActor))\n" +
                            "WITH p\n" +
                            "WHERE length(p)> 1\n" +
                            "RETURN p";


            System.out.println("Executing request to get connections to Kevin Bacon...");

            StatementResult statementResult = session.run(request, params);

            while (statementResult.hasNext()){
                resultsList.addAll(transformRecordToGraphItem(statementResult.next()));
            }
        }catch (Exception e){
            System.out.println("Exception occurred : " + e.getMessage());
            e.printStackTrace();
        }

        return resultsList;
    }

    private List<GraphItem> transformRecordToGraphItem(Record record) {

        List<GraphItem> resultsList = new ArrayList<>();

        record.values().forEach(value -> {

            Path segments = value.asPath();

            segments.nodes().forEach(node -> {
                resultsList.add(transformNodeToGraphNode(node));
            });

            segments.relationships().forEach(relationship -> {
                resultsList.add(transformRelationshipToGraphEdge(relationship));
            });
        });

        return resultsList;
    }

    private GraphNode transformNodeToGraphNode(Node node){
        long id = node.id();
        String label = node.labels().iterator().next(); // TODO not clean, could fail
        String graphNodeTypeKey = "Movie".equals(label) ? "title" : "name";
        String value = (String) node.asMap().get(graphNodeTypeKey); // TODO not clean, could fail
        return new GraphNode(id, value, label);
    }

    private GraphItem transformRelationshipToGraphEdge(Relationship relationship) {
        long id = relationship.id();
        long source = relationship.startNodeId();
        long target = relationship.endNodeId();
        String value = relationship.type();
        return new GraphEdge(id, source, target, value);
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
