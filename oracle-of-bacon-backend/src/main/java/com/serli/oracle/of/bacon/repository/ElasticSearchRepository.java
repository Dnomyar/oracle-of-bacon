package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {

        System.out.println("Getting actors suggest from token '" + searchQuery + "'");

        RestHighLevelClient client = createClient();

        String SUGGESTION_NAME = "suggest_actor_name";
        String ACTOR_NAME_FIELD = "name";
        String SUGGEST_FIELD = "suggest";


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder completionSuggestionBuilder =
                SuggestBuilders.completionSuggestion(SUGGEST_FIELD).text(searchQuery);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(SUGGESTION_NAME, completionSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);


        List<String> suggestions = new LinkedList<>();

        CompletionSuggestion termSuggestion =
                searchResponse.getSuggest().getSuggestion(SUGGESTION_NAME);

        for (CompletionSuggestion.Entry entry : termSuggestion.getEntries()) {
            for (CompletionSuggestion.Entry.Option option : entry.getOptions()) {

                String suggestText =
                        option
                                .getHit()
                                .getSourceAsMap()
                                .get(ACTOR_NAME_FIELD)
                                .toString();

                suggestions.add(suggestText);
            }
        }

        return suggestions;
    }
}
