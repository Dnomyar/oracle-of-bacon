package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.*;


public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static long numberOfLines = 0;

    private static BulkRequest bulkRequest = new BulkRequest();


    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        setMapping(client);

        String inputFilePath = args[0];
        Path path = Paths.get(inputFilePath);

        getNumberOfLines(path);

        indexActors(client, path);

        client.close();
    }


    private static void setMapping(RestHighLevelClient client) throws IOException {

        System.out.println("Setting the mapping...");

        XContentBuilder jsonBuilder = jsonBuilder()
                .startObject()
                    .startObject("mappings")
                        .startObject("actor")
                            .startObject("properties")
                                .startObject("name")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("suggest")
                                    .field("type", "completion")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();



        HttpEntity jsonPayload = new NStringEntity(jsonBuilder.string(), ContentType.APPLICATION_JSON);


        try {

            Response response = client.getLowLevelClient()
                    .performRequest(
                            HttpPut.METHOD_NAME,
                            "imdb",
                            Collections.emptyMap(),
                            jsonPayload
                    );
            System.out.println("Mapping reponse (" + response.getStatusLine() + ")");
        }catch (ResponseException e){

            // TODO : could be cleaner
            boolean doesIndexAlreadyExists =
                    EntityUtils.toString(e.getResponse().getEntity())
                            .contains("resource_already_exists_exception");

            if(doesIndexAlreadyExists){
                System.out.println("Index already exists, skipping...");
            }else{
                throw e;
            }
        }


    }

    private static void getNumberOfLines(Path path) throws IOException {
        numberOfLines = Files.lines(path).count();
    }


    ////////////////////////////////
    // Index actors

    private static void indexActors(RestHighLevelClient client, Path path) throws IOException {

        System.out.println();
        System.out.println("Indexing actors...");

        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {

            bufferedReader.lines()

                    // Get rid of the head which is the header of the csv file
                    .skip(1)

                    //.limit(100)

                    // Remove quote at the beginning and at the end
                    .map(actor -> actor.substring(1, actor.length() - 1))

                    .forEach(actorName -> indexActor(client, actorName));

            runBulk(client, bulkRequest);
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }

    private static void indexActor(RestHighLevelClient client, String actorName) {

        CompletionLoader.count.incrementAndGet();

        try {

            String[] suggestions = concatStringArrays(
                        ngrams(actorName, 2),
                        ngrams(actorName, 1)
                    );


            XContentBuilder json = jsonBuilder()
                    .startObject()
                        .field("name", actorName)
                        .startObject("suggest")
                            .array("input", suggestions)
                        .endObject()
                    .endObject();


            IndexRequest indexRequest = new IndexRequest("imdb", "actor")
                    .source(json);

            addToBulkRequest(client, indexRequest);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    ////////////////////////////////
    // Bulk management

    private static void addToBulkRequest(RestHighLevelClient client, IndexRequest indexRequest){

        long MB_SIZE = 1024 * 1024;

        boolean isSizeOfBulkTooLarge = bulkRequest.estimatedSizeInBytes() > 9 * MB_SIZE;

        if(isSizeOfBulkTooLarge){

            System.out.println("Importing... (" + (int) (((float)count.get() / numberOfLines) * 100) + "%)\r");

            runBulk(client, bulkRequest);
        }

        bulkRequest.add(indexRequest);
    }

    private static void runBulk(RestHighLevelClient client, BulkRequest bulkRequest) {
        try {
            BulkResponse bulk = client.bulk(bulkRequest);
            if(bulk.hasFailures()){
                System.out.println(bulk.buildFailureMessage());
            }
            CompletionLoader.bulkRequest = new BulkRequest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    ///////////////////////////////
    // Utils

    // Function from https://stackoverflow.com/questions/3656762/n-gram-generation-from-a-sentence
    private static String[] ngrams(String s, int len) {
        String[] parts = s.split(" ");
        String[] result = new String[parts.length - len + 1];
        for(int i = 0; i < parts.length - len + 1; i++) {
            StringBuilder sb = new StringBuilder();
            for(int k = 0; k < len; k++) {
                if(k > 0) sb.append(' ');
                sb.append(parts[i+k]);
            }
            result[i] = sb.toString();
        }
        return result;
    }

    private static String[] concatStringArrays(String[] a, String[] b){
        int aLength = a.length;
        int bLength = b.length;
        String[] ngram = (String[]) Array.newInstance(String.class, aLength + bLength);
        System.arraycopy(a, 0, ngram, 0, aLength);
        System.arraycopy(b, 0, ngram, aLength, bLength);
        return ngram;
    }

}
