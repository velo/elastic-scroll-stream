package com.mycroftmind;

import com.mycroftmind.document.Example;
import com.mycroftmind.scrollstream.ScrollStream;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private final Client client;

    private static AtomicInteger counter = new AtomicInteger(0);

    public Main(final Client client) {
        this.client = client;
    }

    public static void main(String[] args) throws UnknownHostException {

        final long startMilis = System.currentTimeMillis();
        final Main thisMain = new Main(Main.client());

        System.out.println("Running collection...");
        thisMain.collectionMethod();
        System.out.println("Counter says: " + Main.counter);
        System.out.println("That took: " + (System.currentTimeMillis() - startMilis));

        System.out.println();
        counter.set(0);

        System.out.println("Running stream...");
        thisMain.streamMethod();
        System.out.println("Counter says: " + Main.counter);
        System.out.println("That took: " + (System.currentTimeMillis() - startMilis));
    }

    public void streamMethod() {

        final SearchRequestBuilder request = client.prepareSearch("shakespeare")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1000)
                .setScroll(TimeValue.timeValueMinutes(1));

        ScrollStream.create(request, client)
                .parallel()
                .map(Example::sourceToExample)
                .forEach(this::forEachMethod);
    }

    public void collectionMethod() {

        final SearchRequestBuilder request = client.prepareSearch("shakespeare")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1000)
                .setScroll(TimeValue.timeValueMinutes(1));

        final SearchResponse response = client.search(request.request()).actionGet();

        SearchHits hits = response.getHits();
        final Set<Example> examples = new HashSet<>();
        while (hits.getHits().length != 0) {
            for (SearchHit sh : hits.getHits()) {
                examples.add(Example.sourceToExample(sh));
            }

            hits = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet().getHits();
        }

        examples.stream()
                .parallel()
                .forEach(this::forEachMethod);
    }

    private void forEachMethod(Example e) {
        counter.incrementAndGet();
    }

    public static Client client() throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

}
