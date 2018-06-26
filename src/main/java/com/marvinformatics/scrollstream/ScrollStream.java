/**
 * Copyright (C) 2018 Marvin Herman Froeder (marvin@marvinformatics.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marvinformatics.scrollstream;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Convert an elastic {@link SearchResponse} into a java 8 {@link Stream}
 *
 * @author velo dot br at gmail dot com
 * @author tomas.valka@gmail.com
 */
public class ScrollStream {

    private static final Logger LOG = LoggerFactory.getLogger(ScrollStream.class);

    /**
     * Settings to be used when configuring {@link SearchRequestBuilder}
     */
    public static class ScrollSettings {

        private final TimeValue scrollKeepAlive;
        private final int pageSize;

        /**
         * @param scrollKeepAlive
         *            Time to keep alive one scroll
         * @param pageSize
         *            how many documents should be loaded in one scroll page
         */
        public ScrollSettings(TimeValue scrollKeepAlive, int pageSize) {
            this.scrollKeepAlive = scrollKeepAlive;
            this.pageSize = pageSize;
        }

        @Override
        public String toString() {
            return "ScrollSettings [scrollKeepAlive=" + scrollKeepAlive + ", pageSize=" + pageSize + "]";
        }

    }

    /**
     * Create a new {@link Stream} based on a {@link SearchRequestBuilder}
     *
     * @param client
     *            to connect to elastic search
     * @param searchRequestBuilder
     *            base search query that will be transmuted into a {@link Stream}
     * @return a new {@link Stream} instance
     */
    public static Stream<SearchHit> create(final Client client, final SearchRequestBuilder searchRequestBuilder) {
        return create(client, new ScrollSettings(TimeValue.timeValueMinutes(1), 1000), searchRequestBuilder);
    }

    /**
     * Create a new {@link Stream} based on a {@link SearchRequestBuilder}
     *
     * @param client
     *            to connect to elastic search
     * @param scrollSettings
     *            with pagination information for {@link SearchScrollRequestBuilder}
     * @param searchRequestBuilder
     *            base search query that will be transmuted into a {@link Stream}
     * @return a new {@link Stream} instance
     */
    public static Stream<SearchHit> create(Client client, ScrollSettings scrollSettings, SearchRequestBuilder searchRequestBuilder) {
        LOG.debug("Creating new stream with scrollSettings={}", scrollSettings);

        final SearchResponse response = searchRequestBuilder
                .setScroll(scrollSettings.scrollKeepAlive)
                .setSize(scrollSettings.pageSize)
                .execute()
                .actionGet();

        LOG.debug("Elastic scroll results totalHits={}", response.getHits().getTotalHits());

        final Spliterator<SearchHit> spliterator = new ElasticsearchScrollSpliterator(
                client,
                response,
                scrollSettings);

        return StreamSupport.stream(spliterator, false);
    }

    private static class ElasticsearchScrollSpliterator implements Spliterator<SearchHit> {

        private final Client client;
        private final ScrollSettings scrollSettings;

        // Characteristics of current (this) page
        private SearchResponse searchResponse;
        private ListenableActionFuture<SearchResponse> nextPageFuture;
        private int fence = 0;
        private final AtomicInteger origin = new AtomicInteger(0);

        private ElasticsearchScrollSpliterator(Client client, SearchResponse searchResponse, ScrollSettings scrollSettings) {
            this.client = client;
            this.searchResponse = searchResponse;
            this.scrollSettings = scrollSettings;
            this.setUpNewPage(this.searchResponse.getHits());
            this.fetchDocuments();
        }

        @Override
        public boolean tryAdvance(Consumer<? super SearchHit> action) {
            final int actualOrigin = this.origin.getAndIncrement();
            if (actualOrigin < this.fence) {
                action.accept(this.searchResponse.getHits().getAt(actualOrigin));
                return true;
            }
            this.searchResponse = this.getNextResponse();
            this.setUpNewPage(this.searchResponse.getHits());
            this.fetchDocuments();
            if (isAtEnd()) {
                return false;
            }
            return this.tryAdvance(action);
        }

        @Override
        public Spliterator<SearchHit> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return Spliterator.IMMUTABLE;
        }

        private boolean isAtEnd() {
            return this.searchResponse.getHits().getHits().length == 0;
        }

        private SearchResponse getNextResponse() {
            return nextPageFuture.actionGet();
        }

        private void fetchDocuments() {
            LOG.debug("Fetching next page scrollId={}", this.searchResponse.getScrollId());

            this.nextPageFuture = this.client
                    .prepareSearchScroll(this.searchResponse.getScrollId())
                    .setScroll(scrollSettings.scrollKeepAlive)
                    .execute();
        }

        private void setUpNewPage(final SearchHits searchHits) {
            this.origin.set(0);
            this.fence = searchHits.hits().length;
        }

    }

}
