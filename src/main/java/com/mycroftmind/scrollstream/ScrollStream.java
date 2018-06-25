/**
 * Copyright (C) ${year} Marvin Herman Froeder (marvin@marvinformatics.com)
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
package com.mycroftmind.scrollstream;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author tomas.valka@gmail.com
 */
public class ScrollStream {
    /**
     * Time to keep alive one scroll
     */
    private static final TimeValue SCROLL_KEEP_ALIVE = TimeValue.timeValueMinutes(1);
    private static final int PAGE_SIZE = 1000;

    public static Stream<SearchHit> create(final SearchRequestBuilder searchRequestBuilder,
            final Client client) {
        final SearchResponse response = searchRequestBuilder
                .setScroll(SCROLL_KEEP_ALIVE)
                .setSize(PAGE_SIZE)
                .execute().actionGet();

        final Spliterator<SearchHit> spliterator = new ElasticsearchScrollSpliterator(new ScrollCharacteristics(client,
                response.getHits().getTotalHits()),
                response);
        return StreamSupport.stream(spliterator, false);
    }

    private static class ElasticsearchScrollSpliterator implements Spliterator<SearchHit> {

        private final ScrollCharacteristics chars;

        // Characteristics of current (this) page
        private SearchResponse searchResponse;
        private ListenableActionFuture<SearchResponse> nextPageFuture;
        private int fence = 0;
        private AtomicInteger origin = new AtomicInteger(0);

        private ElasticsearchScrollSpliterator(final ScrollCharacteristics chars,
                final SearchResponse searchResponse) {
            this.chars = chars;
            this.searchResponse = searchResponse;
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
            this.nextPageFuture = this.chars.client.prepareSearchScroll(this.searchResponse.getScrollId())
                    .setScroll(SCROLL_KEEP_ALIVE)
                    .execute();
        }

        private void setUpNewPage(final SearchHits searchHits) {
            this.origin.set(0);
            this.fence = searchHits.hits().length;
        }

    }

    private static class ScrollCharacteristics {
        final Client client;
        final long totalHits;

        ScrollCharacteristics(final Client client,
                long totalHits) {
            this.client = client;
            this.totalHits = totalHits;
        }
    }

}
