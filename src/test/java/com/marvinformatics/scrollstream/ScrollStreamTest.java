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

import static org.hamcrest.Matchers.equalTo;

import com.marvinformatics.scrollstream.ScrollStream.ScrollSettings;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ScrollStreamTest extends ESIntegTestCase {

    @Test
    public void streamMethod() {
        final SearchRequestBuilder request = client().prepareSearch("shakespeare")
                .setQuery(QueryBuilders.matchAllQuery());

        final AtomicInteger counter = new AtomicInteger(0);

        ScrollStream.create(client(),
                // this settings will force 10 trips to ES
                new ScrollSettings(TimeValue.timeValueMinutes(1), 10),
                request)
                .parallel()
                .map(Example::sourceToExample)
                .forEach(example -> counter.incrementAndGet());

        assertThat(counter.get(), equalTo(100));
    }

    @Before
    public void setup() {
        client().admin().indices().prepareCreate("shakespeare").execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            final Map<String, Object> source = new HashMap<>();
            source.put("line_number", String.valueOf(i));
            source.put("speaker", "coach");
            source.put("play_name", String.valueOf(i % 7));
            client().prepareIndex("shakespeare", "type1", Integer.toString(i)).setSource(source).execute().actionGet();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @After
    public void cleanup() {
        client().admin().indices().prepareDelete("shakespeare").execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
    }

}
