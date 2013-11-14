/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.Indices2416FilterBuilder;
import org.elasticsearch.index.query.Indices2416QueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.hasChildFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.matchers.JUnitMatchers.either;

/**
 *
 */
// The following annotation slows down tests a lot, but prevents:
// java.lang.NoSuchMethodError: org.elasticsearch.common.settings.Settings.getAsMap()Lcom/google/common/collect/ImmutableMap;
//     at org.elasticsearch.test.ElasticsearchIntegrationTest.after(ElasticsearchIntegrationTest.java:225)
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class SimpleQueryTests extends ElasticsearchIntegrationTest {

    /**
     * A query that will execute the wrapped query only for the specified indices, and "match_all" when
     * it does not match those indices.
     */
    public static Indices2416QueryBuilder indices2416Query(QueryBuilder queryBuilder, String... indices) {
        return new Indices2416QueryBuilder(queryBuilder, indices);
    }

    public static Indices2416FilterBuilder indices2416Filter(FilterBuilder filter, String... indices) {
        return new Indices2416FilterBuilder(filter, indices);
    }

    @Test
    public void testindices2416Query() throws Exception {
        createIndex("index1", "index2", "index3");
        ensureGreen();

        client().prepareIndex("index1", "type1").setId("1").setSource("text", "value1").get();
        client().prepareIndex("index2", "type2").setId("2").setSource("text", "value2").get();
        client().prepareIndex("index3", "type3").setId("3").setSource("text", "value3").get();
        refresh();

        SearchResponse response = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indices2416Query(matchQuery("text", "value1"), "index1")
                        .noMatchQuery(matchQuery("text", "value2"))).get();
        assertHitCount(response, 2l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        //default no match query is match_all
        response = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indices2416Query(matchQuery("text", "value1"), "index1")).get();
        assertHitCount(response, 3l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(2).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));

        response = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indices2416Query(matchQuery("text", "value1"), "index1")
                        .noMatchQuery("all")).get();
        assertHitCount(response, 3l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(2).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));

        response = client().prepareSearch("index1", "index2", "index3")
                .setQuery(indices2416Query(matchQuery("text", "value1"), "index1")
                        .noMatchQuery("none")).get();
        assertHitCount(response, 1l);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
    }

    @Test
    public void testindices2416Filter() throws Exception {
        createIndex("index1", "index2", "index3");
        ensureGreen();

        client().prepareIndex("index1", "type1").setId("1").setSource("text", "value1").get();
        client().prepareIndex("index2", "type2").setId("2").setSource("text", "value2").get();
        client().prepareIndex("index3", "type3").setId("3").setSource("text", "value3").get();
        refresh();

        SearchResponse response = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indices2416Filter(termFilter("text", "value1"), "index1")
                        .noMatchFilter(termFilter("text", "value2"))).get();
        assertHitCount(response, 2l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        //default no match filter is "all"
        response = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indices2416Filter(termFilter("text", "value1"), "index1")).get();
        assertHitCount(response, 3l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(2).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));

        response = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indices2416Filter(termFilter("text", "value1"), "index1")
                        .noMatchFilter("all")).get();
        assertHitCount(response, 3l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));
        assertThat(response.getHits().getAt(2).getId(), either(equalTo("1")).or(equalTo("2")).or(equalTo("3")));

        response = client().prepareSearch("index1", "index2", "index3")
                .setFilter(indices2416Filter(termFilter("text", "value1"), "index1")
                        .noMatchFilter("none")).get();
        assertHitCount(response, 1l);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
    }

    @Test // https://github.com/elasticsearch/elasticsearch/issues/2416
    public void testIndices2416QuerySkipParsing() throws Exception {
        createIndex("simple");
        client().admin().indices().prepareCreate("related")
                .addMapping("child", jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent")
                        .endObject().endObject().endObject())
                .get();
        ensureGreen();

        client().prepareIndex("simple", "lone").setId("1").setSource("text", "value1").get();
        client().prepareIndex("related", "parent").setId("2").setSource("text", "parent").get();
        client().prepareIndex("related", "child").setId("3").setParent("2").setSource("text", "value2").get();
        refresh();

        //has_child fails if executed on "simple" index
        try {
            client().prepareSearch("simple")
                    .setQuery(hasChildQuery("child", matchQuery("text", "value"))).get();
            fail("Should have failed as has_child query can only be executed against parent-child types");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.reason(), containsString("No mapping for for type [child]"));
            }
        }

        //has_child doesn't get parsed for "simple" index
        SearchResponse response = client().prepareSearch("related", "simple")
                .setQuery(indices2416Query(hasChildQuery("child", matchQuery("text", "value2")), "related")
                        .noMatchQuery(matchQuery("text", "value1"))).get();
        assertHitCount(response, 2l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));
    }


    @Test // https://github.com/elasticsearch/elasticsearch/issues/2416
    public void testIndices2416FilterskipParsing() throws Exception {
        createIndex("simple");
        client().admin().indices().prepareCreate("related")
                .addMapping("child", jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent")
                        .endObject().endObject().endObject())
                .get();
        ensureGreen();

        client().prepareIndex("simple", "lone").setId("1").setSource("text", "value1").get();
        client().prepareIndex("related", "parent").setId("2").setSource("text", "parent").get();
        client().prepareIndex("related", "child").setId("3").setParent("2").setSource("text", "value2").get();
        refresh();

        //has_child fails if executed on "simple" index
        try {
            client().prepareSearch("simple")
                    .setFilter(hasChildFilter("child", termFilter("text", "value1"))).get();
            fail("Should have failed as has_child query can only be executed against parent-child types");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.reason(), containsString("No mapping for for type [child]"));
            }
        }

        SearchResponse response = client().prepareSearch("related", "simple")
                .setFilter(indices2416Filter(hasChildFilter("child", termFilter("text", "value2")), "related")
                        .noMatchFilter(termFilter("text", "value1"))).get();
        assertHitCount(response, 2l);
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));
    }

}
