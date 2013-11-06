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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.Indices2416FilterBuilder;
import org.elasticsearch.index.query.Indices2416QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.BaseESTest;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleQueryTests extends BaseESTest {

    @Test
    public void testIndicesQuery() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("index1")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin()
                .indices()
                .prepareCreate("index2")
                .addMapping("type2", jsonBuilder().startObject().startObject("type2").endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();

        client().prepareIndex("index1", "type1")
                .setId("1")
                .setSource(jsonBuilder().startObject().field("text", "value").endObject())
                .execute().actionGet();
        client().prepareIndex("index2", "type2")
                .setId("2")
                .setSource(jsonBuilder().startObject().field("text", "value").endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh("index1", "index2")
                .execute().actionGet();

        SearchResponse response = client().prepareSearch("index1", "index2")
                .setQuery(QueryBuilders.indicesQuery(matchQuery("text", "value"), "index1")
                        .noMatchQuery(matchQuery("text", "value"))
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        response = client().prepareSearch("index1", "index2")
                .setQuery(QueryBuilders.indicesQuery(matchQuery("text", "value"), "index1")
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        response = client().prepareSearch("index1", "index2")
                .setQuery(QueryBuilders.indicesQuery(matchQuery("text", "value"), "index1")
                        .noMatchQuery("all")
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        response = client().prepareSearch("index1", "index2")
                .setQuery(QueryBuilders.indicesQuery(matchQuery("text", "value"), "index1")
                        .noMatchQuery("none")
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

    }

    @Test // https://github.com/elasticsearch/elasticsearch/issues/2416
    public void testIndicesQueryHideParsingExceptions() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("simple")
                .addMapping("lone", jsonBuilder().startObject().startObject("lone").endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin()
                .indices()
                .prepareCreate("related")
                .addMapping("parent", jsonBuilder().startObject().startObject("parent").endObject().endObject())
                .addMapping(
                        "child",
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();

        client().prepareIndex("simple", "lone")
                .setId("1")
                .setSource(jsonBuilder().startObject().field("text", "value").endObject())
                .execute().actionGet();
        client().prepareIndex("related", "parent")
                .setId("2")
                .setSource(jsonBuilder().startObject().field("text", "parent").endObject())
                .execute().actionGet();
        client().prepareIndex("related", "child")
                .setId("3")
                .setParent("2")
                .setSource(jsonBuilder().startObject().field("text", "value").endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh("simple", "related")
                .execute().actionGet();

        SearchResponse response = client().prepareSearch("related")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));

        response = client().prepareSearch("simple")
                .setQuery(matchQuery("text", "value")).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        // Can't test the following because the raised error (attested by the failed shard), makes the test fail
        //response = client().prepareSearch("simple")
        //        .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).execute().actionGet();
        //assertThat(response.getFailedShards(), equalTo(1));
        //assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("related", "simple")
                .setQuery(new Indices2416QueryBuilder(matchQuery("text", "parent"), "related")
                        .noMatchQuery(matchQuery("text", "value"))
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        response = client().prepareSearch("related", "simple")
                .setQuery(new Indices2416QueryBuilder(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")), "related")
                        .noMatchQuery(matchQuery("text", "value"))
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));

        response = client().prepareSearch("related", "simple")
                .setFilter(new Indices2416FilterBuilder(FilterBuilders.hasChildFilter("child", termFilter("text", "value")), "related")
                        .noMatchFilter(termFilter("text", "value"))
                ).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).getId(), either(equalTo("1")).or(equalTo("2")));
        assertThat(response.getHits().getAt(1).getId(), either(equalTo("1")).or(equalTo("2")));
    }
}
