package org.elasticsearch.test.integration;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BaseESTest {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    @BeforeClass
    protected void setupServer() {
        node = nodeBuilder().local(true).settings(settingsBuilder()
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")
                .put("index.numberOfReplicas", 0)
                .put("index.numberOfShards", 1)
        ).node();
    }

    @AfterClass
    protected void closeServer() {
        node.close();
    }

    @BeforeMethod
    protected void createIndex() {
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    @AfterMethod
    protected void deleteIndex() {
        logger.info("deleting all indices");
        node.client().admin().indices().delete(deleteIndexRequest("*")).actionGet();
    }

    protected Client client() {
        return node.client();
    }

}
