package com.exo.bch.es;

import static org.hamcrest.core.Is.is;

import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 * exo@exoplatform.com
 * 9/25/15
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class ParentChildTest extends ElasticsearchIntegrationTest {

    /**
     * Configuration of the ES integration tests
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(RestController.HTTP_JSON_ENABLE, true)
                .put(InternalNode.HTTP_ENABLED, true)
                .put("network.host", "127.0.0.1")
                .put("path.data", "target/data")
                .build();
    }

    @Before
    public void waitForNodes() {
        internalCluster().ensureAtLeastNumDataNodes(4);
        assertEquals("All nodes in cluster should have HTTP endpoint exposed", 4, cluster().httpAddresses().length);
    }

    @Test
    public void searchProfileByConnection_NoConnections() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        indexProfile("Frederic");
        //When
        SearchResponse response = getRequestBuilder("BCH","Frederic").execute().actionGet();
        //Then
        assertThat(response.getHits().getTotalHits(), is(0L));
    }

    @Test
    public void searchProfileByConnection_1Connection() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        indexProfile("Frederic");
        addConnection("Frederic", "BCH");
        //When
        SearchResponse response = getRequestBuilder("BCH","Frederic").execute().actionGet();
        //Then
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    @Test
    public void searchProfileByConnection_2Connections() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        indexProfile("Frederic");
        addConnection("Frederic", "BCH");
        addConnection("Frederic", "TCL");
        //When
        SearchResponse response = getRequestBuilder("BCH","Frederic").execute().actionGet();
        //Then
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    @Test
    public void searchProfileByConnectionAndName_2Connections() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        indexProfile("Frederic");
        addConnection("Frederic", "BCH");
        addConnection("Frederic", "PAR");
        indexProfile("Philippe");
        addConnection("Philippe", "BCH");
        addConnection("Philippe", "FDR");
        indexProfile("Thibault");
        addConnection("Thibault", "PAR");
        //When
        SearchResponse responseBCH = getRequestBuilder("BCH","Frederic").execute().actionGet();
        SearchResponse responsePAR = getRequestBuilder("BCH","Philippe").execute().actionGet();
        SearchResponse responseFDR = getRequestBuilder("BCH","Thibault").execute().actionGet();
        //Then
        assertThat(responseBCH.getHits().getTotalHits(), is(1L));
        assertThat(responsePAR.getHits().getTotalHits(), is(1L));
        assertThat(responseFDR.getHits().getTotalHits(), is(0L)); //No part of BCH connections
    }

    private SearchRequestBuilder getRequestBuilder(String currentUserName, String connectionName) {
        return client().prepareSearch("connections")
                .setTypes("profile")
                .setQuery(QueryBuilders.termQuery("name", connectionName))             // Query
                .setPostFilter(FilterBuilders.hasChildFilter("connection",
                        FilterBuilders.orFilter(
                                FilterBuilders.termFilter("sender", currentUserName),
                                FilterBuilders.termFilter("receiver", currentUserName))))   // Filter
//                .setFrom(0).setSize(60).setExplain(true)
        ;
    }

    private void indexProfile(String userName) {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(client().prepareIndex("connections", "profile", userName)
                .setSource("{ \"name\" : \"" + userName + "\" }"));
        bulkRequest.execute().actionGet();
        //  Sync
        admin().indices().prepareRefresh().execute().actionGet();
    }

    private void addConnection(String userName, String connection) {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(client().prepareIndex("connections", "connection")
                .setSource("{ \"sender\" : \""+connection+"\" }")
                .setParent(userName));
        bulkRequest.execute().actionGet();
        //  Sync
        admin().indices().prepareRefresh().execute().actionGet();
    }

    private void initMapping() {
        String mappingProfile    = "{" +
                "   \"properties\" : {" +
                "       \"name\" : {\n" +
                "           \"type\" : \"string\",\n" +
                "           \"index\" : \"not_analyzed\"\n" +
                "       }" +
                "   }" +
                "}";
        String mappingConnection = "{" +
                "   \"_parent\": { \"type\": \"profile\" }," +
                "   \"properties\" : {" +
                "       \"sender\" : {\n" +
                "           \"type\" : \"string\",\n" +
                "           \"index\" : \"not_analyzed\"\n" +
                "       }" +
                "   }" +
                "}";
        CreateIndexRequestBuilder indexBuilder = admin().indices().prepareCreate("connections");
        indexBuilder.addMapping("profile", mappingProfile);
        indexBuilder.addMapping("connection", mappingConnection);
        indexBuilder.execute().actionGet();
    }
}
