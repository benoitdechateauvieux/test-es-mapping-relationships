package com.exo.bch.es;

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

import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;

/**
 * Created by The eXo Platform SAS
 * Author : eXoPlatform
 * exo@exoplatform.com
 * 9/25/15
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class NestedObjectTest extends ElasticsearchIntegrationTest {

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
    public void searchProfileByConnection_2Connections() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        indexProfile("Frederic", "BCH", "TCL");
        //When
        SearchResponse response = getRequestBuilder("BCH","Frederic").execute().actionGet();
        //Then
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    @Test
    public void searchProfileByConnection_50000Connections() throws ExecutionException, InterruptedException {
        //Given
        initMapping();
        String[] connections = new String[50000];
        for (int i=0; i<49999; i++) {
            connections[i] = "conn"+i;
        }
        connections[49999] = "BCH";
        indexProfile("Frederic", connections);
        //When
        SearchResponse response = getRequestBuilder("BCH","Frederic").execute().actionGet();
        //Then
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    private SearchRequestBuilder getRequestBuilder(String currentUserName, String connectionName) {
        return client().prepareSearch("connections")
                .setTypes("profile")
                .setQuery(QueryBuilders.fuzzyQuery("name", connectionName))             // Query
                .setPostFilter(FilterBuilders.nestedFilter("connections",
                        FilterBuilders.boolFilter().must(
                                FilterBuilders.termFilter("connections.userId", currentUserName),
                                FilterBuilders.termFilter("connections.type", "validated")
                        )
                ))  // Filter
//                .setFrom(0).setSize(60).setExplain(true)
                ;
    }

    private void indexProfile(String userName, String... connections) {
        StringBuilder source = new StringBuilder();
        source.append("{ \"name\" : \"" + userName + "\" ");
        if (connections!=null && connections.length>0) {
            source.append(", \"connections\" : [");
            for (String connection : connections) {
                source.append("{");
                source.append(" \"userId\": \""+connection+"\",");
                source.append(" \"type\": \"validated\"");
                source.append("},");
            }
            source.deleteCharAt(source.length()-1);
            source.append("] ");
        }
        source.append(" }");
//        System.out.println(source.toString());
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(client().prepareIndex("connections", "profile", userName)
                .setSource(source.toString()));
        bulkRequest.execute().actionGet();
        //  Sync
        admin().indices().prepareRefresh().execute().actionGet();
    }

    private void initMapping() {
        String mappingProfile    = "{" +
                "   \"properties\" : {" +
                "       \"name\" : {\n" +
                "           \"type\" : \"string\"\n" +
                "       }," +
                "       \"connections\" : {" +
                "           \"type\": \"nested\", \n" +
                "           \"properties\": {" +
                "               \"userId\": {" +
                "                   \"type\" : \"string\",\n" +
                "                   \"index\" : \"not_analyzed\"\n" +
                "               }," +
                "               \"type\": { \"type\": \"string\"  }\n" +
                "           }" +
                "       }" +
                "   }" +
                "}";
        CreateIndexRequestBuilder indexBuilder = admin().indices().prepareCreate("connections");
        indexBuilder.addMapping("profile", mappingProfile);
        indexBuilder.execute().actionGet();
    }
}
