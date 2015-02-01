package org.diveintojee.poc.digitaloceancluster.app1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author louis.gueye@gmail.com
 */
public class MigrationService {

    private Client client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MigrationService(Client client) {
        this.client = client;
    }

    public void migrate(String sourceIndex, String targetIndex, String alias) throws IOException, ExecutionException, InterruptedException {

        final AdminClient admin = client.admin();

        // Create index
        createTargetIndex(targetIndex, admin);

        // Create target index mappings
        createTargetIndexMappings(targetIndex, admin);

        // Bulk index target index from source index
        bulkIndexTargetIndexFromSourceIndex(sourceIndex, targetIndex);

        // Atomically Add and Remove alias
        switchIndex(sourceIndex, targetIndex, alias, admin);

        // Delete source index
        deleteSourceIndex(sourceIndex, admin);

    }

    private void createTargetIndex(String targetIndex, AdminClient admin) throws IOException, ExecutionException, InterruptedException {
        String settingsSource = Resources.toString(Resources.getResource("migrations/" + targetIndex + "/settings.json"), Charsets.UTF_8);
        final CreateIndexResponse createIndexResponse = admin.indices().prepareCreate(targetIndex)
                .setSource(settingsSource).execute().get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create target index '" + targetIndex + "'");
        }
    }

    private void createTargetIndexMappings(String targetIndex, AdminClient admin) throws IOException, ExecutionException, InterruptedException {
        List<String> resolvedMappings = resolveMappings(targetIndex);
        // iterate for mappings
        for (String type : resolvedMappings) {
            String mappingSource = Resources.toString(Resources.getResource("migrations/" + targetIndex + "/mappings/" + type + ".json"), Charsets.UTF_8);
            final PutMappingResponse putMappingResponse = admin.indices().preparePutMapping(targetIndex).setType(type)
                    .setSource(mappingSource).execute().get();
            if (!putMappingResponse.isAcknowledged()) {
                throw new IllegalStateException("Failed to create type '" + type + "' for target index '" + targetIndex + "'");
            }
        }
    }

    private void bulkIndexTargetIndexFromSourceIndex(String sourceIndex, String targetIndex) throws JsonProcessingException, InterruptedException, ExecutionException {

        if (Strings.isEmpty(sourceIndex)) return;

        List<String> resolvedMappings = resolveMappings(targetIndex);
        // iterate for mappings
        for (String type : resolvedMappings) {
            SearchRequestBuilder searchBuilder = client.prepareSearch(sourceIndex)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(600000))
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(200);
            SearchResponse scrollResp = searchBuilder.execute().get();

            //Scroll until no hits are returned
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().get();
                boolean hitsRead = false;
                for (SearchHit hit : scrollResp.getHits()) {
                    hitsRead = true;
                    //Handle the hit…
                    bulkRequestBuilder.add(client.prepareIndex(targetIndex, type)
                            .setSource(hit.getSourceAsString()).setId(hit.getId()));
                }
                //Break condition: No hits are returned
                if (!hitsRead) {
                    break;
                }
            }
            final BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
            if (bulkItemResponses.hasFailures()) {
                throw new IllegalStateException("Failed to bulk index target index '" + targetIndex + "' from source index '" + sourceIndex + "' for type '" + type);
            }
        }

    }

    private void deleteSourceIndex(String sourceIndex, AdminClient admin) throws ExecutionException, InterruptedException {

        if (Strings.isEmpty(sourceIndex)) return;

        final DeleteIndexResponse deleteIndexResponse = admin.indices().prepareDelete(sourceIndex).execute().get();
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to delete source index '" + sourceIndex + "'");
        }
    }

    private void switchIndex(String sourceIndex, String targetIndex, String alias, AdminClient admin) throws ExecutionException, InterruptedException {
        final IndicesAliasesRequestBuilder indicesAliasRequestBuilder = admin.indices().prepareAliases()
                .addAlias(targetIndex, alias);
        if (!Strings.isEmpty(sourceIndex)) {
            indicesAliasRequestBuilder.removeAlias(sourceIndex, alias);
        }
        final IndicesAliasesResponse indicesAliasesResponse = indicesAliasRequestBuilder.execute().get();
        if (!indicesAliasesResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to add/remove alias '" + alias + "'");
        }
    }

    List<String> resolveMappings(String targetIndex) {
        final URL resource = Resources.getResource("migrations/" +targetIndex + "/mappings");
        final String[] mappings = new File(resource.getFile()).list();
        List<String> list = Lists.newArrayList();
        for (String mapping : mappings) {
            list.add(mapping.substring(0, mapping.lastIndexOf(".json")));
        }
        return list;
    }

}
