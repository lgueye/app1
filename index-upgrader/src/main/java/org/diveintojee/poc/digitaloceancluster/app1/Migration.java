package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Migration implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(Migration.class);

	private Client client;
	private Index source;
	private Index target;

	public Migration(Client client, Index source, Index target) {
		this.setClient(client);
		this.setSource(source);
		this.setTarget(target);
	}

	public void setSource(Index source) {
		this.source = source;
	}

	public void setTarget(Index target) {
		this.target = target;
	}

	public void setClient(Client client) {
		this.client = client;
	}

    public void migrate() throws ExecutionException, InterruptedException, IOException {

        // Create index
        createTargetIndex();

        // Bulk index target index from source index
        bulkIndexTargetIndexFromSourceIndex();

        // Atomically Add and Remove alias
        switchIndex();

        // Delete source index
        if (source != null)
            deleteIndex(source.getName());
    }

    private void createTargetIndex() throws IOException, ExecutionException, InterruptedException {

        final IndicesAdminClient indicesAdminClient = client.admin().indices();

        // Create index
		final String indexName = target.getName();
		final CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(indexName)
                .setSource(target.getSettings()).execute().get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create target index '" + indexName + "'");
        }

        // iterate for mappings
        for (Mapping mapping : target.getMappings()) {
            final PutMappingResponse putMappingResponse = indicesAdminClient.preparePutMapping(indexName)
                    .setType(mapping.getType()).setSource(mapping.getDefinition()).execute().get();
            if (!putMappingResponse.isAcknowledged()) {
                throw new IllegalStateException("Failed to create type '" + mapping.getType() + "' for target index '" + indexName + "'");
            }
        }

    }

    private void bulkIndexTargetIndexFromSourceIndex() throws InterruptedException, ExecutionException, MalformedURLException {

		if (source == null || Strings.isEmpty(source.getName())) {
			return;
		}
		final String sourceName = source.getName();
		if (target == null || Strings.isEmpty(target.getName())) {
			throw new IllegalArgumentException("Trying to reindex from '" + sourceName + "' but target index is not defined");
		}
		final String targetName = target.getName();
		final IndicesAdminClient indicesAdminClient = client.admin().indices();

        if (!indicesAdminClient.prepareExists(sourceName).execute().get().isExists()) {
            throw new IllegalArgumentException("Trying to reindex from '" + sourceName + "', but source index does not exist");
        }

        if (!indicesAdminClient.prepareExists(targetName).execute().get().isExists()) {
            throw new IllegalArgumentException("Trying to reindex from '" + sourceName + "' to '" + targetName + "', but target index does not exist");
        }

        SearchRequestBuilder searchBuilder = client.prepareSearch(sourceName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(6000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(200);
        SearchResponse scrollResp = searchBuilder.execute().get();

        //Scroll until no hits are returned
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000)).execute().get();
            boolean hitsRead = false;
            for (SearchHit hit : scrollResp.getHits()) {
                hitsRead = true;
                //Handle the hit…
                bulkRequestBuilder.add(client.prepareIndex(targetName, hit.getType())
                        .setSource(hit.getSourceAsString()).setId(hit.getId()));
            }
            //Break condition: No hits are returned
            if (!hitsRead) {
                break;
            }
        }

        final BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
        indicesAdminClient.prepareRefresh().execute().get();
        if (bulkItemResponses.hasFailures()) {
            throw new IllegalStateException("Failed to bulk index target index '" + targetName + "' from source index '" + sourceName);
        }

    }

    private void deleteIndex(String index) throws ExecutionException, InterruptedException {
        if (Strings.isEmpty(index)) return;

        final IndicesAdminClient indicesAdminClient = client.admin().indices();
        if (!indicesAdminClient.prepareExists(index).execute().get().isExists()) {
            LOG.info("Trying to delete '" + index + "', but index does not exist");
        }
        final DeleteIndexResponse deleteIndexResponse = indicesAdminClient.prepareDelete(index).execute().get();
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to delete index '" + index + "'");
        }
    }

    private void switchIndex() throws ExecutionException, InterruptedException {
        final IndicesAdminClient indicesAdminClient = client.admin().indices();
		String alias = target.getAlias();
		String targetName = target.getName();
        String sourceName = source == null ? null : source.getName();

        if (!indicesAdminClient.prepareExists(targetName).execute().get().isExists()) {
            throw new IllegalStateException("Trying to add alias '" + alias + "' to '" + targetName + "', but target index does not exist");
        }

        final IndicesAliasesRequestBuilder indicesAliasRequestBuilder = indicesAdminClient.prepareAliases()
                .addAlias(targetName, alias);
        if (source != null && !Strings.isEmpty(source.getName())) {
            if (!indicesAdminClient.prepareExists(sourceName).execute().get().isExists()) {
                throw new IllegalStateException("Trying to remove alias '" + alias + "' from '" + sourceName + "', but source index does not exist");
            }
            indicesAliasRequestBuilder.removeAlias(sourceName, alias);
        }
        final IndicesAliasesResponse indicesAliasesResponse = indicesAliasRequestBuilder.execute().get();
        if (!indicesAliasesResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to atomically add alias '" + alias + "' to target index '" + targetName + "' and remove it from source index '" + sourceName + "'");
        }
    }

}
