package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.io.Resources;
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
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Migration implements Serializable {
	private String alias;
	private String source;
	private String target;

	public Migration(String alias, String source, String target) {
		this.setAlias(alias);
		this.setSource(source);
		this.setTarget(target);
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Migration)) return false;

		Migration migration = (Migration) o;

		if (alias != null ? !alias.equals(migration.alias) : migration.alias != null) return false;
		if (source != null ? !source.equals(migration.source) : migration.source != null) return false;
		if (target != null ? !target.equals(migration.target) : migration.target != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = alias != null ? alias.hashCode() : 0;
		result = 31 * result + (source != null ? source.hashCode() : 0);
		result = 31 * result + (target != null ? target.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("alias", alias)
				.add("source", source)
				.add("target", target)
				.toString();
	}

    public void migrate() {
        final String alias = getAlias();
        final String source = getSource();
        final String target = getTarget();

        // Create index
        createIndex(alias, target);

        // Bulk index target index from source index
        bulkIndexTargetIndexFromSourceIndex(source, target);

        // Atomically Add and Remove alias
        switchIndex(alias, source, target);

        // Delete source index
        deleteIndex(source);
    }
    private void createIndex(String alias, String index) throws IOException, ExecutionException, InterruptedException {
        final IndicesAdminClient indicesAdminClient = client.admin().indices();

        // Create index
        String settingsSource = Resources.toString(Resources.getResource(settingsPath(alias, index)), Charsets.UTF_8);
        final CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(index)
                .setSource(settingsSource).execute().get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create target index '" + index + "'");
        }

        // Create mappings
        List<String> resolvedMappings = resolveMappings(alias, index);
        // iterate for mappings
        for (String typeFileName : resolvedMappings) {
            String mappingSource = Resources.toString(Resources.getResource(typePath(alias, index, typeFileName)), Charsets.UTF_8);
            String type = typeFileName.substring(0, typeFileName.lastIndexOf(".json"));
            final PutMappingResponse putMappingResponse = indicesAdminClient.preparePutMapping(index)
                    .setType(type).setSource(mappingSource).execute().get();
            if (!putMappingResponse.isAcknowledged()) {
                throw new IllegalStateException("Failed to create type '" + typeFileName + "' for target index '" + index + "'");
            }
        }

    }

    private void bulkIndexTargetIndexFromSourceIndex(String sourceIndex, String targetIndex) throws InterruptedException, ExecutionException, MalformedURLException {

        if (Strings.isEmpty(sourceIndex)) {
            return;
        }
        if (Strings.isEmpty(targetIndex)) {
            throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "' but target index is not defined");
        }

        final IndicesAdminClient indicesAdminClient = client.admin().indices();

        if (!indicesAdminClient.prepareExists(sourceIndex).execute().get().isExists()) {
            throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "', but source index does not exist");
        }
        if (!indicesAdminClient.prepareExists(targetIndex).execute().get().isExists()) {
            throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "' to '" + targetIndex + "', but target index does not exist");
        }

        SearchRequestBuilder searchBuilder = client.prepareSearch(sourceIndex)
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
                bulkRequestBuilder.add(client.prepareIndex(targetIndex, hit.getType())
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
            throw new IllegalStateException("Failed to bulk index target index '" + targetIndex + "' from source index '" + sourceIndex);
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

    private void switchIndex(String alias, String sourceIndex, String targetIndex) throws ExecutionException, InterruptedException {
        final IndicesAdminClient indicesAdminClient = client.admin().indices();

        if (!indicesAdminClient.prepareExists(targetIndex).execute().get().isExists()) {
            throw new IllegalStateException("Trying to add alias '" + alias + "' to '" + targetIndex + "', but target index does not exist");
        }

        final IndicesAliasesRequestBuilder indicesAliasRequestBuilder = indicesAdminClient.prepareAliases()
                .addAlias(targetIndex, alias);
        if (!Strings.isEmpty(sourceIndex)) {
            if (!indicesAdminClient.prepareExists(sourceIndex).execute().get().isExists()) {
                throw new IllegalStateException("Trying to remove alias '" + alias + "' from '" + sourceIndex + "', but source index does not exist");
            }
            indicesAliasRequestBuilder.removeAlias(sourceIndex, alias);
        }
        final IndicesAliasesResponse indicesAliasesResponse = indicesAliasRequestBuilder.execute().get();
        if (!indicesAliasesResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to atomically add alias '" + alias + "' to target index '" + targetIndex + "' and remove it from source index '" + sourceIndex + "'");
        }
    }

}
