package org.diveintojee.poc.digitaloceancluster.app1;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author louis.gueye@gmail.com
 */
@Component
public class MigrationService {

    private Client client;

	private static final Logger LOG = LoggerFactory.getLogger(MigrationService.class);

    @Autowired
    public MigrationService(Client client) {
        this.client = client;
    }

    public void migrate() throws IOException, ExecutionException, InterruptedException {
		List<Migration> migrations = resolveMigrations();
		for (Migration migration : migrations) {
			LOG.info("Migrating alias {} from {} to {}", migration.getAlias(), migration.getSource(), migration.getTarget());
			migrate(migration);
		}
	}

	private List<Migration> resolveMigrations() throws ExecutionException, InterruptedException {
        final String[] aliasesArray = new File(Resources.getResource(migrationsPath()).getFile()).list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return new File(dir, name).isDirectory();
			}
		});
		final List<String> aliasesList = Lists.newArrayList(aliasesArray);
		Collections.sort(aliasesList);
		final IndicesAdminClient indicesAdminClient = client.admin().indices();
		List<Migration> migrations = Lists.newArrayList();

		for (String alias : aliasesList) {
			String[] indicesArray = new File(Resources.getResource(aliasPath(alias)).getFile()).list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return new File(dir, name).isDirectory();
				}
			});
			final List<String> indicesList = Lists.newArrayList(indicesArray);
			Collections.sort(indicesList);
			for (int i = 0; i < indicesList.size(); i++) {
				String source = (i == 0) ? null : indicesList.get(i - 1);
				String target = indicesList.get(i);
				if (!indicesAdminClient.prepareExists(target).execute().get().isExists()) {
					migrations.add(new Migration(alias, source, target));
				}

			}
		}
		return migrations;
	}

    private String migrationsPath() {
        return "migrations";
    }

    private String aliasPath(String alias) {
        return migrationsPath() + "/" + alias;
    }

    private String indexPath(String alias, String index) {
        return aliasPath(alias) + "/" + index;
    }

    private String settingsPath(String alias, String index) {
        return indexPath(alias, index) + "/settings.json";
    }

    private String mappingsPath(String alias, String index) {
        return indexPath(alias, index) + "/mappings";
    }

    private String typePath(String alias, String index, String type) {
        return mappingsPath(alias, index) + "/" + type;
    }

    void migrate(Migration migration) throws InterruptedException, ExecutionException, IOException {

        final String alias = migration.getAlias();
        final String source = migration.getSource();
        final String target = migration.getTarget();

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

    List<String> resolveMappings(String alias, String index) throws MalformedURLException {
		final File mappingsFile = new File(Resources.getResource(mappingsPath(alias, index)).getFile());
        final String[] mappings = mappingsFile.list();
        return Lists.newArrayList(mappings);
    }

}
