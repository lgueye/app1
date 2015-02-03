package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

/**
 * @author louis.gueye@gmail.com
 */
public class MigrationService {

    private Client client;

	private static final Logger LOG = LoggerFactory.getLogger(MigrationService.class);

    public MigrationService(Client client) {
        this.client = client;
    }

    public void migrate() throws IOException, ExecutionException, InterruptedException {
		List<Migration> migrations = resolveMigrations();
		for (Migration migration : migrations) {
			if (migration.getSource() != null)
				LOG.info("Migrating alias {} from {} to {}", migration.getAlias().getName(), migration.getSource().getName(), migration.getTarget().getName());
			else
				LOG.info("Migrating alias {} from {} to {}", migration.getAlias().getName(), null, migration.getTarget().getName());
			migrate(migration);
		}
	}

	private List<Migration> resolveMigrations() throws ExecutionException, InterruptedException {
		final String[] aliasesArray = new File(Resources.getResource("migrations").getFile()).list(new FilenameFilter() {
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
			String[] indicesArray = new File(Resources.getResource("migrations/" + alias).getFile()).list(new FilenameFilter() {
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
					File aliasFile = new File(Resources.getResource("migrations/" + alias).getFile());
					File sourceFile = (source == null) ? null : new File(aliasFile, source);
					File targetFile = new File(aliasFile, target);
					migrations.add(new Migration(aliasFile, sourceFile, targetFile));
				}

			}
		}
		return migrations;
	}

	private void migrate(Migration migration) throws InterruptedException, ExecutionException, IOException {
		migrate(migration.getAlias(), migration.getSource(), migration.getTarget());
	}

	public void migrate(File alias, File sourceIndex, File targetIndex) throws IOException, ExecutionException, InterruptedException {

        final AdminClient admin = client.admin();

        // Create index
        createIndex(alias, targetIndex, admin);

		String sourceIndexName = sourceIndex != null ? sourceIndex.getName() : null;
		String targetIndexName = targetIndex != null ? targetIndex.getName() : null;

		// Bulk index target index from source index
		bulkIndexTargetIndexFromSourceIndex(sourceIndexName, targetIndex);

		// Atomically Add and Remove alias
        switchIndex(alias.getName(), sourceIndexName, targetIndexName, admin);

        // Delete source index
        deleteIndex(sourceIndexName, admin);

    }

    private void createIndex(File aliasFile, File indexFile, AdminClient admin) throws IOException, ExecutionException, InterruptedException {
		// Create index
		final String indexName = indexFile.getName();
		final String aliasName = aliasFile.getName();
		String settingsSource = Resources.toString(Resources.getResource("migrations/" + aliasName + "/" + indexName + "/settings.json"), Charsets.UTF_8);
        final CreateIndexResponse createIndexResponse = admin.indices().prepareCreate(indexName)
                .setSource(settingsSource).execute().get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create target index '" + indexFile + "'");
        }

		// Create mappings
		List<File> resolvedMappings = resolveMappings(indexFile);
		// iterate for mappings
		for (File type : resolvedMappings) {
			final String typeFileName = type.getName();
			final String typeName = typeFileName.substring(0, typeFileName.lastIndexOf(".json"));
			String mappingSource = Resources.toString(Resources.getResource("migrations/" + aliasName + "/" + indexName + "/mappings/" + typeFileName), Charsets.UTF_8);
			final PutMappingResponse putMappingResponse = admin.indices().preparePutMapping(indexName)
					.setType(typeName).setSource(mappingSource).execute().get();
			if (!putMappingResponse.isAcknowledged()) {
				throw new IllegalStateException("Failed to create type '" + type + "' for target index '" + indexFile + "'");
			}
		}

    }

    private void bulkIndexTargetIndexFromSourceIndex(String sourceIndex, File targetIndexFile) throws JsonProcessingException, InterruptedException, ExecutionException, MalformedURLException {

        if (Strings.isEmpty(sourceIndex)) {
			return;
		}
		if (targetIndexFile == null) {
			throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "' but target index is not defined");
		}
		String targetIndex = targetIndexFile.getName();
		if (!client.admin().indices().prepareExists(sourceIndex).execute().get().isExists()) {
			throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "', but source index does not exist");
		}
		if (!client.admin().indices().prepareExists(targetIndex).execute().get().isExists()) {
			throw new IllegalArgumentException("Trying to reindex from '" + sourceIndex + "' to '" + targetIndex + "', but target index does not exist");
		}

        List<File> resolvedMappings = resolveMappings(targetIndexFile);
        // iterate for mappings
        for (File type : resolvedMappings) {
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
			client.admin().indices().prepareRefresh().execute().get();
            if (bulkItemResponses.hasFailures()) {
				throw new IllegalStateException("Failed to bulk index target index '" + targetIndex + "' from source index '" + sourceIndex + "' for type '" + type);
            }
        }
    }

    private void deleteIndex(String index, AdminClient admin) throws ExecutionException, InterruptedException {
        if (Strings.isEmpty(index)) return;
		if (!client.admin().indices().prepareExists(index).execute().get().isExists()) {
			LOG.info("Trying to delete '" + index + "', but index does not exist");
		}
        final DeleteIndexResponse deleteIndexResponse = admin.indices().prepareDelete(index).execute().get();
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to delete index '" + index + "'");
        }
    }

    private void switchIndex(String alias, String sourceIndex, String targetIndex, AdminClient admin) throws ExecutionException, InterruptedException {
		if (!client.admin().indices().prepareExists(targetIndex).execute().get().isExists()) {
			throw new IllegalStateException("Trying to add alias '" + alias + "' to '" + targetIndex + "', but target index does not exist");
		}

        final IndicesAliasesRequestBuilder indicesAliasRequestBuilder = admin.indices().prepareAliases()
                .addAlias(targetIndex, alias);
        if (!Strings.isEmpty(sourceIndex)) {
			if (!client.admin().indices().prepareExists(sourceIndex).execute().get().isExists()) {
				throw new IllegalStateException("Trying to remove alias '" + alias + "' from '" + sourceIndex + "', but source index does not exist");
			}
            indicesAliasRequestBuilder.removeAlias(sourceIndex, alias);
        }
        final IndicesAliasesResponse indicesAliasesResponse = indicesAliasRequestBuilder.execute().get();
        if (!indicesAliasesResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to atomically add alias '" + alias + "' to target index '" + targetIndex + "' and remove it from source index '" + sourceIndex + "'");
        }
    }

    List<File> resolveMappings(File indexFile) throws MalformedURLException {
		final File mappingsFile = new File(indexFile, "mappings");
        final String[] mappings = mappingsFile.list();
        List<File> list = Lists.newArrayList();
        for (String mapping : mappings) {
            list.add(new File (mappingsFile, mapping));
        }
        return list;
    }

}
