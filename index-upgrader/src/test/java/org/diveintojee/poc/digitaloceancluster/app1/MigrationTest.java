package org.diveintojee.poc.digitaloceancluster.app1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.commons.lang.RandomStringUtils;
import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MigrationTest extends ElasticsearchIntegrationTest {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void crudShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		final AdminClient admin = ElasticsearchIntegrationTest.client().admin();

        final String targetIndex = "tests_v1";
        final String sourceIndex = null;

		// Create index
        String settingsSource = Resources.toString(Resources.getResource("migrations/" + targetIndex + "/settings.json"), Charsets.UTF_8);
        final CreateIndexResponse createIndexResponse = admin.indices().prepareCreate(targetIndex)
				.setSource(settingsSource).execute().get();
        assertEquals(true, createIndexResponse.isAcknowledged());


        // Add mappings
        List<String> resolvedMappings = resolveMappings(targetIndex);
        // iterate for mappings
        for (String type : resolvedMappings) {
            String mappingSource = Resources.toString(Resources.getResource("migrations/" + targetIndex + "/mappings/" + type + ".json"), Charsets.UTF_8);
            final PutMappingResponse putMappingResponse = admin.indices().preparePutMapping(targetIndex).setType(type)
					.setSource(mappingSource).execute().get();
            assertTrue(putMappingResponse.isAcknowledged());

            if ("tests".equals(type)) {
                final Domain domain = new Domain();
                Long id = 1L;
                domain.setId(id);
                domain.setTitle(RandomStringUtils.randomAlphanumeric(Domain.TITLE_MAX_SIZE));
                domain.setDescription(RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE));

                final String idAsString = String.valueOf(id);
                // Create
                indexDocument(targetIndex, type, domain, idAsString);
                // Read
                Domain saved = retrieveDocument(targetIndex, type, idAsString, Domain.class);
                assertNotNull(saved);
                // Update
                String newDescription = RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE);
                saved.setDescription(newDescription);
                indexDocument(targetIndex, type, saved, idAsString);
                Domain updated = retrieveDocument(targetIndex, type, idAsString, Domain.class);
                assertEquals(newDescription, updated.getDescription());
                // Delete
                deleteDocument(targetIndex, type, idAsString);
                assertNull(retrieveDocument(targetIndex, type, idAsString, Domain.class));
            }

        }

        // Atomically Add and Remove alias
        final String alias = "tests";
        final IndicesAliasesRequestBuilder addAliasRequestBuilder = admin.indices().prepareAliases()
                .addAlias(targetIndex, alias);
        if (!Strings.isEmpty(sourceIndex)) {
            addAliasRequestBuilder.removeAlias(sourceIndex, alias);
        }
        final IndicesAliasesResponse indicesAliasesResponse = addAliasRequestBuilder.execute().get();
        assertTrue(indicesAliasesResponse.isAcknowledged());


    }

	private List<String> resolveMappings(String targetIndex) {
		final URL resource = Resources.getResource("migrations/" +targetIndex + "/mappings");
		final String[] mappings = new File(resource.getFile()).list();
		List<String> list = Lists.newArrayList();
		for (String mapping : mappings) {
			list.add(mapping.substring(0, mapping.lastIndexOf(".json")));
		}
		return list;
	}

	@Test
	public void testResolveMappings() {
        final Client client = ElasticsearchIntegrationTest.client();
        MigrationService migrationService = new MigrationService(client);
        String targetIndex = "tests_v1";
        final List<String> expected = Lists.newArrayList();
		expected.add("tests");
		assertEquals(expected, migrationService.resolveMappings(targetIndex));
	}

	private void deleteDocument(String alias, String type, String id) throws ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();
		client.prepareDelete(alias, type, id.toString()).execute().get();
	}

	private <T> T retrieveDocument(String alias, String type, String id, Class<T> clazz) throws ExecutionException, InterruptedException, IOException {
		final Client client = ElasticsearchIntegrationTest.client();
		final GetResponse getResponse = client.prepareGet(alias, type, id.toString()).execute().get();
		if (!getResponse.isExists()) return null;
		assertTrue(getResponse.isExists());
		return objectMapper.readValue(getResponse.getSourceAsString(), clazz);
	}

	private <T> void indexDocument(String alias, String type, T document, String id) throws InterruptedException, ExecutionException, JsonProcessingException {
		final Client client = ElasticsearchIntegrationTest.client();
		final IndexResponse indexResponse = client.prepareIndex(alias, type)
				.setSource(objectMapper.writeValueAsString(document)).setId(id).execute().get();
		assertEquals(id, indexResponse.getId());
	}

    @Test
    public void migrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
        final Client client = ElasticsearchIntegrationTest.client();
        MigrationService migrationService = new MigrationService(client);

        final String sourceIndex = "tests_v1";
        final String targetIndex = "tests_v2";
        final String alias = "tests";

        migrationService.migrate(null, sourceIndex, alias);

        List<TestDomain> sourceList = bulkIndexSourceIndex(sourceIndex, "tests", client);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceIndexSearchResponse = client.prepareSearch(sourceIndex)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(200).execute().get();
        assertEquals(100L, sourceIndexSearchResponse.getHits().getTotalHits());
        List<TestDomain> sourceIndexDocuments = Lists.newArrayList();
        for (SearchHit hit : sourceIndexSearchResponse.getHits()) {
            TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
            sourceIndexDocuments.add(document);
        }
        assertEquals(100L, sourceIndexDocuments.size());
        final Collection<TestDomain> transform = Collections2.transform(sourceList, new Function<TestDomain, TestDomain>() {

            @Override
            public TestDomain apply(TestDomain input) {
                input.setImageUrl(null);
                return input;
            }
        });
        final List<TestDomain> transformedSourceList = Lists.newArrayList(transform);
        assertEquals(100L, transformedSourceList.size());

        assertEquals(transformedSourceList, sourceIndexDocuments);

        migrationService.migrate(sourceIndex, targetIndex, alias);

        client.admin().indices().prepareRefresh().execute().get();
        final SearchResponse targetIndexSearchResponse = client.prepareSearch(targetIndex)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(200).execute().get();
        assertEquals(100L, targetIndexSearchResponse.getHits().getTotalHits());
        List<TestDomain> targetIndexDocuments = Lists.newArrayList();
        for (SearchHit hit : targetIndexSearchResponse.getHits()) {
            TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
            targetIndexDocuments.add(document);
        }

        assertEquals(sourceIndexDocuments, targetIndexDocuments);
    }

    private List<TestDomain> bulkIndexSourceIndex(String index, String type, Client client) throws JsonProcessingException, InterruptedException, ExecutionException {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        List<TestDomain> sourceList = Lists.newArrayList();
        for (int i = 1; i <= 100; i++) {
            TestDomain document = new TestDomain();
            final Long id = Long.valueOf(i);
            document.setId(id);
            document.setDescription(RandomStringUtils.randomAlphanumeric(200));
            document.setTitle(RandomStringUtils.randomAlphanumeric(20));
            //document.setImageUrl("http://example.com/" + RandomStringUtils.randomAlphanumeric(50));
            sourceList.add(document);
            bulkRequestBuilder.add(client.prepareIndex(index, type).setSource(objectMapper.writeValueAsString(document)).setId(id.toString()));
        }
        final BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
        assertFalse(bulkItemResponses.hasFailures());
        return sourceList;
    }

}
