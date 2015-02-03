package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class MigrationServiceTest extends ElasticsearchIntegrationTest {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void crudShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();
		MigrationService migrationService = new MigrationService(client);

		File sourceIndex = new File(Resources.getResource("migrations/index1/index1_v1").getFile());
		File alias = new File(Resources.getResource("migrations/index1").getFile());

		migrationService.migrate(alias, null, sourceIndex);

		final TestDomain domain = new TestDomain();
		Long id = 1L;
		domain.setId(id);
		domain.setTitle(RandomStringUtils.randomAlphanumeric(20));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(200));

		final String idAsString = String.valueOf(id);
		String type = "domains";
		String aliasName = alias.getName();
		// Create
		indexDocument(aliasName, type, domain, idAsString);
		// Read
		TestDomain saved = retrieveDocument(aliasName, type, idAsString, TestDomain.class);
		assertNotNull(saved);
		// Update
		String newDescription = RandomStringUtils.randomAlphanumeric(200);
		saved.setDescription(newDescription);
		indexDocument(aliasName, type, saved, idAsString);
		TestDomain updated = retrieveDocument(aliasName, type, idAsString, TestDomain.class);
		assertEquals(newDescription, updated.getDescription());
		// Delete
		deleteDocument(aliasName, type, idAsString);
		assertNull(retrieveDocument(aliasName, type, idAsString, TestDomain.class));

    }

	@Test
	public void testResolveMappings() throws MalformedURLException {
        final Client client = ElasticsearchIntegrationTest.client();
        MigrationService migrationService = new MigrationService(client);
		File targetIndex = new File(Resources.getResource("migrations/index1/index1_v1").getFile());
        final List<File> expected = Lists.newArrayList();
		expected.add(new File(Resources.getResource("migrations/index1/index1_v1/mappings/domains.json").getFile()));
		assertEquals(expected, migrationService.resolveMappings(targetIndex));
	}

	private void deleteDocument(String alias, String type, String id) throws ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();
		client.prepareDelete(alias, type, id).execute().get();
	}

	private <T> T retrieveDocument(String alias, String type, String id, Class<T> clazz) throws ExecutionException, InterruptedException, IOException {
		final Client client = ElasticsearchIntegrationTest.client();
		final GetResponse getResponse = client.prepareGet(alias, type, id).execute().get();
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
    public void partialMigrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
        final Client client = ElasticsearchIntegrationTest.client();
        MigrationService migrationService = new MigrationService(client);

		File sourceIndex = new File(Resources.getResource("migrations/index1/index1_v1").getFile());
		File alias = new File(Resources.getResource("migrations/index1").getFile());

        migrationService.migrate(alias, null, sourceIndex);

        List<TestDomain> sourceList = bulkIndexSourceIndex(sourceIndex.getName(), "domains", client);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceIndexSearchResponse = client.prepareSearch(sourceIndex.getName())
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

		// When
		File targetIndex = new File(Resources.getResource("migrations/index1/index1_v2").getFile());
        migrationService.migrate(alias, sourceIndex, targetIndex);
        client.admin().indices().prepareRefresh().execute().get();

		String targetIndexName = targetIndex.getName();
		List<TestDomain> targetIndexDocuments = buildDocuments(client, sourceIndexDocuments.size(), targetIndexName);
		assertEquals(sourceIndexDocuments, targetIndexDocuments);
    }

    @Test
    public void fullMigrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
        final Client client = ElasticsearchIntegrationTest.client();
        MigrationService migrationService = new MigrationService(client);

		File sourceIndex = new File(Resources.getResource("migrations/index1/index1_v1").getFile());
		File alias = new File(Resources.getResource("migrations/index1").getFile());

        migrationService.migrate(alias, null, sourceIndex);

        List<TestDomain> sourceList = bulkIndexSourceIndex(sourceIndex.getName(), "domains", client);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceIndexSearchResponse = client.prepareSearch(sourceIndex.getName())
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

		// When
        migrationService.migrate();
        client.admin().indices().prepareRefresh().execute().get();

		// Then
		String targetIndex;
		List<TestDomain> targetIndexDocuments;
		targetIndex = "index1_v3";
		targetIndexDocuments = buildDocuments(client, sourceIndexDocuments.size(), targetIndex);
		assertEquals(sourceIndexDocuments, targetIndexDocuments);
    }

	private List<TestDomain> buildDocuments(Client client, int sourceResultsSize, String targetIndex) throws InterruptedException, ExecutionException, IOException {
		File targetIndexFile = new File(Resources.getResource("migrations/index1/" + targetIndex).getFile());
		final SearchResponse searchResponse = client.prepareSearch(targetIndexFile.getName())
				.setQuery(QueryBuilders.matchAllQuery()).setSize(sourceResultsSize).execute().get();
		assertEquals( (long) sourceResultsSize, searchResponse.getHits().getTotalHits());
		List<TestDomain> targetIndexDocuments = Lists.newArrayList();
		for (SearchHit hit : searchResponse.getHits()) {
			TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
			targetIndexDocuments.add(document);
		}
		return targetIndexDocuments;
	}

	private List<TestDomain> bulkIndexSourceIndex(String index, String type, Client client) throws JsonProcessingException, InterruptedException, ExecutionException {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        List<TestDomain> sourceList = Lists.newArrayList();
        for (int i = 1; i <= 100; i++) {
            TestDomain document = new TestDomain();
            final Long id = (long) i;
            document.setId(id);
            document.setDescription(RandomStringUtils.randomAlphanumeric(200));
            document.setTitle(RandomStringUtils.randomAlphanumeric(20));
            //document.setImageUrl("http://example.com/" + RandomStringUtils.randomAlphanumeric(50));
            sourceList.add(document);
            bulkRequestBuilder.add(client.prepareIndex(index, type).setSource(objectMapper.writeValueAsString(document)).setId(id.toString()));
        }
        final BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
		client.admin().indices().prepareRefresh().execute().get();
        assertFalse(bulkItemResponses.hasFailures());
        return sourceList;
    }

}
