package org.diveintojee.poc.digitaloceancluster.app1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MigrationServiceTest extends ElasticsearchIntegrationTest {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void crudShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();

        final String alias = "index1";
        final String version = "v1";
        final String settings = "{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1\n" +
                "    }\n" +
                "}";
        final String type = "domains";
        final String definition = "{\n" +
                "    \"domains\" : {\n" +
                "        \"dynamic\": \"strict\",\n" +
                "        \"properties\" : {\n" +
                "            \"id\" : {\"type\" : \"long\", \"store\" : true },\n" +
                "            \"title\" : {\"type\" : \"string\", \"store\" : true },\n" +
                "            \"description\" : {\"type\" : \"string\", \"store\" : true }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Index target = buildIndex(alias, version, settings, type, definition);

		new Migration(client, null, target).migrate();

		final TestDomain domain = new TestDomain();
		Long id = 1L;
		domain.setId(id);
		domain.setTitle(RandomStringUtils.randomAlphanumeric(20));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(200));

		final String idAsString = String.valueOf(id);
		// Create
		indexDocument(alias, type, domain, idAsString);
		// Read
		TestDomain saved = retrieveDocument(alias, type, idAsString, TestDomain.class);
		assertNotNull(saved);
		// Update
		String newDescription = RandomStringUtils.randomAlphanumeric(200);
		saved.setDescription(newDescription);
		indexDocument(alias, type, saved, idAsString);
		TestDomain updated = retrieveDocument(alias, type, idAsString, TestDomain.class);
		assertEquals(newDescription, updated.getDescription());
		// Delete
		deleteDocument(alias, type, idAsString);
		assertNull(retrieveDocument(alias, type, idAsString, TestDomain.class));

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
        String alias;
        String version;
        String settings;
        String type;
        String definition;

        alias = "index1";
        version = "v1";
        settings = "{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1\n" +
                "    }\n" +
                "}";
        type = "domains";
        definition = "{\n" +
                "    \"domains\" : {\n" +
                "        \"dynamic\": \"strict\",\n" +
                "        \"properties\" : {\n" +
                "            \"id\" : {\"type\" : \"long\", \"store\" : true },\n" +
                "            \"title\" : {\"type\" : \"string\", \"store\" : true },\n" +
                "            \"description\" : {\"type\" : \"string\", \"store\" : true }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Index indexV1 = buildIndex(alias, version, settings, type, definition);

        new Migration(client, null, indexV1).migrate();

        final int documentsCount = 100;
        List<TestDomain> sourceList = bulkIndexSourceIndex(indexV1.getAlias(), type, documentsCount);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceSearchResponse = client.prepareSearch(indexV1.getAlias())
                .setQuery(QueryBuilders.matchAllQuery()).setSize(200).execute().get();
        assertEquals((long) documentsCount, sourceSearchResponse.getHits().getTotalHits());
        List<TestDomain> sourceIndexDocuments = Lists.newArrayList();
        for (SearchHit hit : sourceSearchResponse.getHits()) {
            TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
            sourceIndexDocuments.add(document);
        }
        assertEquals((long)documentsCount, sourceIndexDocuments.size());
        final Collection<TestDomain> transform = Collections2.transform(sourceList, new Function<TestDomain, TestDomain>() {

            @Override
            public TestDomain apply(TestDomain input) {
                input.setImageUrl(null);
                return input;
            }
        });
        final List<TestDomain> transformedSourceList = Lists.newArrayList(transform);
        assertEquals((long)documentsCount, transformedSourceList.size());

        assertEquals(transformedSourceList, sourceIndexDocuments);

		// When
        alias = "index1";
        version = "v2";
        settings = "{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1\n" +
                "    }\n" +
                "}";
        type = "domains";
        definition = "{\n" +
                "    \"domains\" : {\n" +
                "        \"dynamic\": \"strict\",\n" +
                "        \"properties\" : {\n" +
                "            \"id\" : {\"type\" : \"long\", \"store\" : true },\n" +
                "            \"title\" : {\"type\" : \"string\", \"store\" : true },\n" +
                "            \"description\" : {\"type\" : \"string\", \"store\" : true },\n" +
                "            \"imageUrl\" : {\"type\" : \"string\", \"store\" : true }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Index indexV2 = buildIndex(alias, version, settings, type, definition);
        new Migration(client, indexV1, indexV2).migrate();
        client.admin().indices().prepareRefresh().execute().get();

		List<TestDomain> targetIndexDocuments = buildDocuments(sourceIndexDocuments.size(), indexV2.getAlias());
		assertEquals(sourceIndexDocuments, targetIndexDocuments);
    }

    private Index buildIndex(String alias, String version, String settings, String type, String definition) {
        Index target = new Index();
        target.setAlias(alias);
        target.setVersion(version);
        target.setSettings(settings);
        final Mapping mapping = new Mapping();
        mapping.setParent(target);
        mapping.setType(type);
        mapping.setDefinition(definition);
        target.addMapping(mapping);
        return target;
    }

    @Test
    public void fullMigrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
        final Client client = ElasticsearchIntegrationTest.client();
        String alias;
        String version;
        String settings;
        String type;
        String definition;

        alias = "index1";
        version = "v1";
        settings = "{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1\n" +
                "    }\n" +
                "}";
        type = "domains";
        definition = "{\n" +
                "    \"domains\" : {\n" +
                "        \"dynamic\": \"strict\",\n" +
                "        \"properties\" : {\n" +
                "            \"id\" : {\"type\" : \"long\", \"store\" : true },\n" +
                "            \"title\" : {\"type\" : \"string\", \"store\" : true },\n" +
                "            \"description\" : {\"type\" : \"string\", \"store\" : true }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Index indexV1 = buildIndex(alias, version, settings, type, definition);

        new Migration(client, null, indexV1).migrate();

        final int documentsCount = 100;
        List<TestDomain> sourceList = bulkIndexSourceIndex(alias, "domains", documentsCount);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceSearchResponse = client.prepareSearch(alias)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(200).execute().get();
        assertEquals((long) documentsCount, sourceSearchResponse.getHits().getTotalHits());
        List<TestDomain> sourceDocuments = Lists.newArrayList();
        for (SearchHit hit : sourceSearchResponse.getHits()) {
            TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
            sourceDocuments.add(document);
        }
        assertEquals((long) documentsCount, sourceDocuments.size());
        final Collection<TestDomain> transform = Collections2.transform(sourceList, new Function<TestDomain, TestDomain>() {

            @Override
            public TestDomain apply(TestDomain input) {
                input.setImageUrl(null);
                return input;
            }
        });
        final List<TestDomain> transformedSourceList = Lists.newArrayList(transform);
        assertEquals((long) documentsCount, transformedSourceList.size());

        assertEquals(transformedSourceList, sourceDocuments);

		// When
        MigrationService migrationService = new MigrationService(client);
        migrationService.migrate();
        client.admin().indices().prepareRefresh().execute().get();

		// Then
		String target;
		List<TestDomain> targetDocuments;
		target = "index1_v3";
		targetDocuments = buildDocuments(sourceDocuments.size(), target);
		assertEquals(sourceDocuments, targetDocuments);
    }

	private List<TestDomain> buildDocuments(int expectedResultsSize, String target) throws InterruptedException, ExecutionException, IOException {
        final Client client = ElasticsearchIntegrationTest.client();
		final SearchResponse searchResponse = client.prepareSearch(target)
				.setQuery(QueryBuilders.matchAllQuery()).setSize(expectedResultsSize).execute().get();
		assertEquals( (long) expectedResultsSize, searchResponse.getHits().getTotalHits());
		List<TestDomain> targetDocuments = Lists.newArrayList();
		for (SearchHit hit : searchResponse.getHits()) {
			TestDomain document = objectMapper.readValue(hit.getSourceAsString(), TestDomain.class);
			targetDocuments.add(document);
		}
		return targetDocuments;
	}

	private List<TestDomain> bulkIndexSourceIndex(String index, String type, int documentsCount) throws JsonProcessingException, InterruptedException, ExecutionException {
        final Client client = ElasticsearchIntegrationTest.client();
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        List<TestDomain> sourceList = Lists.newArrayList();
        for (int i = 1; i <= documentsCount; i++) {
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
