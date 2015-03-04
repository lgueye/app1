package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

public class MigrationServiceTest extends ElasticsearchIntegrationTest {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void crudShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
		final Client client = ElasticsearchIntegrationTest.client();
		MigrationService migrationService = new MigrationService(client);
		final Resource[] indexV1Resources = new PathMatchingResourcePatternResolver().getResources("/migrations/index1/v1");
		final Resource indexV1Resource = indexV1Resources[0];
		Index target = migrationService.buildIndex(indexV1Resource.getURL());
		// When I migrate indices from nothing to index1
		new Migration(client, null, target).migrate();

		final TestDomain domain = new TestDomain();
		Long id = 1L;
		domain.setId(id);
		domain.setTitle(RandomStringUtils.randomAlphanumeric(20));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(200));

		final String idAsString = String.valueOf(id);
		final String alias = target.getAlias();
		// And I index documents
		for (Mapping mapping : target.getMappings()) {
			// Create
			final String type = mapping.getType();
			IndexOperations.indexDocument(alias, type, domain, idAsString);
			// Read
			TestDomain saved = IndexOperations.retrieveDocument(alias, type, idAsString, TestDomain.class);
			assertNotNull(saved);
			// Update
			String newDescription = RandomStringUtils.randomAlphanumeric(200);
			saved.setDescription(newDescription);
			IndexOperations.indexDocument(alias, type, saved, idAsString);
			TestDomain updated = IndexOperations.retrieveDocument(alias, type, idAsString, TestDomain.class);
			assertEquals(newDescription, updated.getDescription());
			// Delete
			IndexOperations.deleteDocument(alias, type, idAsString);
			assertNull(IndexOperations.retrieveDocument(alias, type, idAsString, TestDomain.class));
		}
    }

    @Test
    public void partialMigrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
        final Client client = ElasticsearchIntegrationTest.client();
		final MigrationService migrationService = new MigrationService(client);
		final Resource indexV1Resource = new PathMatchingResourcePatternResolver().getResources("/migrations/index1/v1")[0];
		Index indexV1 = migrationService.buildIndex(indexV1Resource.getURL());
        new Migration(client, null, indexV1).migrate();

        final int documentsCount = 100;
		final String alias = indexV1.getAlias();
		List<TestDomain> sourceList = IndexOperations.bulkIndex(alias, indexV1.getMappings().iterator().next().getType(), documentsCount);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceSearchResponse = client.prepareSearch(alias)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(documentsCount).execute().get();
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

		final Resource indexV2Resource = new PathMatchingResourcePatternResolver().getResources("/migrations/index1/v2")[0];
		Index indexV2 = migrationService.buildIndex(indexV2Resource.getURL());

		// When
        new Migration(client, indexV1, indexV2).migrate();
        client.admin().indices().prepareRefresh().execute().get();

		// Then
		List<TestDomain> targetIndexDocuments = IndexOperations.buildDocuments(sourceIndexDocuments.size(), indexV2.getAlias());
		assertEquals(sourceIndexDocuments, targetIndexDocuments);
    }

    @Test
    public void fullMigrationShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		// Given
        final Client client = ElasticsearchIntegrationTest.client();
		final MigrationService migrationService = new MigrationService(client);
		final Resource indexV1Resource = new PathMatchingResourcePatternResolver().getResources("/migrations/index1/v1")[0];
		Index indexV1 = migrationService.buildIndex(indexV1Resource.getURL());
		new Migration(client, null, indexV1).migrate();

        final int documentsCount = 100;
		final String alias = indexV1.getAlias();
		List<TestDomain> sourceList = IndexOperations.bulkIndex(alias, indexV1.getMappings().iterator().next().getType(), documentsCount);
        client.admin().indices().prepareRefresh().execute().get();

        final SearchResponse sourceSearchResponse = client.prepareSearch(alias)
                .setQuery(QueryBuilders.matchAllQuery()).setSize(documentsCount).execute().get();
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
        migrationService.migrate();
        client.admin().indices().prepareRefresh().execute().get();

		// Then
		String target;
		List<TestDomain> targetDocuments;
		target = "index1_v3";
		targetDocuments = IndexOperations.buildDocuments(sourceDocuments.size(), target);
		assertEquals(sourceDocuments, targetDocuments);
    }

	/**
	 * Given the following indices structure
	 * index1 {v1, v2, v3, v4, v5}
	 * If current index is v2
	 * Then migration should migrate v3, v4 and v5
	 *
	 *
	 * Given following index mappings
	 * index1
	 * 	v1
	 * 		mappings
	 * 			logs
	 * 			domains
	 * 			transactions
	 * 	And I populate index v1 types
	 * 	When I migrate from index v1 to index v2
	 * 	Then all types should be populated
	 */
	@Test
	public void veryCompleteTest() {

	}

}
