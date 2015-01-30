package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.RandomStringUtils;
import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class MigrationTest extends ElasticsearchIntegrationTest {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void crudShouldSucceed() throws IOException, ExecutionException, InterruptedException {
		final AdminClient admin = ElasticsearchIntegrationTest.client().admin();
		// Create index
		String settingsSource = Resources.toString(Resources.getResource("migrations/domains_v1/settings.json"), Charsets.UTF_8);
		final CreateIndexResponse createIndexResponse = admin.indices().prepareCreate("domains_v1")
				.setSource(settingsSource).execute().actionGet();
		assertEquals(true, createIndexResponse.isAcknowledged());

		// Add mappings
		String mappingSource = Resources.toString(Resources.getResource("migrations/domains_v1/mappings/domains.json"), Charsets.UTF_8);
		final PutMappingResponse putMappingResponse = admin.indices().preparePutMapping("domains_v1").setType("domains")
				.setSource(mappingSource).execute().actionGet();
		assertTrue(putMappingResponse.isAcknowledged());

		// Add alias
		final IndicesAliasesResponse indicesAliasesResponse = admin.indices().prepareAliases()
				.addAlias("domains_v1", "domains").execute().actionGet();
		assertTrue(indicesAliasesResponse.isAcknowledged());

		final Domain domain = new Domain();
		Long id = 1L;
		domain.setId(id);
		domain.setTitle(RandomStringUtils.randomAlphanumeric(Domain.TITLE_MAX_SIZE));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE));
		// Create
		createDomain(domain);
		// Read
		Domain saved = retrieveDomain(id);
		assertNotNull(saved);
		// Update
		String newDescription = RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE);
		saved.setDescription(newDescription);
		updateDomain(saved);
		Domain updated = retrieveDomain(id);
		assertEquals(newDescription, updated.getDescription());
		// Delete
		deleteDomain(id);
		Domain persisted = retrieveDomain(id);
		assertNull(persisted);
	}

	private void deleteDomain(Long id) throws ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();
		client.prepareDelete("domains", "domains", id.toString()).execute().get();
	}

	private void updateDomain(Domain domain) throws JsonProcessingException, ExecutionException, InterruptedException {
		indexDomain(domain);
	}

	private Domain retrieveDomain(Long id) throws ExecutionException, InterruptedException, IOException {
		final Client client = ElasticsearchIntegrationTest.client();
		final GetResponse getResponse = client.prepareGet("domains", "domains", id.toString()).execute().get();
		if (!getResponse.isExists()) return null;
		assertTrue(getResponse.isExists());
		return objectMapper.readValue(getResponse.getSourceAsString(), Domain.class);
	}

	private void createDomain(final Domain domain) throws JsonProcessingException, ExecutionException, InterruptedException {
		indexDomain(domain);
	}

	private void indexDomain(Domain domain) throws InterruptedException, ExecutionException, JsonProcessingException {
		final Client client = ElasticsearchIntegrationTest.client();
		final IndexResponse indexResponse = client.prepareIndex("domains", "domains")
				.setSource(objectMapper.writeValueAsString(domain)).setId(domain.getId().toString()).execute().get();
		assertEquals(domain.getId().toString(), indexResponse.getId());
	}

}
