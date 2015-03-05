package org.diveintojee.poc.digitaloceancluster.app1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author louis.gueye@gmail.com
 */
public class IndexOperations {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void deleteDocument(String alias, String type, String id) throws ExecutionException, InterruptedException {
		final Client client = ElasticsearchIntegrationTest.client();
		client.prepareDelete(alias, type, id).execute().get();
	}

	public static <T> T retrieveDocument(String alias, String type, String id, Class<T> clazz) throws ExecutionException, InterruptedException, IOException {
		final Client client = ElasticsearchIntegrationTest.client();
		final GetResponse getResponse = client.prepareGet(alias, type, id).execute().get();
		if (!getResponse.isExists()) return null;
		assertTrue(getResponse.isExists());
		return objectMapper.readValue(getResponse.getSourceAsString(), clazz);
	}

	public static <T> void indexDocument(String alias, String type, T document, String id) throws InterruptedException, ExecutionException, JsonProcessingException {
		final Client client = ElasticsearchIntegrationTest.client();
		final IndexResponse indexResponse = client.prepareIndex(alias, type)
				.setSource(objectMapper.writeValueAsString(document)).setId(id).execute().get();
		assertEquals(id, indexResponse.getId());
	}

	public static List<TestDomain> bulkIndex(String index, String type, int documentsCount) throws JsonProcessingException, InterruptedException, ExecutionException {
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

	public static List<TestDomain> buildDocuments(int expectedResultsSize, String target) throws InterruptedException, ExecutionException, IOException {
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

}
