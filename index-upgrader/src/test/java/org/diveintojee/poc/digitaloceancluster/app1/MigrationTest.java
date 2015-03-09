package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.collect.Sets;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.AbstractListenableActionFuture;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.sql.Time;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CreateIndexResponse.class)
public class MigrationTest {

	@Test(expected = IllegalStateException.class)
	public void createTargetIndexShouldThrowIllegalStateExceptionIfIndexCreationIsNotAcknowledged() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Index target = mock(Index.class);
        String indexName = "whatever";
        final boolean shouldAcknowledge = false;
        String settings = "settings";
        when(target.getName()).thenReturn(indexName);
        when(target.getSettings()).thenReturn(settings);
        mockIndexAcknowledgement(indicesAdminClient, indexName, shouldAcknowledge, settings);

		// When
		new Migration(client, null, target).createTargetIndex();
	}

    @Test(expected = IllegalStateException.class)
	public void createTargetIndexShouldThrowIllegalStateExceptionIfMappingCreationIsNotAcknowledged() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Index target = mock(Index.class);
        String indexName = "whatever";
        String settings = "settings";
        when(target.getSettings()).thenReturn(settings);
        when(target.getName()).thenReturn(indexName);
        mockIndexAcknowledgement(indicesAdminClient, indexName, true, settings);
		Mapping mapping = mock(Mapping.class);
		final String type = "whatevertype";
		when(mapping.getType()).thenReturn(type);
		final String definition = "whateverdefinition";
		when(mapping.getDefinition()).thenReturn(definition);
		Set<Mapping> mappings = Sets.newHashSet(mapping);
		when(target.getMappings()).thenReturn(mappings);
        mockMappingAcknowledgement(indicesAdminClient, indexName, type, definition, false);
		// When
		new Migration(client, null, target).createTargetIndex();
	}

    @Test
	public void createTargetIndexShouldSucceed() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        Index target = mock(Index.class);
        String indexName = "whatever";
		when(target.getName()).thenReturn(indexName);
        String settings = "settings";
        final boolean shouldAcknowledge = true;
        when(target.getSettings()).thenReturn(settings);
        mockIndexAcknowledgement(indicesAdminClient, indexName, shouldAcknowledge, settings);

		Mapping mapping = mock(Mapping.class);
		final String type = "whatevertype";
		when(mapping.getType()).thenReturn(type);
		final String definition = "whateverdefinition";
		when(mapping.getDefinition()).thenReturn(definition);
		Set<Mapping> mappings = Sets.newHashSet(mapping);
		when(target.getMappings()).thenReturn(mappings);
        mockMappingAcknowledgement(indicesAdminClient, indexName, type, definition, true);

		// When
		new Migration(client, null, target).createTargetIndex();
	}

	@Test
	public void bulkIndexTargetIndexFromSourceIndexShouldPerformNothingIfSourceIndexIsNull() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		// When
		new Migration(client, null, target).bulkIndexTargetIndexFromSourceIndex();
		// Then
		verifyZeroInteractions(client);
	}

	@Test
	public void bulkIndexTargetIndexFromSourceIndexShouldPerformNothingIfSourceIndexNameIsNull() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		Index source = mock(Index.class);
		// When
		new Migration(client, source, target).bulkIndexTargetIndexFromSourceIndex();
		// Then
		verifyZeroInteractions(client);
	}

	@Test
	public void bulkIndexTargetIndexFromSourceIndexShouldPerformNothingIfSourceIndexNameIsEmpty() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		Index source = mock(Index.class);
		when(source.getName()).thenReturn("");
		// When
		new Migration(client, source, target).bulkIndexTargetIndexFromSourceIndex();
		// Then
		verifyZeroInteractions(client);
	}

	@Test(expected = IllegalStateException.class)
	public void bulkIndexTargetIndexFromSourceIndexShouldThrowIllegalStateExceptionIfTargetIndexIsNull() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index source = mock(Index.class);
		final String sourceIndexName = "source_index_name";
		when(source.getName()).thenReturn(sourceIndexName);
		// When
		new Migration(client, source, null).bulkIndexTargetIndexFromSourceIndex();
	}

	@Test(expected = IllegalStateException.class)
	public void bulkIndexTargetIndexFromSourceIndexShouldThrowIllegalStateExceptionIfSourceIndexDoesNotExist() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index source = mock(Index.class);
		Index target = mock(Index.class);
		final String sourceIndexName = "source_index_name";
		when(source.getName()).thenReturn(sourceIndexName);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        mockIndexExistence(sourceIndexName, false, indicesAdminClient);

		// When
		new Migration(client, source, target).bulkIndexTargetIndexFromSourceIndex();
	}

	@Test(expected = IllegalStateException.class)
	public void bulkIndexTargetIndexFromSourceIndexShouldThrowIllegalStateExceptionIfTargetIndexDoesNotExist() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index source = mock(Index.class);
		Index target = mock(Index.class);
		final String sourceIndexName = "source_index_name";
        final String targetIndexName = "target_index_name";
        when(source.getName()).thenReturn(sourceIndexName);
        when(target.getName()).thenReturn(targetIndexName);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        mockIndexExistence(sourceIndexName, true, indicesAdminClient);
        mockIndexExistence(targetIndexName, false, indicesAdminClient);

		// When
		new Migration(client, source, target).bulkIndexTargetIndexFromSourceIndex();
	}

    /**
     * TODO: refactor this test, it's unmaintainable
     *
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     */
	@Test(expected = IllegalStateException.class)
	public void bulkIndexTargetIndexFromSourceIndexShouldThrowIllegalStateExceptionIfBulkIndexHasFailures() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index source = mock(Index.class);
		Index target = mock(Index.class);
		final String sourceIndexName = "source_index_name";
        final String targetIndexName = "target_index_name";
        when(source.getName()).thenReturn(sourceIndexName);
        when(target.getName()).thenReturn(targetIndexName);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        mockIndexExistence(sourceIndexName, true, indicesAdminClient);
        mockIndexExistence(targetIndexName, true, indicesAdminClient);

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        when(client.prepareSearch(sourceIndexName)).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSearchType(SearchType.SCAN)).thenReturn(searchRequestBuilder);
        final TimeValue keepAlive = new TimeValue(6000);
        when(searchRequestBuilder.setScroll(keepAlive)).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSize(200)).thenReturn(searchRequestBuilder);
        ListenableActionFuture<SearchResponse> searchResponseListenableActionFuture = mock(ListenableActionFuture.class);
        when(searchRequestBuilder.execute()).thenReturn(searchResponseListenableActionFuture);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponseListenableActionFuture.get()).thenReturn(searchResponse);

        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        String initialScrollId = "1";
        when(searchResponse.getScrollId()).thenReturn(initialScrollId);
        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        when(client.prepareSearchScroll(initialScrollId)).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.setScroll(keepAlive)).thenReturn(searchScrollRequestBuilder);
        ListenableActionFuture<SearchResponse> searchScrollResponseListenableActionFuture = mock(ListenableActionFuture.class);
        when(searchScrollRequestBuilder.execute()).thenReturn(searchScrollResponseListenableActionFuture);
        SearchResponse searchScrollResponse = mock(SearchResponse.class);
        when(searchScrollResponseListenableActionFuture.get()).thenReturn(searchScrollResponse);
        SearchHits searchHits = mock(SearchHits.class);
        when(searchScrollResponse.getHits()).thenReturn(searchHits);
        String hitType = "whatever-hit-type";
        final SearchHit hit0 = mock(SearchHit.class);
        SearchHit[] hits = new SearchHit[]{hit0};
        when(searchHits.getHits()).thenReturn(hits);
        when(hit0.getType()).thenReturn(hitType);

        String hitSource = "{hit source}";
        when(hit0.getSourceAsString()).thenReturn(hitSource);
        String hitId = "150";
        when(hit0.getId()).thenReturn(hitId);
        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(client.prepareIndex(targetIndexName, hitType)).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.setSource(hitSource)).thenReturn(indexRequestBuilder);
        when(indexRequestBuilder.setId(hitId)).thenReturn(indexRequestBuilder);
        ListenableActionFuture<BulkResponse> bulkResponseListenableActionFuture = mock(ListenableActionFuture.class);
        when(bulkRequestBuilder.execute()).thenReturn(bulkResponseListenableActionFuture);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponseListenableActionFuture.get()).thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);

//        RefreshRequestBuilder refreshRequestBuilder = mock(RefreshRequestBuilder.class);
//        when(indicesAdminClient.prepareRefresh(targetIndexName)).thenReturn(refreshRequestBuilder);
//        ListenableActionFuture<RefreshResponse> refreshResponseListenableActionFuture = mock(ListenableActionFuture.class);
//        when(refreshRequestBuilder.execute()).thenReturn(refreshResponseListenableActionFuture);
//        RefreshResponse refreshResponse = mock(RefreshResponse.class);
//        when(refreshResponseListenableActionFuture.get()).thenReturn(refreshResponse);
//

		// When
		new Migration(client, source, target).bulkIndexTargetIndexFromSourceIndex();
	}

    private void mockIndexExistence(String indexName, boolean shouldExist, IndicesAdminClient indicesAdminClient) throws InterruptedException, ExecutionException {
        IndicesExistsRequestBuilder indicesExistsRequestBuilder = mock(IndicesExistsRequestBuilder.class);
        when(indicesAdminClient.prepareExists(indexName)).thenReturn(indicesExistsRequestBuilder);
        ListenableActionFuture<IndicesExistsResponse> indicesExistsResponseListenableActionFuture = mock(ListenableActionFuture.class);
        when(indicesExistsRequestBuilder.execute()).thenReturn(indicesExistsResponseListenableActionFuture);
        IndicesExistsResponse indicesExistsResponse = mock(IndicesExistsResponse.class);
        when(indicesExistsResponseListenableActionFuture.get()).thenReturn(indicesExistsResponse);
        when(indicesExistsResponse.isExists()).thenReturn(shouldExist);
    }

    private void mockMappingAcknowledgement(IndicesAdminClient indicesAdminClient, String indexName, String type, String definition, boolean shouldAcknowledge) throws InterruptedException, ExecutionException {
        PutMappingRequestBuilder putMappingRequestBuilder = mock(PutMappingRequestBuilder.class);
        when(indicesAdminClient.preparePutMapping(indexName)).thenReturn(putMappingRequestBuilder);
        when(putMappingRequestBuilder.setType(type)).thenReturn(putMappingRequestBuilder);
        when(putMappingRequestBuilder.setSource(definition)).thenReturn(putMappingRequestBuilder);
        ListenableActionFuture<PutMappingResponse> putMappingResponseListenableActionFuture = mock(AbstractListenableActionFuture.class);
        when(putMappingRequestBuilder.execute()).thenReturn(putMappingResponseListenableActionFuture);
        PutMappingResponse putMappingResponse = PowerMockito.mock(PutMappingResponse.class);
        when(putMappingResponseListenableActionFuture.get()).thenReturn(putMappingResponse);
        when(putMappingResponse.isAcknowledged()).thenReturn(shouldAcknowledge);
    }

    private void mockIndexAcknowledgement(IndicesAdminClient indicesAdminClient, String indexName, boolean shouldAcknowledge, String settings) throws InterruptedException, ExecutionException {
        CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
        when(indicesAdminClient.prepareCreate(indexName)).thenReturn(createIndexRequestBuilder);
        when(createIndexRequestBuilder.setSource(settings)).thenReturn(createIndexRequestBuilder);
        ListenableActionFuture<CreateIndexResponse> listenableActionFuture = mock(AbstractListenableActionFuture.class);
        when(createIndexRequestBuilder.execute()).thenReturn(listenableActionFuture);
        CreateIndexResponse createIndexResponse = PowerMockito.mock(CreateIndexResponse.class);
        when(listenableActionFuture.get()).thenReturn(createIndexResponse);
        when(createIndexResponse.isAcknowledged()).thenReturn(shouldAcknowledge);
    }

}
