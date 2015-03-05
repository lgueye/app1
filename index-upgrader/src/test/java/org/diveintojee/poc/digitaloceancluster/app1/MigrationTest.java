package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.collect.Sets;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.support.AbstractListenableActionFuture;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
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
