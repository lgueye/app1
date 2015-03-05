package org.diveintojee.poc.digitaloceancluster.app1;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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

import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CreateIndexResponse.class)
public class MigrationTest {

	@Test(expected = IllegalStateException.class)
	public void createTargetIndexShouldThrowIllegalStateExceptionIfIndexCreationIsNotAcknowledged() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		AdminClient adminClient = mock(AdminClient.class);
		when(client.admin()).thenReturn(adminClient);
		IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
		when(adminClient.indices()).thenReturn(indicesAdminClient);
		String indexName = "whatever";
		when(target.getName()).thenReturn(indexName);
		CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
		when(indicesAdminClient.prepareCreate(indexName)).thenReturn(createIndexRequestBuilder);
		String settings = "settings";
		when(target.getSettings()).thenReturn(settings);
		when(createIndexRequestBuilder.setSource(settings)).thenReturn(createIndexRequestBuilder);
		ListenableActionFuture<CreateIndexResponse> listenableActionFuture = mock(AbstractListenableActionFuture.class);
		when(createIndexRequestBuilder.execute()).thenReturn(listenableActionFuture);
		CreateIndexResponse createIndexResponse = PowerMockito.mock(CreateIndexResponse.class);
		when(listenableActionFuture.get()).thenReturn(createIndexResponse);
		when(createIndexResponse.isAcknowledged()).thenReturn(false);

		// When
		new Migration(client, null, target).createTargetIndex();
	}

	@Test(expected = IllegalStateException.class)
	public void createTargetIndexShouldThrowIllegalStateExceptionIfMappingCreationIsNotAcknowledged() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		AdminClient adminClient = mock(AdminClient.class);
		when(client.admin()).thenReturn(adminClient);
		IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
		when(adminClient.indices()).thenReturn(indicesAdminClient);
		String indexName = "whatever";
		when(target.getName()).thenReturn(indexName);
		CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
		when(indicesAdminClient.prepareCreate(indexName)).thenReturn(createIndexRequestBuilder);
		String settings = "settings";
		when(target.getSettings()).thenReturn(settings);
		when(createIndexRequestBuilder.setSource(settings)).thenReturn(createIndexRequestBuilder);
		ListenableActionFuture<CreateIndexResponse> createIndexResponseListenableActionFuture = mock(AbstractListenableActionFuture.class);
		when(createIndexRequestBuilder.execute()).thenReturn(createIndexResponseListenableActionFuture);
		CreateIndexResponse createIndexResponse = PowerMockito.mock(CreateIndexResponse.class);
		when(createIndexResponseListenableActionFuture.get()).thenReturn(createIndexResponse);
		when(createIndexResponse.isAcknowledged()).thenReturn(true);
		Mapping mapping = mock(Mapping.class);
		final String type = "whatevertype";
		when(mapping.getType()).thenReturn(type);
		final String definition = "whateverdefinition";
		when(mapping.getDefinition()).thenReturn(definition);
		Set<Mapping> mappings = Sets.newHashSet(mapping);
		when(target.getMappings()).thenReturn(mappings);
		PutMappingRequestBuilder putMappingRequestBuilder = mock(PutMappingRequestBuilder.class);
		when(indicesAdminClient.preparePutMapping(indexName)).thenReturn(putMappingRequestBuilder);
		when(putMappingRequestBuilder.setType(type)).thenReturn(putMappingRequestBuilder);
		when(putMappingRequestBuilder.setSource(definition)).thenReturn(putMappingRequestBuilder);
		ListenableActionFuture<PutMappingResponse> putMappingResponseListenableActionFuture = mock(AbstractListenableActionFuture.class);
		when(putMappingRequestBuilder.execute()).thenReturn(putMappingResponseListenableActionFuture);
		PutMappingResponse putMappingResponse = PowerMockito.mock(PutMappingResponse.class);
		when(putMappingResponseListenableActionFuture.get()).thenReturn(putMappingResponse);
		when(putMappingResponse.isAcknowledged()).thenReturn(false);
		// When
		new Migration(client, null, target).createTargetIndex();
	}

	@Test
	public void createTargetIndexShouldSucceed() throws ExecutionException, InterruptedException, IOException {

		// Given
		Client client = mock(Client.class);
		Index target = mock(Index.class);
		AdminClient adminClient = mock(AdminClient.class);
		when(client.admin()).thenReturn(adminClient);
		IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
		when(adminClient.indices()).thenReturn(indicesAdminClient);
		String indexName = "whatever";
		when(target.getName()).thenReturn(indexName);
		CreateIndexRequestBuilder createIndexRequestBuilder = mock(CreateIndexRequestBuilder.class);
		when(indicesAdminClient.prepareCreate(indexName)).thenReturn(createIndexRequestBuilder);
		String settings = "settings";
		when(target.getSettings()).thenReturn(settings);
		when(createIndexRequestBuilder.setSource(settings)).thenReturn(createIndexRequestBuilder);
		ListenableActionFuture<CreateIndexResponse> createIndexResponseListenableActionFuture = mock(AbstractListenableActionFuture.class);
		when(createIndexRequestBuilder.execute()).thenReturn(createIndexResponseListenableActionFuture);
		CreateIndexResponse createIndexResponse = PowerMockito.mock(CreateIndexResponse.class);
		when(createIndexResponseListenableActionFuture.get()).thenReturn(createIndexResponse);
		when(createIndexResponse.isAcknowledged()).thenReturn(true);
		Mapping mapping = mock(Mapping.class);
		final String type = "whatevertype";
		when(mapping.getType()).thenReturn(type);
		final String definition = "whateverdefinition";
		when(mapping.getDefinition()).thenReturn(definition);
		Set<Mapping> mappings = Sets.newHashSet(mapping);
		when(target.getMappings()).thenReturn(mappings);
		PutMappingRequestBuilder putMappingRequestBuilder = mock(PutMappingRequestBuilder.class);
		when(indicesAdminClient.preparePutMapping(indexName)).thenReturn(putMappingRequestBuilder);
		when(putMappingRequestBuilder.setType(type)).thenReturn(putMappingRequestBuilder);
		when(putMappingRequestBuilder.setSource(definition)).thenReturn(putMappingRequestBuilder);
		ListenableActionFuture<PutMappingResponse> putMappingResponseListenableActionFuture = mock(AbstractListenableActionFuture.class);
		when(putMappingRequestBuilder.execute()).thenReturn(putMappingResponseListenableActionFuture);
		PutMappingResponse putMappingResponse = PowerMockito.mock(PutMappingResponse.class);
		when(putMappingResponseListenableActionFuture.get()).thenReturn(putMappingResponse);
		when(putMappingResponse.isAcknowledged()).thenReturn(true);
		// When
		new Migration(client, null, target).createTargetIndex();

		verify(putMappingResponse, times(1)).isAcknowledged();
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
	public void bulkIndexTargetIndexFromSourceIndexShouldPerformNothingIfTargetIndexIsNull() throws InterruptedException, ExecutionException, IOException {
		// Given
		Client client = mock(Client.class);
		Index source = mock(Index.class);
		final String sourceIndexName = "source_index_name";
		when(source.getName()).thenReturn(sourceIndexName);
		// When
		new Migration(client, source, null).bulkIndexTargetIndexFromSourceIndex();
	}

}