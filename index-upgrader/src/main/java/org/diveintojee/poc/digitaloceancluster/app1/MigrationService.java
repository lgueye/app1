package org.diveintojee.poc.digitaloceancluster.app1;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author louis.gueye@gmail.com
 */
@Component
public class MigrationService {

    private Client client;

//	private static final Logger LOG = LoggerFactory.getLogger(MigrationService.class);

    @Autowired
    public MigrationService(Client client) {
        this.client = client;
    }

    public void migrate() throws IOException, ExecutionException, InterruptedException {
        Set<Index> indicesGraph = buildIndicesGraph("/migrations/**/v?");
        List<Migration> migrations = resolveMigrations(indicesGraph);
        for (Migration migration : migrations) {
            migration.migrate();
        }
	}

    List<Migration> resolveMigrations(Set<Index> indicesGraph) throws ExecutionException, InterruptedException {
		Map<String, List<Index>> aliases = groupIndicesByAlias(indicesGraph);
        List<Migration> migrations = Lists.newArrayList();
        for (String alias : aliases.keySet()) {
            final List<Index> indices = aliases.get(alias);
            int position = findCurrentIndex(indices);
            for (int i = position + 1; i < indices.size(); i++) {
                final Index sourceIndex = (i == 0) ? null : indices.get(i - 1);
                final Index targetIndex = indices.get(i);
                migrations.add(new Migration(this.client, sourceIndex, targetIndex));
            }
        }
		return migrations;
    }

	Map<String, List<Index>> groupIndicesByAlias(Set<Index> indicesGraph) {
		Map<String, List<Index>> aliases = Maps.newHashMap();
		for (Index index : indicesGraph) {
			final String key = index.getAlias();
			if (aliases.get(key) == null) {
				aliases.put(key, Lists.<Index>newArrayList());
			}
			aliases.get(key).add(index);
		}
		return aliases;
	}

	int findCurrentIndex(final List<Index> indices) throws ExecutionException, InterruptedException {
        final IndicesAdminClient indicesAdminClient = client.admin().indices();
        int idx = -1;
        for (int i = 0; i < indices.size(); i++) {
            Index index = indices.get(i);
            if (indicesAdminClient.prepareExists(index.getName()).execute().get().isExists()) {
                idx = i;
            }
        }
        return idx;
    }


    Set<Index> buildIndicesGraph(String indicesPattern) throws IOException {
		final Resource[] resources = new PathMatchingResourcePatternResolver().getResources(indicesPattern);
        Set<Index> indices = Sets.newTreeSet();
        for (Resource resource : resources) {
			final URL resourceURL = resource.getURL();
			Index index = buildIndex(resourceURL);
            indices.add(index);
        }
        return indices;
    }

	Index buildIndex(URL resourceURL) throws IOException {
		String urlAsString = String.valueOf(resourceURL);
        String normalizedURL = trimTrailingSlash(urlAsString);
        String alias = extractIndexAlias(normalizedURL);
		String version = extractIndexVersion(normalizedURL);
		String settings = extractIndexSettings(normalizedURL);
		Index index = new Index();
		index.setAlias(alias);
		index.setVersion(version);
		index.setSettings(settings);
		final String mappingsPattern = "/migrations/" + alias + "/" + version + "/mappings/**.json";
		final Resource[] mappingResources = new PathMatchingResourcePatternResolver().getResources(mappingsPattern);
		for (Resource mappingResource : mappingResources) {
			final String mappingURL = String.valueOf(mappingResource.getURL());
			final String type = extractMappingType(mappingURL);
			final String definition = extractMappingDefinition(mappingURL);
			Mapping mapping = new Mapping();
			mapping.setType(type);
			mapping.setDefinition(definition);
			mapping.setParent(index);
			index.addMapping(mapping);
		}
		return index;
	}

    String trimTrailingSlash(final String input) {
        if (input.endsWith("/")) return input.substring(0, input.length() -1);
        return input;
    }


    String extractMappingDefinition(final String input) throws IOException {
		if (input.endsWith("/")) throw new IllegalArgumentException("URL should not end with / char");
        return Resources.toString(new ClassPathResource(input.substring(input.indexOf("/migrations"))).getURL(), Charsets.UTF_8);
    }

    String extractMappingType(final String mappingURL) {
		if (mappingURL.endsWith("/")) throw new IllegalArgumentException("URL should not end with / char");
        return mappingURL.substring(mappingURL.lastIndexOf("/") + 1, mappingURL.lastIndexOf("."));
    }

    String extractIndexSettings(final String input) throws IOException {
		if (input.endsWith("/")) throw new IllegalArgumentException("URL should not end with / char");
        final String pattern = "/migrations/";
        final String settings = input.substring(input.indexOf(pattern), input.length()) + "/settings.json";
        return Resources.toString(new ClassPathResource(settings).getURL(), Charsets.UTF_8);
    }

    String extractIndexVersion(final String input) {
		if (input.endsWith("/")) throw new IllegalArgumentException("URL should not end with / char");
		final String pattern = "/migrations/";
		final String patternSuffix = input.substring(input.indexOf(pattern) + pattern.length(), input.length());
        return patternSuffix.substring(patternSuffix.indexOf("/") + 1, patternSuffix.length());
    }

    String extractIndexAlias(final String input) {
		if (input.endsWith("/")) throw new IllegalArgumentException("URL should not end with / char");
        final String pattern = "/migrations/";
        final String patternSuffix = input.substring(input.indexOf(pattern) + pattern.length(), input.length());
        return patternSuffix.substring(0, patternSuffix.indexOf("/"));
    }

}
