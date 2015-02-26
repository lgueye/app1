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
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
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
        Set<Index> indicesGraph = buildIndicesGraph();
        System.out.println("indicesGraph = " + indicesGraph);
        List<Migration> migrations = resolveMigrations(indicesGraph);
        for (Migration migration : migrations) {
            migration.migrate();
        }
	}

    private List<Migration> resolveMigrations(Set<Index> indicesGraph) throws ExecutionException, InterruptedException {
		Map<String, List<Index>> aliases = groupIndicesByAlias(indicesGraph);
        List<Migration> migrations = Lists.newArrayList();
        for (String alias : aliases.keySet()) {
            final List<Index> indices = aliases.get(alias);
            int position = findCurrentIndexVersion(indices);
            for (int i = position + 1; i < indices.size(); i++) {
                final Index sourceIndex = (i == 0) ? null : indices.get(i - 1);
                final Index targetIndex = indices.get(i);
                migrations.add(new Migration(this.client, sourceIndex, targetIndex));
            }
        }
		return migrations;
    }

	private Map<String, List<Index>> groupIndicesByAlias(Set<Index> indicesGraph) {
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

	private int findCurrentIndexVersion(final List<Index> indices) throws ExecutionException, InterruptedException {
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


    private Set<Index> buildIndicesGraph() throws IOException {
        final ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
        final Resource[] resources = resourceResolver.getResources("/migrations/**/v*");
        Set<Index> indices = Sets.newTreeSet();
        for (Resource resource : resources) {
            Index index = new Index();
            String input = String.valueOf(resource.getURL());
            String alias = extractIndexAlias(input);
            index.setAlias(alias);
            String version = extractIndexVersion(input);
            index.setVersion(version);
            String settings = extractIndexSettings(input);
            index.setSettings(settings);
            final Resource[] mappingResources = resourceResolver.getResources("/migrations/" + alias + "/" + version + "/mappings/**.json");
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
            indices.add(index);
        }
        return indices;
    }

    private String extractMappingDefinition(String input) throws IOException {
        return Resources.toString(new ClassPathResource(input.substring(input.indexOf("/migrations"))).getURL(), Charsets.UTF_8);
    }

    private String extractMappingType(String mappingURL) {
        return mappingURL.substring(mappingURL.lastIndexOf("/") + 1, mappingURL.lastIndexOf("."));
    }

    private String extractIndexSettings(String input) throws IOException {
        final String pattern = "/migrations/";
        String settings = input.substring(input.indexOf(pattern), input.length()) + "settings.json";
        settings = Resources.toString(new ClassPathResource(settings).getURL(), Charsets.UTF_8);
        return settings;
    }

    private String extractIndexVersion(String input) {
        final String pattern = "/migrations/";
        String version = input.substring(input.indexOf(pattern) + pattern.length(), input.length());
        version = version.substring(version.indexOf("/") + 1, version.lastIndexOf("/"));
        return version;
    }

    private String extractIndexAlias(String input) {
        final String pattern = "/migrations/";
        String alias = input.substring(input.indexOf(pattern) + pattern.length(), input.length());
        alias = alias.substring(0, alias.indexOf("/"));
        return alias;
    }

}
