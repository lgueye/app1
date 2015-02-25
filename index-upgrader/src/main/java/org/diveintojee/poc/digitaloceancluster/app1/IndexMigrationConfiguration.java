package org.diveintojee.poc.digitaloceancluster.app1;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Splitter;

@Configuration
public class IndexMigrationConfiguration {

    @Value("${elasticsearch.transport.addresses}")
    private String transportAddresses;
    @Value("${elasticsearch.transport.port}")
    private int transportPort;
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Bean
    public Client elasticsearchClient() throws ClassNotFoundException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        TransportClient client = new TransportClient(settings);
        for (String host : Splitter.on(',').split(transportAddresses)) {
            client.addTransportAddress(new InetSocketTransportAddress(host, transportPort));
        }
        return client;
    }
}
