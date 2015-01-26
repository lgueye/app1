package org.diveintojee.poc.ansiblecluster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "org.diveintojee.poc.ansiblecluster")
@EnableJpaRepositories(basePackages = "org.diveintojee.poc.ansiblecluster.persistence.data")
@EnableElasticsearchRepositories(basePackages = "org.diveintojee.poc.ansiblecluster.persistence.index")
public class ClusterApplication extends SpringBootServletInitializer {


    public static void main(String[] args) {
        SpringApplication.run(ClusterApplication.class, args);
    }

    @Override
    protected final SpringApplicationBuilder configure(final SpringApplicationBuilder application) {
        return application.sources(ClusterApplication.class);
    }
}
