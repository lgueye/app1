package org.diveintojee.poc.digitaloceancluster.app1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author louis.gueye@gmail.com
 */
@Configuration
@EnableAutoConfiguration(exclude={
        ElasticsearchAutoConfiguration.class,
        JpaRepositoriesAutoConfiguration.class})
@ComponentScan
public class IndexMigrationApplication implements CommandLineRunner {

    @Autowired
    private MigrationService migrationService;

    @Override
    public void run(String... args) throws InterruptedException, ExecutionException, IOException {
        this.migrationService.migrate();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IndexMigrationApplication.class, args);
    }

}
