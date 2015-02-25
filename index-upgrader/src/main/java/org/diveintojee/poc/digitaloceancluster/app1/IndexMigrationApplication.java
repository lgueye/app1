package org.diveintojee.poc.digitaloceancluster.app1;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchAutoConfiguration;

/**
 * @author louis.gueye@gmail.com
 */
@SpringBootApplication(exclude={
        ElasticsearchAutoConfiguration.class,
        JpaRepositoriesAutoConfiguration.class})
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
