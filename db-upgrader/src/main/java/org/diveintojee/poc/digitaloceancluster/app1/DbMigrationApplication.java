package org.diveintojee.poc.digitaloceancluster.app1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchDataAutoConfiguration;

/**
 * @author louis.gueye@gmail.com
 */
@SpringBootApplication(exclude={
        ElasticsearchDataAutoConfiguration.class,
        JpaRepositoriesAutoConfiguration.class})
public class DbMigrationApplication implements CommandLineRunner {

    @Autowired
    private MigrationService migrationService;

    @Override
    public void run(String... args) {
        this.migrationService.migrate();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(DbMigrationApplication.class, args);
    }

}
