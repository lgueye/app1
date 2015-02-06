package org.diveintojee.poc.digitaloceancluster.app1;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import java.sql.Driver;

@Configuration
public class DbMigrationConfiguration {

    @Value("${datasource.driverClassName}")
    private String driverClassName;
    @Value("${datasource.username}")
    private String userName;
    @Value("${datasource.password}")
    private String password;
    @Value("${datasource.url}")
    private String url;

    @Bean
    public SimpleDriverDataSource dataSource() throws ClassNotFoundException {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass((Class<? extends Driver>) Class.forName(driverClassName));
        dataSource.setUsername(userName);
        dataSource.setUrl(url);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Bean
    public Flyway flyway() throws ClassNotFoundException {
        Flyway flyway = new Flyway();
        flyway.setLocations("migrations");
        flyway.setDataSource(dataSource());
        return flyway;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() throws ClassNotFoundException {
        return new JdbcTemplate(dataSource());
    }
}
