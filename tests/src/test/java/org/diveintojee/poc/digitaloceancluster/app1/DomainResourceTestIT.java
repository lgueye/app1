package org.diveintojee.poc.digitaloceancluster.app1;

import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ClusterTestConfig.class})
public class DomainResourceTestIT {

    @Autowired
    private ClusterAppClient api;

    @Test
    public void crudDomainShouldSucceed() throws IOException {
        Domain domain = Fixtures.validDomain();
        // Create
        URI uri = api.createDomain(domain);
        assertNotNull(uri);

        // Read
        Domain persisted = api.loadDomain(uri);
        assertNotNull(persisted);
        Long id = persisted.getId();
        assertNotNull(id);
        persisted.setId(null);
        assertEquals(persisted, domain);

        // Update
        persisted.setId(id);
        String expectedName = "test test test";
        persisted.setTitle(expectedName);
        api.updateDomain(uri, persisted);
        Domain updated = api.loadDomain(uri);
        assertEquals(persisted, updated);

        // Delete
        api.deleteResource(uri);
        try {
            new RestTemplate().getForEntity(uri, Domain.class);
        } catch (HttpClientErrorException e) {
            assertEquals(HttpStatus.NOT_FOUND, e.getStatusCode());
        } catch (Exception e) {
            fail("expected {" + HttpClientErrorException.class + "}, got {" + e + "}");
        }
    }


    @Test
    public void searchDomainsShouldMatchDescription() throws IOException {

        // Given
        api.deleteAllDomains();
        api.createDomain(Fixtures.createDomain("Sit precari invehi ut vehementius quisque.", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));
        api.createDomain(Fixtures.createDomain("Tempus coniunx id conductae hastam est conductae", "Inlustris Commagena civitatibus Hierapoli est Euphratensis Commagena Osdroenam dictum Euphratensis clementer Osdroenam civitatibus descriptione vetere ab Samosata ut Hierapoli."));
        api.createDomain(Fixtures.createDomain("Uni cuius libidines praeter ac exitum Pisonis nec", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));

        // When
        List<Domain> results = api.searchDomains("virgi");

        // Then
        assertEquals(2, results.size());
    }

    @Test
    public void searchDomainsShouldMatchTitle() throws IOException {

        // Given
        api.deleteAllDomains();
        api.createDomain(Fixtures.createDomain("Sit precari conductae ut vehementius quisque", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));
        api.createDomain(Fixtures.createDomain("Tempus coniunx id conductae hastam est conductae", "Inlustris Commagena civitatibus Hierapoli est Euphratensis Commagena Osdroenam dictum Euphratensis clementer Osdroenam civitatibus descriptione vetere ab Samosata ut Hierapoli."));
        api.createDomain(Fixtures.createDomain("Uni cuius libidines praeter ac exitum Pisonis nec", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));

        // When
        List<Domain> results = api.searchDomains("condu");

        // Then
        assertEquals(2, results.size());
    }

    @Test
    public void findAllDomainsShouldSucceed() throws IOException {

        // Given
        api.deleteAllDomains();
        api.createDomain(Fixtures.createDomain("Sit precari conductae ut vehementius quisque", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));
        api.createDomain(Fixtures.createDomain("Tempus coniunx id conductae hastam est conductae", "Inlustris Commagena civitatibus Hierapoli est Euphratensis Commagena Osdroenam dictum Euphratensis clementer Osdroenam civitatibus descriptione vetere ab Samosata ut Hierapoli."));
        api.createDomain(Fixtures.createDomain("Uni cuius libidines praeter ac exitum Pisonis nec", "Et Valerius cum absentia mariti virginis Scipionis causa adultae ex erubesceret alitur ille diuturnum et ex cum virginis mariti mariti."));

        // When
        List<Domain> results = api.findAllDomains();

        // Then
        assertEquals(3, results.size());
    }

}
