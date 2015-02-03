package org.diveintojee.poc.digitaloceancluster.app1;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.Maps;

@Component
public class ClusterAppClient {

    @Autowired
    RestOperations restTemplate;
    @Value("${cluster-app.host}")
    private String host;
    @Value("${cluster-app.port}")
    private int port;

    private String getResourceLocation() {
        return "http://" + host + ":" + port + "/api/domains";
    }

    public URI createDomain(Domain domain) throws IOException {
        return restTemplate.postForLocation(getResourceLocation(), domain);
    }

    public Domain loadDomain(URI uri) {
        Domain persisted = restTemplate.getForObject(uri, Domain.class);
        assertNotNull(persisted);
        return persisted;
    }

    public void updateDomain(URI uri, Domain persisted) {
        restTemplate.put(uri, persisted);
    }

    public void deleteResource(URI uri) {
        restTemplate.delete(uri);
    }

    public void deleteAllDomains() {
        restTemplate.delete(getResourceLocation());
    }

    public List<Domain> findAllDomains() {
        Map<String, ?> uriVariables = Maps.newHashMap();
        HttpEntity entity = new HttpEntity(null);
        ResponseEntity<List<Domain>> response = new RestTemplate()
                .exchange(getResourceLocation(), HttpMethod.GET, entity, new ParameterizedTypeReference<List<Domain>>() {
                }, uriVariables);
        return response.getBody();
    }

    public List<Domain> searchDomains(String keyword) {
        Map<String, String> params = Maps.newHashMap();
        params.put("q", keyword);
        ResponseEntity<List<Domain>> response = restTemplate.exchange(getResourceLocation() + "/search?q={q}", HttpMethod.GET, null, new ParameterizedTypeReference<List<Domain>>() {
        }, params);
        return response.getBody();
    }

}
