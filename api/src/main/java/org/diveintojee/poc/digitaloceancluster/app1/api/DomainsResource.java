package org.diveintojee.poc.digitaloceancluster.app1.api;

import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.diveintojee.poc.digitaloceancluster.app1.service.DomainService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author louis.gueye@gmail.com
 */
@RestController
@RequestMapping("/api/domains")
public class DomainsResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DomainsResource.class);

    @Autowired
    private DomainService service;

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<Void> create(@RequestBody @Valid Domain domain) {
        LOGGER.debug("Attempt to create domain: {}", domain);
        // Save domain and get new id
        Long id = this.service.save(domain);
        LOGGER.debug("Saved domain with id: {}", id);

        // Build URI
        final URI location = ServletUriComponentsBuilder.fromCurrentServletMapping().path("/api/domains/{id}").build().expand(id).toUri();
        LOGGER.debug("New resource can be found at : {}", location.toString());

        // Add uri location
        final HttpHeaders headers = new HttpHeaders();
        headers.setLocation(location);

        // Add header to response
        return new ResponseEntity<>(headers, HttpStatus.CREATED);
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<Domain> findAll() {
        LOGGER.debug("Searching all domains");
        return this.service.findAll();
    }

    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public List<Domain> search(@RequestParam("q") String query) {
        LOGGER.debug("Searching domains for which title or description contains {}", query);
        return this.service.search(query);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Domain get(@PathVariable("id") Long id) {
        Domain domain = this.service.getOne(id);
        if (domain == null) {
            final String message = "Domain with id {" + id + "} was not found";
            LOGGER.debug(message);
            throw new ResourceNotFoundException(message);
        }
        LOGGER.debug("Found domain with id: {}", id);
        return domain;
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void update(@PathVariable("id") Long id, @RequestBody @Valid Domain domain) {
        service.update(id, domain);
        LOGGER.debug("Updated domain with id: {}", id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void delete(@PathVariable("id") Long id) {
        this.service.delete(id);
        LOGGER.debug("Deleted domain with id: {}", id);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Domain not found")
    public void notFound() {
    }

    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void delete() {
        this.service.delete();
        LOGGER.debug("Deleted all domains");
    }

    @RequestMapping(value = "/index/refresh", method = RequestMethod.POST)
    public ResponseEntity<Void> refreshIndex() throws ExecutionException, InterruptedException {
        this.service.refreshIndex("domains");
        LOGGER.debug("Refreshing index");

        // Add header to response
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
