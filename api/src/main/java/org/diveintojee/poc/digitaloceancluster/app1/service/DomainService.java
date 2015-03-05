package org.diveintojee.poc.digitaloceancluster.app1.service;

import com.google.common.collect.Lists;
import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.diveintojee.poc.digitaloceancluster.app1.persistence.data.DatabaseRepository;
import org.diveintojee.poc.digitaloceancluster.app1.persistence.index.IndexRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.transaction.Transactional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author louis.gueye@gmail.com
 */
@Component
@Validated
public class DomainService {

    @Autowired
    private DatabaseRepository databaseRepository;

    @Autowired
    private IndexRepository indexRepository;

    private static final Logger LOGGER = LoggerFactory.getLogger(DomainService.class);

    @Transactional
    public Long save(@NotNull @Valid final Domain domain) {
        Domain persisted = databaseRepository.save(domain);
        LOGGER.debug("Saved domain in db : {}", persisted);
//        if (persisted == null) throw new IllegalStateException("Not null entity expected");
//        if (persisted.getId() == null) throw new IllegalStateException("Not null identifier expected");
        indexRepository.save(persisted);
        LOGGER.debug("Indexed domain in db : {}", persisted);
        return persisted.getId();
    }

    public List<Domain> findAll() {
        return Lists.newArrayList(indexRepository.findAll());
    }

    public List<Domain> search(String query) {
        return indexRepository.search(query);
    }

    public Domain getOne(Long id) {
        return indexRepository.findOne(id);
    }

    public void update(Long id, @NotNull @Valid final Domain domain) {
        Domain persisted = databaseRepository.save(domain);
        LOGGER.debug("Updated domain in db : {}", persisted);
        indexRepository.save(persisted);
        LOGGER.debug("Indexed domain in db : {}", persisted);
    }

    public void delete(Long id) {
        databaseRepository.delete(id);
        LOGGER.debug("Deleted domain from db : {}", id);
        indexRepository.delete(id);
        LOGGER.debug("Deleted domain from index : {}", id);
    }

    public void delete() {
        databaseRepository.deleteAll();
        LOGGER.debug("Deleted all domains from db");
        indexRepository.deleteAll();
        LOGGER.debug("Deleted all domains from index");
    }
}
