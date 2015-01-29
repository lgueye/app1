package org.diveintojee.poc.ansiblecluster.service;

import com.google.common.collect.Lists;
import org.diveintojee.poc.ansiblecluster.domain.Domain;
import org.diveintojee.poc.ansiblecluster.persistence.data.DatabaseRepository;
import org.diveintojee.poc.ansiblecluster.persistence.index.IndexRepository;
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

    @Transactional
    public Long save(@NotNull @Valid final Domain domain) {
        Domain persisted = databaseRepository.save(domain);
//        if (persisted == null) throw new IllegalStateException("Not null entity expected");
//        if (persisted.getId() == null) throw new IllegalStateException("Not null identifier expected");
        indexRepository.save(persisted);
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
        indexRepository.save(persisted);
    }

    public void delete(Long id) {
        databaseRepository.delete(id);
        indexRepository.delete(id);
    }

    public void delete() {
        databaseRepository.deleteAll();
        indexRepository.deleteAll();
    }
}
