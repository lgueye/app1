package org.diveintojee.poc.ansiblecluster.persistence.data;

import org.diveintojee.poc.ansiblecluster.domain.Domain;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;

/**
 * @author louis.gueye@gmail.com
 */
@Transactional
public interface DatabaseRepository extends CrudRepository<Domain, Long> {
}
