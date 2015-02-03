package org.diveintojee.poc.digitaloceancluster.app1.persistence.data;

import javax.transaction.Transactional;

import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.springframework.data.repository.CrudRepository;

/**
 * @author louis.gueye@gmail.com
 */
@Transactional
public interface DatabaseRepository extends CrudRepository<Domain, Long> {
}
