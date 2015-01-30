package org.diveintojee.poc.digitaloceancluster.app1.persistence.index;

import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @author louis.gueye@gmail.com
 */
public interface IndexRepository extends ElasticsearchRepository<Domain, Long> {

    public static final String DOMAINS_QUERY = "{\"bool\" : {\"should\" : [ {\"wildcard\" : { \"title\" : \"*?0*\" }}, {\"wildcard\" : { \"description\" : \"*?0*\" }}]}}";

    @Query(DOMAINS_QUERY)
    List<Domain> search(String query);

}
