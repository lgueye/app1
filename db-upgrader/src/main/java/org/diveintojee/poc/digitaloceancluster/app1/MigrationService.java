package org.diveintojee.poc.digitaloceancluster.app1;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MigrationService {

    @Autowired
	private Flyway flyway;

	public void migrate() {
		this.flyway.migrate();
	}
}
