package org.diveintojee.poc.digitaloceancluster.app1;

import org.flywaydb.core.Flyway;

public class MigrationService {

	private Flyway flyway;

	public MigrationService(Flyway flyway) {
		this.flyway = flyway;
	}
	public void migrate() {
		this.flyway.migrate();
	}
}
