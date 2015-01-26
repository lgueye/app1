package org.diveintojee.poc.ansiblecluster;

import org.apache.commons.lang3.RandomStringUtils;
import org.diveintojee.poc.ansiblecluster.domain.Domain;

public class Fixtures {

    public static Domain validDomain() {
        Domain domain = new Domain();
        domain.setDescription(RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE));
        domain.setTitle(RandomStringUtils.randomAlphanumeric(Domain.TITLE_MAX_SIZE));
        return domain;
    }

    public static Domain createDomain(String title, String description) {
        Domain domain = new Domain();
        domain.setTitle(title);
        domain.setDescription(description);
        return domain;
    }
}
