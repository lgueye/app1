package org.diveintojee.poc.digitaloceancluster.app1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.RandomStringUtils;
import org.diveintojee.poc.ansiblecluster.domain.Domain;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class MigrationTest extends ElasticsearchIntegrationTest {

	@Test
	public void crudShouldSucceed() throws IOException {
		final AdminClient admin = ElasticsearchIntegrationTest.client().admin();
		// Create index
		String settingsSource = Resources.toString(Resources.getResource("migration/domains_v1/settings.json"), Charsets.UTF_8);;
		admin.indices().prepareCreate("domains_v1").setSource(settingsSource).execute().actionGet();
		// Add mappings
		String mappingSource = Resources.toString(Resources.getResource("migration/domains_v1/mappings/domains.json"), Charsets.UTF_8);
		admin.indices().preparePutMapping("domains_v1").setSource(mappingSource).execute().actionGet();
		// Add alias
		admin.indices().prepareAliases().addAlias("domains_v1", "domains").execute().actionGet();

		final Domain domain = new Domain();
		Long id = 1L;
		domain.setId(id);
		domain.setTitle(RandomStringUtils.randomAlphanumeric(Domain.TITLE_MAX_SIZE));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE));
		// Create
		createDomain(domain);
		assertNotNull(id);
		// Read
		Domain saved = retrieveDomain(id);
		assertNotNull(saved);
		assertEquals(id, saved.getId());
		// Update
		String newDescription = RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE);
		saved.setDescription(newDescription);
		updateDomain(saved);
		Domain updated = retrieveDomain(id);
		assertEquals(newDescription, updated.getDescription());
		// Delete
		deleteDomain(id);
		try {
			retrieveDomain(id);
		} catch (EmptyResultDataAccessException e) {
		} catch (Throwable t) {
			Assert.fail("EmptyResultDataAccessException expected, got " + t);
		}

	}


	private void deleteDomain(Long id) {
		String query = "delete from domains where id = ?";
		Object[] args = new Object[] {id};
		int out = jdbcTemplate.update(query, args);
		assertEquals(1, out);
	}

	private void updateDomain(Domain domain) {
		String query = "update domains set title = ?, description = ? where id = ?";
		Object[] args = new Object[] {domain.getTitle(), domain.getDescription(), domain.getId()};
		int out = jdbcTemplate.update(query, args);
		assertEquals(1, out);
	}

	private Domain retrieveDomain(Long id) {
		String query = "select id, title, description from domains where id = ?";

		//using RowMapper anonymous class, we can create a separate RowMapper for reuse
		return jdbcTemplate.queryForObject(query, new Object[]{id}, new RowMapper<Domain>(){
			@Override
			public Domain mapRow(ResultSet rs, int rowNum)
					throws SQLException {
				Domain domain = new Domain();
				domain.setId(rs.getLong("id"));
				domain.setTitle(rs.getString("title"));
				domain.setDescription(rs.getString("description"));
				return domain;
			}});
	}

	private Long createDomain(final Domain domain) {
		final Client client = ElasticsearchIntegrationTest.client();
		client.prepareIndex()
	}

}
