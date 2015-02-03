package org.diveintojee.poc.digitaloceancluster.app1;

import org.apache.commons.lang.RandomStringUtils;
import org.diveintojee.poc.digitaloceancluster.app1.domain.Domain;
import org.flywaydb.core.Flyway;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MigrationTest {

	private SimpleDriverDataSource dataSource;
	private JdbcTemplate jdbcTemplate;

	@Before
	public void before() {
		dataSource = new SimpleDriverDataSource();
		dataSource.setDriverClass(org.h2.Driver.class);
		dataSource.setUsername("sa");
		dataSource.setUrl("jdbc:h2:mem:target/h2;DB_CLOSE_DELAY=-1");
		dataSource.setPassword("");
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Test
	public void migrationShouldSucceed() {
		Flyway flyway = new Flyway();
		flyway.setLocations("migrations");
		flyway.setDataSource(dataSource);
		MigrationService migrationService = new MigrationService(flyway);
		migrationService.migrate();

		final Domain domain = new Domain();
		domain.setTitle(RandomStringUtils.randomAlphanumeric(Domain.TITLE_MAX_SIZE));
		domain.setDescription(RandomStringUtils.randomAlphanumeric(Domain.DESCRIPTION_MAX_SIZE));
		// Create
		Long id = createDomain(domain);
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
		final String INSERT_QUERY = "insert into domains (title, description) values (?, ?)";
		KeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(new PreparedStatementCreator() {
			public PreparedStatement createPreparedStatement(
					Connection connection) throws SQLException {
				PreparedStatement ps = connection.prepareStatement(INSERT_QUERY, new String[] { "id" });
				ps.setString(1, domain.getTitle());
				ps.setString(2, domain.getDescription());
				return ps;
			}
		}, keyHolder);

		return keyHolder.getKey().longValue();
	}
}
