package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class EnvironmentRepository {
  private final JdbcTemplate jdbcTemplate;
  private final RowMapper<EnvironmentRecord> rowMapper = this::mapRow;

  public EnvironmentRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public List<EnvironmentRecord> findAllActive() {
    return jdbcTemplate.query("""
        select * from environments
        where deleted_at is null
        order by name
        """, rowMapper);
  }

  public Optional<EnvironmentRecord> findActiveById(String id) {
    List<EnvironmentRecord> rows = jdbcTemplate.query("""
        select * from environments
        where id = ? and deleted_at is null
        """, rowMapper, id);
    return rows.stream().findFirst();
  }

  public Optional<EnvironmentRecord> findById(String id) {
    List<EnvironmentRecord> rows = jdbcTemplate.query("""
        select * from environments
        where id = ?
        """, rowMapper, id);
    return rows.stream().findFirst();
  }

  public boolean existsActiveById(String id) {
    Integer count = jdbcTemplate.queryForObject("""
        select count(*) from environments
        where id = ? and deleted_at is null
        """, Integer.class, id);
    return count != null && count > 0;
  }

  public void insert(EnvironmentRecord environment) {
    jdbcTemplate.update("""
        insert into environments (
          id, name, kind, region, cluster_label, summary, broker_url, admin_url,
          auth_mode, credential_reference, tls_enabled, status, sync_status, sync_message,
          last_synced_at, last_test_status, last_test_message, last_tested_at, deleted_at,
          created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
        """,
        environment.id(),
        environment.name(),
        environment.kind(),
        environment.region(),
        environment.clusterLabel(),
        environment.summary(),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.authMode(),
        environment.credentialReference(),
        environment.tlsEnabled(),
        environment.status().name(),
        environment.syncStatus(),
        environment.syncMessage(),
        timestamp(environment.lastSyncedAt()),
        environment.lastTestStatus(),
        environment.lastTestMessage(),
        timestamp(environment.lastTestedAt()),
        timestamp(environment.deletedAt()));
  }

  public void update(EnvironmentRecord environment) {
    jdbcTemplate.update("""
        update environments
        set name = ?, kind = ?, region = ?, cluster_label = ?, summary = ?, broker_url = ?, admin_url = ?,
            auth_mode = ?, credential_reference = ?, tls_enabled = ?, status = ?, sync_status = ?, sync_message = ?,
            last_synced_at = ?, last_test_status = ?, last_test_message = ?, last_tested_at = ?, deleted_at = ?, updated_at = current_timestamp
        where id = ?
        """,
        environment.name(),
        environment.kind(),
        environment.region(),
        environment.clusterLabel(),
        environment.summary(),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.authMode(),
        environment.credentialReference(),
        environment.tlsEnabled(),
        environment.status().name(),
        environment.syncStatus(),
        environment.syncMessage(),
        timestamp(environment.lastSyncedAt()),
        environment.lastTestStatus(),
        environment.lastTestMessage(),
        timestamp(environment.lastTestedAt()),
        timestamp(environment.deletedAt()),
        environment.id());
  }

  private EnvironmentRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new EnvironmentRecord(
        rs.getString("id"),
        rs.getString("name"),
        rs.getString("kind"),
        rs.getString("region"),
        rs.getString("cluster_label"),
        rs.getString("summary"),
        rs.getString("broker_url"),
        rs.getString("admin_url"),
        rs.getString("auth_mode"),
        rs.getString("credential_reference"),
        rs.getBoolean("tls_enabled"),
        EnvironmentStatus.valueOf(rs.getString("status")),
        rs.getString("sync_status"),
        rs.getString("sync_message"),
        instant(rs.getTimestamp("last_synced_at")),
        rs.getString("last_test_status"),
        rs.getString("last_test_message"),
        instant(rs.getTimestamp("last_tested_at")),
        instant(rs.getTimestamp("deleted_at")));
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }

  private Timestamp timestamp(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }
}
