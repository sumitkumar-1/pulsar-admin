package com.pulsaradmin.worker;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerEnvironmentRepository {
  private final JdbcTemplate jdbcTemplate;

  public WorkerEnvironmentRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public Optional<WorkerEnvironmentRecord> findActiveById(String id) {
    List<WorkerEnvironmentRecord> rows = jdbcTemplate.query("""
        select id, broker_url, admin_url, auth_mode, credential_reference, tls_enabled
        from environments
        where id = ? and deleted_at is null
        """, (rs, rowNum) -> new WorkerEnvironmentRecord(
        rs.getString("id"),
        rs.getString("broker_url"),
        rs.getString("admin_url"),
        rs.getString("auth_mode"),
        rs.getString("credential_reference"),
        rs.getBoolean("tls_enabled")), id);
    return rows.stream().findFirst();
  }
}
