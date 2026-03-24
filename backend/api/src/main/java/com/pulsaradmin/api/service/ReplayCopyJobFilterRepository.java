package com.pulsaradmin.api.service;

import java.util.Collection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ReplayCopyJobFilterRepository {
  private final JdbcTemplate jdbcTemplate;

  public ReplayCopyJobFilterRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void insertValues(String jobId, Collection<String> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    jdbcTemplate.batchUpdate(
        "insert into replay_copy_job_filters (job_id, filter_value, created_at) values (?, ?, ?)",
        values,
        values.size(),
        (ps, value) -> {
          ps.setString(1, jobId);
          ps.setString(2, value);
          ps.setTimestamp(3, java.sql.Timestamp.from(java.time.Instant.now()));
        });
  }
}
