package com.pulsaradmin.worker;

import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerReplayCopyJobFilterRepository {
  private final JdbcTemplate jdbcTemplate;

  public WorkerReplayCopyJobFilterRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public Set<String> findValuesByJobId(String jobId) {
    return new LinkedHashSet<>(jdbcTemplate.query("""
        select filter_value
        from replay_copy_job_filters
        where job_id = ?
        order by id
        """, (rs, rowNum) -> rs.getString("filter_value"), jobId));
  }
}
