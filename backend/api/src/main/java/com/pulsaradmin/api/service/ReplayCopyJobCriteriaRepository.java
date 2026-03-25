package com.pulsaradmin.api.service;

import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ReplayCopyJobCriteriaRepository {
  private final JdbcTemplate jdbcTemplate;

  public ReplayCopyJobCriteriaRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void insertRows(String jobId, List<Map<String, String>> rows) {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      int currentRowIndex = rowIndex;
      Map<String, String> row = rows.get(rowIndex);
      if (row == null || row.isEmpty()) {
        continue;
      }
      jdbcTemplate.batchUpdate(
          "insert into replay_copy_job_criteria (job_id, row_index, field_name, field_value, created_at) values (?, ?, ?, ?, ?)",
          row.entrySet(),
          row.size(),
          (ps, entry) -> {
            ps.setString(1, jobId);
            ps.setInt(2, currentRowIndex);
            ps.setString(3, entry.getKey());
            ps.setString(4, entry.getValue());
            ps.setTimestamp(5, java.sql.Timestamp.from(java.time.Instant.now()));
          });
    }
  }
}
