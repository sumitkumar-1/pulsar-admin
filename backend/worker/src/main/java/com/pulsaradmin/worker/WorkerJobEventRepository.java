package com.pulsaradmin.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerJobEventRepository {
  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  public WorkerJobEventRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  public void insert(String jobId, String eventType, Map<String, Object> details) {
    jdbcTemplate.update("""
        insert into job_events (job_id, event_type, details, created_at)
        values (?, ?, ?, ?)
        """,
        jobId,
        eventType,
        writeJson(details),
        Timestamp.from(Instant.now()));
  }

  private String writeJson(Map<String, Object> value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to write worker job event details.", exception);
    }
  }
}
