package com.pulsaradmin.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobEventRepository {
  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  public JobEventRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
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
        java.sql.Timestamp.from(Instant.now()));
  }

  private String writeJson(Map<String, Object> value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to write job event details.", exception);
    }
  }
}
