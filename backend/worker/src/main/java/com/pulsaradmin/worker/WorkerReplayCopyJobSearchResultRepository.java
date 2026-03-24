package com.pulsaradmin.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerReplayCopyJobSearchResultRepository {
  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  public WorkerReplayCopyJobSearchResultRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  public void insert(
      String jobId,
      String messageId,
      String messageKey,
      Map<String, String> properties,
      String payload,
      String matchedFieldValue) {
    jdbcTemplate.update("""
        insert into replay_copy_job_search_results
          (job_id, message_id, message_key, properties_json, payload, matched_field_value, created_at)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        jobId,
        messageId,
        messageKey,
        writeJson(properties),
        payload == null ? "" : payload,
        matchedFieldValue,
        Timestamp.from(Instant.now()));
  }

  private String writeJson(Map<String, String> value) {
    try {
      return objectMapper.writeValueAsString(value == null ? Map.of() : value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to write search result properties.", exception);
    }
  }
}
