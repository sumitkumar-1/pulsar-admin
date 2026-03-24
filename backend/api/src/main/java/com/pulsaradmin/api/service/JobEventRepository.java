package com.pulsaradmin.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.shared.model.ReplayCopyJobEventResponse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobEventRepository {
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final RowMapper<ReplayCopyJobEventResponse> rowMapper = this::mapRow;

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

  public List<ReplayCopyJobEventResponse> findByJobId(String jobId) {
    return jdbcTemplate.query("""
        select id, job_id, event_type, details, created_at
        from job_events
        where job_id = ?
        order by id
        """, rowMapper, jobId);
  }

  private ReplayCopyJobEventResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new ReplayCopyJobEventResponse(
        rs.getLong("id"),
        rs.getString("job_id"),
        rs.getString("event_type"),
        readJson(rs.getString("details")),
        rs.getTimestamp("created_at") == null ? null : rs.getTimestamp("created_at").toInstant());
  }

  private Map<String, Object> readJson(String value) {
    try {
      return objectMapper.readValue(value == null || value.isBlank() ? "{}" : value, MAP_TYPE);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to read job event details.", exception);
    }
  }

  private String writeJson(Map<String, Object> value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to write job event details.", exception);
    }
  }
}
