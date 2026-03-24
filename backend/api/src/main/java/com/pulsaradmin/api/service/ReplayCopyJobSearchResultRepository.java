package com.pulsaradmin.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class ReplayCopyJobSearchResultRepository {
  private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final RowMapper<ReplayCopySearchResultRecord> rowMapper = this::mapRow;

  public ReplayCopyJobSearchResultRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  public List<ReplayCopySearchResultRecord> findByJobId(String jobId) {
    return jdbcTemplate.query("""
        select id, job_id, message_id, message_key, properties_json, payload, matched_field_value
        from replay_copy_job_search_results
        where job_id = ?
        order by id
        """, rowMapper, jobId);
  }

  private ReplayCopySearchResultRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new ReplayCopySearchResultRecord(
        rs.getLong("id"),
        rs.getString("job_id"),
        rs.getString("message_id"),
        rs.getString("message_key"),
        readJson(rs.getString("properties_json")),
        rs.getString("payload"),
        rs.getString("matched_field_value"));
  }

  private Map<String, String> readJson(String value) {
    try {
      return objectMapper.readValue(value == null || value.isBlank() ? "{}" : value, MAP_TYPE);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to read search result properties.", exception);
    }
  }

  public record ReplayCopySearchResultRecord(
      long id,
      String jobId,
      String messageId,
      String messageKey,
      Map<String, String> properties,
      String payload,
      String matchedFieldValue) {
  }
}
