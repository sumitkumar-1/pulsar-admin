package com.pulsaradmin.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.shared.job.JobRecord;
import com.pulsaradmin.shared.job.JobStatus;
import com.pulsaradmin.shared.job.JobType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerJobRepository {
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final RowMapper<JobRecord> rowMapper = this::mapRow;

  public WorkerJobRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  public List<JobRecord> findQueuedReplayCopyJobs(int limit) {
    return jdbcTemplate.query("""
        select * from jobs
        where status = 'QUEUED' and type in ('REPLAY', 'COPY')
        order by created_at
        limit ?
        """, rowMapper, limit);
  }

  public boolean claimJob(String jobId) {
    int updated = jdbcTemplate.update("""
        update jobs
        set status = 'RUNNING', updated_at = ?
        where id = ? and status = 'QUEUED'
        """,
        Timestamp.from(Instant.now()),
        jobId);
    return updated == 1;
  }

  public Optional<JobRecord> findById(String jobId) {
    List<JobRecord> rows = jdbcTemplate.query("""
        select * from jobs where id = ?
        """, rowMapper, jobId);
    return rows.stream().findFirst();
  }

  public void update(JobRecord job) {
    jdbcTemplate.update("""
        update jobs
        set status = ?, parameters = ?, updated_at = ?
        where id = ?
        """,
        job.status().name(),
        writeJson(job.parameters()),
        Timestamp.from(job.updatedAt()),
        job.id());
  }

  private JobRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new JobRecord(
        rs.getString("id"),
        JobType.valueOf(rs.getString("type")),
        rs.getString("environment_id"),
        JobStatus.valueOf(rs.getString("status")),
        readJson(rs.getString("parameters")),
        instant(rs.getTimestamp("created_at")),
        instant(rs.getTimestamp("updated_at")));
  }

  private Map<String, Object> readJson(String value) {
    try {
      return objectMapper.readValue(value == null || value.isBlank() ? "{}" : value, MAP_TYPE);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to read worker job parameters.", exception);
    }
  }

  private String writeJson(Map<String, Object> value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to write worker job parameters.", exception);
    }
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
