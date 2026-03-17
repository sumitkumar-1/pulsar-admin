package com.pulsaradmin.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.TopicDetails;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class EnvironmentSnapshotRepository {
  private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {};
  private static final TypeReference<List<TopicDetails>> TOPIC_LIST = new TypeReference<>() {};

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;
  private final RowMapper<EnvironmentSnapshotRecord> rowMapper = this::mapRow;

  public EnvironmentSnapshotRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  public Optional<EnvironmentSnapshotRecord> findByEnvironmentId(String environmentId) {
    List<EnvironmentSnapshotRecord> rows = jdbcTemplate.query("""
        select * from environment_snapshots where environment_id = ?
        """, rowMapper, environmentId);
    return rows.stream().findFirst();
  }

  public void upsert(String environmentId, EnvironmentSnapshot snapshot) {
    String tenants = write(snapshot.tenants());
    String namespaces = write(snapshot.namespaces());
    String topics = write(snapshot.topics());

    Integer updated = jdbcTemplate.update("""
        update environment_snapshots
        set health_status = ?, health_message = ?, pulsar_version = ?, broker_url = ?, admin_url = ?,
            tenant_count = ?, namespace_count = ?, topic_count = ?,
            tenants_json = ?, namespaces_json = ?, topics_json = ?, updated_at = current_timestamp
        where environment_id = ?
        """,
        snapshot.health().status().name(),
        snapshot.health().message(),
        snapshot.health().pulsarVersion(),
        snapshot.health().brokerUrl(),
        snapshot.health().adminUrl(),
        snapshot.tenants().size(),
        snapshot.namespaces().size(),
        snapshot.topics().size(),
        tenants,
        namespaces,
        topics,
        environmentId);

    if (updated == 0) {
      jdbcTemplate.update("""
          insert into environment_snapshots (
            environment_id, health_status, health_message, pulsar_version, broker_url, admin_url,
            tenant_count, namespace_count, topic_count, tenants_json, namespaces_json, topics_json
          ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
          environmentId,
          snapshot.health().status().name(),
          snapshot.health().message(),
          snapshot.health().pulsarVersion(),
          snapshot.health().brokerUrl(),
          snapshot.health().adminUrl(),
          snapshot.tenants().size(),
          snapshot.namespaces().size(),
          snapshot.topics().size(),
          tenants,
          namespaces,
          topics);
    }
  }

  private EnvironmentSnapshotRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new EnvironmentSnapshotRecord(
        rs.getString("environment_id"),
        EnvironmentStatus.valueOf(rs.getString("health_status")),
        rs.getString("health_message"),
        rs.getString("pulsar_version"),
        rs.getString("broker_url"),
        rs.getString("admin_url"),
        rs.getInt("tenant_count"),
        rs.getInt("namespace_count"),
        rs.getInt("topic_count"),
        readStringList(rs.getString("tenants_json")),
        readStringList(rs.getString("namespaces_json")),
        readTopicList(rs.getString("topics_json")),
        instant(rs.getTimestamp("updated_at")));
  }

  private List<String> readStringList(String json) {
    try {
      return objectMapper.readValue(json, STRING_LIST);
    } catch (JsonProcessingException exception) {
      throw new BadRequestException("Unable to read environment snapshot list data.");
    }
  }

  private List<TopicDetails> readTopicList(String json) {
    try {
      return objectMapper.readValue(json, TOPIC_LIST);
    } catch (JsonProcessingException exception) {
      throw new BadRequestException("Unable to read environment snapshot topic data.");
    }
  }

  private String write(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new BadRequestException("Unable to write environment snapshot data.");
    }
  }

  private Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
