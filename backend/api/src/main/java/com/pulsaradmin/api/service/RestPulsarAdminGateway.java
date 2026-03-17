package com.pulsaradmin.api.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicHealth;
import com.pulsaradmin.shared.model.TopicPartitionSummary;
import com.pulsaradmin.shared.model.TopicStatsSummary;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

public class RestPulsarAdminGateway implements PulsarAdminGateway {
  private final RestClient restClient;
  private final ObjectMapper objectMapper;

  public RestPulsarAdminGateway(RestClient restClient, ObjectMapper objectMapper) {
    this.restClient = restClient;
    this.objectMapper = objectMapper;
  }

  @Override
  public EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment) {
    try {
      JsonNode clusters = getJson(environment, "/admin/v2/clusters");
      int clusterCount = clusters.isArray() ? clusters.size() : 0;

      return new EnvironmentConnectionTestResult(
          environment.id(),
          true,
          "SUCCESS",
          "Connection verified against Pulsar admin REST API. Found " + clusterCount + " cluster entries.",
          Instant.now(),
          true);
    } catch (RestClientException | IOException exception) {
      return new EnvironmentConnectionTestResult(
          environment.id(),
          false,
          "FAILED",
          "Unable to reach the Pulsar admin REST API: " + exception.getMessage(),
          Instant.now(),
          false);
    }
  }

  @Override
  public EnvironmentSnapshot syncMetadata(EnvironmentDetails environment) {
    try {
      JsonNode tenantsNode = getJson(environment, "/admin/v2/tenants");
      List<String> tenants = readStringArray(tenantsNode);
      List<String> namespaces = new ArrayList<>();
      List<TopicDetails> topics = new ArrayList<>();

      for (String tenant : tenants) {
        JsonNode namespacesNode = getJson(environment, "/admin/v2/namespaces/" + tenant);
        for (String namespacePath : readStringArray(namespacesNode)) {
          namespaces.add(namespacePath);
          String[] namespaceSegments = namespacePath.split("/", 2);
          if (namespaceSegments.length != 2) {
            continue;
          }

          JsonNode topicsNode = getJson(environment, "/admin/v2/persistent/" + namespacePath);
          for (String fullTopicName : readStringArray(topicsNode)) {
            topics.add(toTopicDetails(fullTopicName, namespaceSegments[0], namespaceSegments[1]));
          }
        }
      }

      EnvironmentHealth health = new EnvironmentHealth(
          environment.id(),
          topics.isEmpty() ? EnvironmentStatus.DEGRADED : EnvironmentStatus.HEALTHY,
          environment.brokerUrl(),
          environment.adminUrl(),
          "rest-sync",
          "Metadata synced from Pulsar admin REST API. "
              + tenants.size() + " tenants, "
              + namespaces.size() + " namespaces, "
              + topics.size() + " topics.");

      return new EnvironmentSnapshot(health, tenants, namespaces, topics);
    } catch (RestClientException | IOException exception) {
      throw new BadRequestException("Unable to sync metadata from Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit) {
    throw new BadRequestException("Peek Messages is not yet available in REST gateway mode. Keep using mock mode until client-backed data-plane integration is added.");
  }

  @Override
  public ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.topicName());

    try {
      String normalizedTarget = request.target().trim().toUpperCase();

      return switch (normalizedTarget) {
        case "TIMESTAMP" -> resetCursorByTimestamp(environment, topicName, request.subscriptionName(), request.timestamp());
        case "EARLIEST" -> resetCursorToEarliest(environment, topicName, request.subscriptionName());
        case "LATEST" -> resetCursorToLatest(environment, topicName, request.subscriptionName());
        default -> throw new BadRequestException("Reset target must be EARLIEST, LATEST, or TIMESTAMP.");
      };
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to reset the subscription cursor via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.topicName());

    try {
      postWithoutBody(
          environment,
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic()
              + "/subscription/" + request.subscriptionName()
              + "/skip/" + request.messageCount()
              + "/skipMessages");

      return new SkipMessagesResponse(
          environment.id(),
          request.topicName(),
          request.subscriptionName(),
          request.messageCount(),
          "Skipped " + request.messageCount() + " messages via Pulsar admin REST API for subscription "
              + request.subscriptionName() + ".");
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to skip messages via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  private TopicDetails toTopicDetails(String fullTopicName, String tenant, String namespace) {
    PulsarTopicName parsed = PulsarTopicName.parse(fullTopicName);
    boolean partitioned = parsed.topic().contains("-partition-");
    return new TopicDetails(
        parsed.fullName(),
        tenant,
        namespace,
        parsed.topic(),
        partitioned,
        partitioned ? 1 : 0,
        TopicHealth.HEALTHY,
        new TopicStatsSummary(0, 0, 0, 0, 0, 0, 0, 0, 0),
        new SchemaSummary("UNKNOWN", "-", "UNKNOWN", false),
        "Unassigned",
        "Imported from Pulsar admin REST sync. Live stats and schema details will be enriched in later integration steps.",
        List.<TopicPartitionSummary>of(),
        List.of());
  }

  private JsonNode getJson(EnvironmentDetails environment, String path) throws IOException {
    String rawBody = restClient.get()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path)
        .retrieve()
        .body(String.class);

    if (rawBody == null || rawBody.isBlank()) {
      return objectMapper.createArrayNode();
    }

    return objectMapper.readTree(rawBody);
  }

  private void postWithoutBody(EnvironmentDetails environment, String path) {
    restClient.post()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path)
        .retrieve()
        .toBodilessEntity();
  }

  private List<String> readStringArray(JsonNode node) {
    List<String> values = new ArrayList<>();
    Set<String> deduped = new HashSet<>();
    if (node != null && node.isArray()) {
      for (JsonNode item : node) {
        if (item.isTextual() && deduped.add(item.asText())) {
          values.add(item.asText());
        }
      }
    }
    return values;
  }

  private String normalizeAdminUrl(String adminUrl) {
    return adminUrl != null && adminUrl.endsWith("/")
        ? adminUrl.substring(0, adminUrl.length() - 1)
        : adminUrl;
  }

  private ResetCursorResponse resetCursorByTimestamp(
      EnvironmentDetails environment,
      PulsarTopicName topicName,
      String subscriptionName,
      String timestamp) {
    if (timestamp == null || timestamp.isBlank()) {
      throw new BadRequestException("A timestamp is required when reset target is TIMESTAMP.");
    }

    long millis = OffsetDateTime.parse(timestamp).toInstant().toEpochMilli();
    postWithoutBody(
        environment,
        "/admin/v2/" + topicName.domain()
            + "/" + topicName.tenant()
            + "/" + topicName.namespace()
            + "/" + topicName.topic()
            + "/subscription/" + subscriptionName
            + "/resetcursor/" + millis);

    return new ResetCursorResponse(
        environment.id(),
        topicName.fullName(),
        subscriptionName,
        "TIMESTAMP",
        Instant.ofEpochMilli(millis).toString(),
        "Cursor reset by timestamp via Pulsar admin REST API for subscription " + subscriptionName + ".");
  }

  private ResetCursorResponse resetCursorToEarliest(
      EnvironmentDetails environment,
      PulsarTopicName topicName,
      String subscriptionName) {
    postWithoutBody(
        environment,
        "/admin/v2/" + topicName.domain()
            + "/" + topicName.tenant()
            + "/" + topicName.namespace()
            + "/" + topicName.topic()
            + "/subscription/" + subscriptionName
            + "/resetcursor/0");

    return new ResetCursorResponse(
        environment.id(),
        topicName.fullName(),
        subscriptionName,
        "EARLIEST",
        Instant.EPOCH.toString(),
        "Cursor reset to the earliest available position via Pulsar admin REST API for subscription "
            + subscriptionName + ".");
  }

  private ResetCursorResponse resetCursorToLatest(
      EnvironmentDetails environment,
      PulsarTopicName topicName,
      String subscriptionName) {
    postWithoutBody(
        environment,
        "/admin/v2/" + topicName.domain()
            + "/" + topicName.tenant()
            + "/" + topicName.namespace()
            + "/" + topicName.topic()
            + "/subscription/" + subscriptionName
            + "/skip_all/skipAllMessages");

    return new ResetCursorResponse(
        environment.id(),
        topicName.fullName(),
        subscriptionName,
        "LATEST",
        null,
        "Cursor moved to the latest position by clearing backlog via Pulsar admin REST API for subscription "
            + subscriptionName + ".");
  }
}
