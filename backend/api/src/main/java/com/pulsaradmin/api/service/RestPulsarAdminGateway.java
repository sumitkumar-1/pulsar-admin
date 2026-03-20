package com.pulsaradmin.api.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
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
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

public class RestPulsarAdminGateway implements PulsarAdminGateway {
  private static final int CLIENT_OPERATION_TIMEOUT_SECONDS = 5;
  private static final int PEEK_READ_TIMEOUT_MS = 250;
  private static final int PEEK_ROLLBACK_DAYS = 3650;
  private static final int MAX_PAYLOAD_PREVIEW_CHARS = 1600;
  private static final int MAX_SUMMARY_CHARS = 140;

  private final RestClient restClient;
  private final ObjectMapper objectMapper;
  private final PulsarClientFactory pulsarClientFactory;
  private final ConcurrentMap<String, PulsarClient> clientCache = new ConcurrentHashMap<>();

  public RestPulsarAdminGateway(RestClient restClient, ObjectMapper objectMapper) {
    this(restClient, objectMapper, RestPulsarAdminGateway::buildClient);
  }

  RestPulsarAdminGateway(
      RestClient restClient,
      ObjectMapper objectMapper,
      PulsarClientFactory pulsarClientFactory) {
    this.restClient = restClient;
    this.objectMapper = objectMapper;
    this.pulsarClientFactory = pulsarClientFactory;
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
            topics.add(toTopicDetails(environment, fullTopicName, namespaceSegments[0], namespaceSegments[1]));
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
  public void createTopic(EnvironmentDetails environment, CreateTopicRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.fullTopicName());

    try {
      if (request.partitions() > 0) {
        putJson(
            environment,
            "/admin/v2/" + topicName.domain()
                + "/" + topicName.tenant()
                + "/" + topicName.namespace()
                + "/" + topicName.topic()
                + "/partitions",
            String.valueOf(request.partitions()));
        return;
      }

      putWithoutBody(
          environment,
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic());
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to create topic via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void createSubscription(EnvironmentDetails environment, CreateSubscriptionRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.topicName());
    String subscriptionName = request.subscriptionName().trim();

    try {
      putWithoutBody(
          environment,
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic()
              + "/subscription/" + subscriptionName);

      if ("EARLIEST".equalsIgnoreCase(request.initialPosition())) {
        resetCursorToEarliest(environment, topicName, subscriptionName);
      }
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to create subscription via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void deleteSubscription(EnvironmentDetails environment, String topicName, String subscriptionName) {
    PulsarTopicName parsed = PulsarTopicName.parse(topicName);

    try {
      deleteWithoutBody(
          environment,
          "/admin/v2/" + parsed.domain()
              + "/" + parsed.tenant()
              + "/" + parsed.namespace()
              + "/" + parsed.topic()
              + "/subscription/" + subscriptionName);
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to delete subscription via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit) {
    try {
      PulsarClient client = getOrCreateClient(environment);
      List<String> targetTopics = resolveTargetTopics(client, topicName);
      List<com.pulsaradmin.shared.model.PeekMessage> messages = readMessages(client, targetTopics, limit);

      return new PeekMessagesResponse(
          environment.id(),
          topicName,
          limit,
          messages.size(),
          messages.size() == limit,
          messages);
    } catch (PulsarClientException | ExecutionException | InterruptedException exception) {
      if (exception instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new BadRequestException("Unable to peek messages through the Pulsar client: " + exception.getMessage());
    }
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

  @Override
  public UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.topicName());

    try {
      putWithoutBody(
          environment,
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic()
              + "/unload");

      TopicDetails refreshedTopic = toTopicDetails(
          environment,
          topicName.fullName(),
          topicName.tenant(),
          topicName.namespace());

      return new UnloadTopicResponse(
          environment.id(),
          request.topicName(),
          "Unloaded topic " + request.topicName()
              + " via Pulsar admin REST API. Broker ownership can now rebalance cleanly.",
          refreshedTopic);
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to unload topic via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  private TopicDetails toTopicDetails(
      EnvironmentDetails environment,
      String fullTopicName,
      String tenant,
      String namespace) {
    PulsarTopicName parsed = PulsarTopicName.parse(fullTopicName);
    JsonNode statsNode = safeGetJson(environment, statsPath(parsed));
    JsonNode schemaNode = safeGetJson(environment, schemaPath(parsed));

    TopicStatsSummary stats = toTopicStats(statsNode);
    List<String> subscriptions = readSubscriptions(statsNode);
    List<TopicPartitionSummary> partitionSummaries = readPartitionSummaries(statsNode);
    boolean partitioned = !partitionSummaries.isEmpty() || parsed.topic().contains("-partition-");
    SchemaSummary schema = toSchemaSummary(schemaNode);
    TopicHealth health = deriveTopicHealth(stats, subscriptions);
    String notes = buildTopicNotes(schema, statsNode, subscriptions);

    return new TopicDetails(
        parsed.fullName(),
        tenant,
        namespace,
        parsed.topic(),
        partitioned,
        partitioned ? Math.max(1, partitionSummaries.size()) : 0,
        health,
        stats,
        schema,
        "Unassigned",
        notes,
        partitionSummaries,
        subscriptions);
  }

  private JsonNode getJson(EnvironmentDetails environment, String path) throws IOException {
    var request = restClient.get()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path);

    applyAuthHeaders(request, environment);

    String rawBody = request.retrieve().body(String.class);

    if (rawBody == null || rawBody.isBlank()) {
      return objectMapper.createArrayNode();
    }

    return objectMapper.readTree(rawBody);
  }

  private JsonNode safeGetJson(EnvironmentDetails environment, String path) {
    try {
      return getJson(environment, path);
    } catch (IOException | RestClientException exception) {
      return objectMapper.createObjectNode();
    }
  }

  private void postWithoutBody(EnvironmentDetails environment, String path) {
    var request = restClient.post()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path);

    applyAuthHeaders(request, environment);

    request.retrieve().toBodilessEntity();
  }

  private void putWithoutBody(EnvironmentDetails environment, String path) {
    var request = restClient.put()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path);

    applyAuthHeaders(request, environment);

    request.retrieve().toBodilessEntity();
  }

  private void putJson(EnvironmentDetails environment, String path, String body) {
    var request = restClient.put()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path)
        .contentType(org.springframework.http.MediaType.APPLICATION_JSON);

    applyAuthHeaders(request, environment);

    request.body(body).retrieve().toBodilessEntity();
  }

  private void deleteWithoutBody(EnvironmentDetails environment, String path) {
    var request = restClient.delete()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path);

    applyAuthHeaders(request, environment);

    request.retrieve().toBodilessEntity();
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

  private String statsPath(PulsarTopicName topicName) {
    return "/admin/v2/" + topicName.domain()
        + "/" + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.topic()
        + "/stats";
  }

  private String schemaPath(PulsarTopicName topicName) {
    return "/admin/v2/schemas/"
        + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.topic()
        + "/schema";
  }

  private TopicStatsSummary toTopicStats(JsonNode statsNode) {
    JsonNode subscriptionsNode = statsNode.path("subscriptions");
    JsonNode producersNode = statsNode.path("publishers").isArray()
        ? statsNode.path("publishers")
        : statsNode.path("producers");

    long backlog = 0;
    int consumers = 0;
    int subscriptionCount = 0;

    if (subscriptionsNode.isObject()) {
      subscriptionCount = subscriptionsNode.size();
      for (JsonNode subscriptionNode : iterable(subscriptionsNode.elements())) {
        backlog += subscriptionNode.path("msgBacklog").asLong(0);
        JsonNode consumerNode = subscriptionNode.path("consumers");
        if (consumerNode.isArray()) {
          consumers += consumerNode.size();
        }
      }
    }

    return new TopicStatsSummary(
        backlog,
        producersNode.isArray() ? producersNode.size() : 0,
        subscriptionCount,
        consumers,
        statsNode.path("msgRateIn").asDouble(0),
        statsNode.path("msgRateOut").asDouble(0),
        statsNode.path("msgThroughputIn").asDouble(0),
        statsNode.path("msgThroughputOut").asDouble(0),
        statsNode.path("storageSize").asLong(0));
  }

  private List<String> readSubscriptions(JsonNode statsNode) {
    JsonNode subscriptionsNode = statsNode.path("subscriptions");
    if (!subscriptionsNode.isObject()) {
      return List.of();
    }

    return StreamSupport.stream(iterable(subscriptionsNode.fieldNames()).spliterator(), false)
        .sorted()
        .toList();
  }

  private List<TopicPartitionSummary> readPartitionSummaries(JsonNode statsNode) {
    JsonNode partitionsNode = statsNode.path("partitions");
    if (!partitionsNode.isObject()) {
      return List.of();
    }

    List<TopicPartitionSummary> partitionSummaries = new ArrayList<>();
    partitionsNode.fields().forEachRemaining(entry -> {
      JsonNode partitionNode = entry.getValue();
      long backlog = sumPartitionBacklog(partitionNode.path("subscriptions"));
      int consumers = sumPartitionConsumers(partitionNode.path("subscriptions"));
      partitionSummaries.add(new TopicPartitionSummary(
          entry.getKey(),
          backlog,
          consumers,
          partitionNode.path("msgRateIn").asDouble(0),
          partitionNode.path("msgRateOut").asDouble(0),
          derivePartitionHealth(backlog, consumers)));
    });

    partitionSummaries.sort(Comparator.comparing(TopicPartitionSummary::partitionName));
    return partitionSummaries;
  }

  private long sumPartitionBacklog(JsonNode subscriptionsNode) {
    long backlog = 0;
    if (subscriptionsNode.isObject()) {
      for (JsonNode subscriptionNode : iterable(subscriptionsNode.elements())) {
        backlog += subscriptionNode.path("msgBacklog").asLong(0);
      }
    }
    return backlog;
  }

  private int sumPartitionConsumers(JsonNode subscriptionsNode) {
    int consumers = 0;
    if (subscriptionsNode.isObject()) {
      for (JsonNode subscriptionNode : iterable(subscriptionsNode.elements())) {
        JsonNode consumerNode = subscriptionNode.path("consumers");
        if (consumerNode.isArray()) {
          consumers += consumerNode.size();
        }
      }
    }
    return consumers;
  }

  private TopicHealth deriveTopicHealth(TopicStatsSummary stats, List<String> subscriptions) {
    if (stats.backlog() > 50_000 && stats.consumers() == 0) {
      return TopicHealth.CRITICAL;
    }

    if (stats.backlog() > 5_000 || (!subscriptions.isEmpty() && stats.consumers() == 0)) {
      return TopicHealth.ATTENTION;
    }

    return TopicHealth.HEALTHY;
  }

  private TopicHealth derivePartitionHealth(long backlog, int consumers) {
    if (backlog > 10_000 && consumers == 0) {
      return TopicHealth.CRITICAL;
    }

    if (backlog > 1_000 || consumers == 0) {
      return TopicHealth.ATTENTION;
    }

    return TopicHealth.HEALTHY;
  }

  private SchemaSummary toSchemaSummary(JsonNode schemaNode) {
    JsonNode dataNode = schemaNode.path("data");
    boolean present = dataNode.isObject() && dataNode.size() > 0;
    String type = dataNode.path("type").asText(
        schemaNode.path("type").asText(present ? "UNKNOWN" : "NONE"));
    String version = schemaNode.path("version").asText(
        dataNode.path("version").asText(present ? "latest" : "-"));
    String compatibility = schemaNode.path("compatibilityStrategy").asText(
        schemaNode.path("compatibility").asText("UNKNOWN"));

    return new SchemaSummary(type, version, compatibility, present);
  }

  private String buildTopicNotes(
      SchemaSummary schema,
      JsonNode statsNode,
      List<String> subscriptions) {
    StringBuilder notes = new StringBuilder("Imported from Pulsar admin REST sync.");

    if (!subscriptions.isEmpty()) {
      notes.append(" Found ").append(subscriptions.size()).append(" subscriptions.");
    }

    if (schema.present()) {
      notes.append(" Schema detected: ").append(schema.type()).append(".");
    } else {
      notes.append(" Schema metadata is unavailable for this topic.");
    }

    if (statsNode.isObject() && statsNode.size() > 0) {
      notes.append(" Live topic stats were captured during sync.");
    } else {
      notes.append(" Topic stats could not be retrieved during sync.");
    }

    return notes.toString();
  }

  private <T> Iterable<T> iterable(java.util.Iterator<T> iterator) {
    return () -> iterator;
  }

  private PulsarClient getOrCreateClient(EnvironmentDetails environment) throws PulsarClientException {
    String cacheKey = environment.id()
        + "|" + environment.brokerUrl()
        + "|" + environment.tlsEnabled()
        + "|" + environment.authMode()
        + "|" + environment.credentialReference();
    PulsarClient existing = clientCache.get(cacheKey);
    if (existing != null) {
      return existing;
    }

    PulsarClient created = pulsarClientFactory.create(environment);
    PulsarClient previous = clientCache.putIfAbsent(cacheKey, created);
    if (previous != null) {
      closeQuietly(created);
      return previous;
    }

    return created;
  }

  private static PulsarClient buildClient(EnvironmentDetails environment) throws PulsarClientException {
    var builder = PulsarClient.builder()
        .serviceUrl(environment.brokerUrl())
        .enableTls(environment.tlsEnabled())
        .operationTimeout(CLIENT_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    String authMode = environment.authMode() == null ? "" : environment.authMode().trim().toLowerCase();
    if ("token".equals(authMode)) {
      String token = EnvironmentCredentials.resolve(environment)
          .authorizationHeader()
          .replaceFirst("(?i)^Bearer\\s+", "")
          .trim();
      builder.authentication(AuthenticationFactory.token(token));
    } else if ("basic".equals(authMode)) {
      throw new BadRequestException("Live peek currently supports auth modes none and token.");
    } else if ("mtls".equals(authMode)) {
      throw new BadRequestException("mTLS environments are not yet supported by the live gateway.");
    }

    return builder.build();
  }

  private List<String> resolveTargetTopics(PulsarClient client, String topicName)
      throws ExecutionException, InterruptedException {
    List<String> partitions = client.getPartitionsForTopic(topicName).get();
    if (partitions == null || partitions.isEmpty()) {
      return List.of(topicName);
    }
    return partitions;
  }

  private List<com.pulsaradmin.shared.model.PeekMessage> readMessages(
      PulsarClient client,
      List<String> targetTopics,
      int limit) throws PulsarClientException {
    List<Reader<byte[]>> readers = new ArrayList<>();

    try {
      for (String targetTopic : targetTopics) {
        readers.add(client.newReader()
            .topic(targetTopic)
            .startMessageFromRollbackDuration(PEEK_ROLLBACK_DAYS, TimeUnit.DAYS)
            .create());
      }

      List<com.pulsaradmin.shared.model.PeekMessage> messages = new ArrayList<>();
      boolean madeProgress = true;

      while (messages.size() < limit && madeProgress) {
        madeProgress = false;
        for (Reader<byte[]> reader : readers) {
          if (messages.size() >= limit) {
            break;
          }

          Message<byte[]> message = reader.readNext(PEEK_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (message != null) {
            messages.add(toPeekMessage(message));
            madeProgress = true;
          }
        }
      }

      return messages;
    } finally {
      for (Reader<byte[]> reader : readers) {
        closeQuietly(reader);
      }
    }
  }

  private com.pulsaradmin.shared.model.PeekMessage toPeekMessage(Message<byte[]> message) {
    String payload = formatPayload(message.getData());
    String publishTime = toIsoTimestamp(message.getPublishTime());
    String eventTime = message.getEventTime() > 0 ? toIsoTimestamp(message.getEventTime()) : null;
    String key = message.hasKey() ? message.getKey() : "No key";
    String producerName = message.getProducerName() == null || message.getProducerName().isBlank()
        ? "Unknown producer"
        : message.getProducerName();

    return new com.pulsaradmin.shared.model.PeekMessage(
        String.valueOf(message.getMessageId()),
        key,
        publishTime,
        eventTime,
        producerName,
        summarizePayload(payload),
        payload,
        formatSchemaVersion(message.getSchemaVersion()));
  }

  private String formatPayload(byte[] payloadBytes) {
    if (payloadBytes == null || payloadBytes.length == 0) {
      return "";
    }

    String payload = new String(payloadBytes, StandardCharsets.UTF_8);
    if (payload.length() <= MAX_PAYLOAD_PREVIEW_CHARS) {
      return payload;
    }

    return payload.substring(0, MAX_PAYLOAD_PREVIEW_CHARS) + "\n...truncated";
  }

  private String summarizePayload(String payload) {
    if (payload == null || payload.isBlank()) {
      return "Message has an empty payload.";
    }

    String collapsed = payload.replaceAll("\\s+", " ").trim();
    if (collapsed.length() <= MAX_SUMMARY_CHARS) {
      return collapsed;
    }

    return collapsed.substring(0, MAX_SUMMARY_CHARS - 3) + "...";
  }

  private String formatSchemaVersion(byte[] schemaVersion) {
    if (schemaVersion == null || schemaVersion.length == 0) {
      return "unknown";
    }

    StringBuilder builder = new StringBuilder();
    for (byte value : schemaVersion) {
      builder.append(String.format("%02x", value));
    }
    return builder.toString();
  }

  private String toIsoTimestamp(long millis) {
    return Instant.ofEpochMilli(millis).toString();
  }

  private void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ignored) {
      // Best-effort cleanup for transient readers and cached clients.
    }
  }

  private void applyAuthHeaders(RestClient.RequestHeadersSpec<?> request, EnvironmentDetails environment) {
    EnvironmentCredentials.AuthHeaders authHeaders = EnvironmentCredentials.resolve(environment);
    if (authHeaders.present()) {
      request.header("Authorization", authHeaders.authorizationHeader());
    }
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

  @FunctionalInterface
  interface PulsarClientFactory {
    PulsarClient create(EnvironmentDetails environment) throws PulsarClientException;
  }
}
