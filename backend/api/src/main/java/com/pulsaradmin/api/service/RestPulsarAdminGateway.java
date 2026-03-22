package com.pulsaradmin.api.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.ConsumedMessage;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PlatformArtifactDetails;
import com.pulsaradmin.shared.model.PlatformArtifactSummary;
import com.pulsaradmin.shared.model.PlatformArtifactMutationRequest;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SchemaDetails;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.SchemaUpdateRequest;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicHealth;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicPolicies;
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
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
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
          JsonNode partitionedTopicsNode = safeGetJson(environment, "/admin/v2/persistent/" + namespacePath + "/partitioned");
          List<String> namespaceTopics = new ArrayList<>();
          namespaceTopics.addAll(readStringArray(topicsNode));
          namespaceTopics.addAll(readStringArray(partitionedTopicsNode));

          for (String fullTopicName : canonicalTopicNames(namespaceTopics)) {
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
  public void updateTopicPartitions(EnvironmentDetails environment, String topicName, int partitions) {
    PulsarTopicName parsedTopicName = PulsarTopicName.parse(topicName);

    try {
      postJson(
          environment,
          "/admin/v2/" + parsedTopicName.domain()
              + "/" + parsedTopicName.tenant()
              + "/" + parsedTopicName.namespace()
              + "/" + parsedTopicName.topic()
              + "/partitions",
          String.valueOf(partitions));
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to update topic partitions via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void createTenant(EnvironmentDetails environment, CreateTenantRequest request) {
    String tenantName = request.tenant().trim();
    List<String> adminRoles = sanitizeStringList(request.adminRoles());
    List<String> allowedClusters = resolveAllowedClusters(environment, request.allowedClusters());

    try {
      putJson(
          environment,
          "/admin/v2/tenants/" + tenantName,
          objectMapper.writeValueAsString(Map.of(
              "adminRoles", adminRoles,
              "allowedClusters", allowedClusters)));
    } catch (RestClientException | IOException exception) {
      throw new BadRequestException("Unable to create tenant via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void createNamespace(EnvironmentDetails environment, CreateNamespaceRequest request) {
    try {
      putWithoutBody(
          environment,
          "/admin/v2/namespaces/" + request.tenant().trim() + "/" + request.namespace().trim());
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to create namespace via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public TenantDetails getTenantDetails(EnvironmentDetails environment, String tenant) {
    try {
      JsonNode tenantNode = getJson(environment, "/admin/v2/tenants/" + tenant);
      return new TenantDetails(
          environment.id(),
          tenant,
          readStringArray(tenantNode.path("adminRoles")),
          readStringArray(tenantNode.path("allowedClusters")),
          0,
          0,
          Instant.now());
    } catch (IOException | RestClientException exception) {
      throw new BadRequestException("Unable to load tenant details via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public TenantDetails updateTenant(EnvironmentDetails environment, TenantUpdateRequest request) {
    String tenantName = request.tenant().trim();
    List<String> adminRoles = sanitizeStringList(request.adminRoles());
    List<String> allowedClusters = resolveAllowedClusters(environment, request.allowedClusters());

    try {
      putJson(
          environment,
          "/admin/v2/tenants/" + tenantName,
          objectMapper.writeValueAsString(Map.of(
              "adminRoles", adminRoles,
              "allowedClusters", allowedClusters)));
      return new TenantDetails(
          environment.id(),
          tenantName,
          adminRoles,
          allowedClusters,
          0,
          0,
          Instant.now());
    } catch (RestClientException | IOException exception) {
      throw new BadRequestException("Unable to update tenant via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void deleteTenant(EnvironmentDetails environment, String tenant) {
    try {
      deleteWithoutBody(environment, "/admin/v2/tenants/" + tenant);
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to delete tenant via Pulsar admin REST API: " + exception.getMessage());
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
  public TerminateTopicResponse terminateTopic(EnvironmentDetails environment, TerminateTopicRequest request) {
    PulsarTopicName topicName = PulsarTopicName.parse(request.topicName());

    try {
      JsonNode resultNode = postForJson(
          environment,
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic()
              + "/terminate",
          null);

      String lastMessageId = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
      TopicDetails refreshedTopic = toTopicDetails(environment, topicName.fullName(), topicName.tenant(), topicName.namespace());

      return new TerminateTopicResponse(
          environment.id(),
          request.topicName(),
          lastMessageId,
          "Terminated topic " + request.topicName() + " via Pulsar admin REST API.",
          refreshedTopic);
    } catch (RestClientException | IOException exception) {
      throw new BadRequestException("Unable to terminate topic via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public TopicPolicies getTopicPolicies(EnvironmentDetails environment, String topicName) {
    PulsarTopicName parsed = PulsarTopicName.parse(topicName);
    String base = "/admin/v2/" + parsed.domain()
        + "/" + parsed.tenant()
        + "/" + parsed.namespace()
        + "/" + parsed.topic();

    return new TopicPolicies(
        readRetentionMinutes(safeGetJson(environment, base + "/retention")),
        readRetentionSizeMb(safeGetJson(environment, base + "/retention")),
        safeGetJson(environment, base + "/messageTTL").asInt(0),
        safeGetJson(environment, base + "/compactionThreshold").asLong(0),
        safeGetJson(environment, base + "/maxProducers").asInt(0),
        safeGetJson(environment, base + "/maxConsumers").asInt(0),
        safeGetJson(environment, base + "/maxSubscriptionsPerTopic").asInt(0));
  }

  @Override
  public TopicPolicies updateTopicPolicies(EnvironmentDetails environment, String topicName, TopicPolicies policies) {
    PulsarTopicName parsed = PulsarTopicName.parse(topicName);
    String base = "/admin/v2/" + parsed.domain()
        + "/" + parsed.tenant()
        + "/" + parsed.namespace()
        + "/" + parsed.topic();
    try {
      putJson(environment, base + "/retention", objectMapper.writeValueAsString(Map.of(
          "retentionTimeInMinutes", coalesceInt(policies.retentionTimeInMinutes()),
          "retentionSizeInMB", coalesceInt(policies.retentionSizeInMb()))));
      postJson(environment, base + "/messageTTL", String.valueOf(coalesceInt(policies.ttlInSeconds())));
      putJson(environment, base + "/compactionThreshold", String.valueOf(coalesceLong(policies.compactionThresholdInBytes())));
      postJson(environment, base + "/maxProducers", String.valueOf(coalesceInt(policies.maxProducers())));
      postJson(environment, base + "/maxConsumers", String.valueOf(coalesceInt(policies.maxConsumers())));
      postJson(environment, base + "/maxSubscriptionsPerTopic", String.valueOf(coalesceInt(policies.maxSubscriptions())));
      return getTopicPolicies(environment, topicName);
    } catch (IOException | RestClientException exception) {
      throw new BadRequestException("Unable to update topic policies via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public NamespaceDetails getNamespaceDetails(EnvironmentDetails environment, String tenant, String namespace) {
    EnvironmentSnapshot snapshot = syncMetadata(environment);
    List<TopicListItem> topics = snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(tenant) && topic.namespace().equals(namespace))
        .map(topic -> new TopicListItem(
            topic.fullName(),
            topic.tenant(),
            topic.namespace(),
            topic.topic(),
            topic.partitioned(),
            topic.partitions(),
            topic.schema().present(),
            topic.health(),
            topic.stats(),
            topic.notes()))
        .sorted(Comparator.comparing(TopicListItem::topic))
        .toList();

    return new NamespaceDetails(
        environment.id(),
        tenant,
        namespace,
        topics.size(),
        topics,
        getNamespacePolicies(environment, tenant, namespace),
        Instant.now(),
        "Namespace details loaded from Pulsar admin REST sync.");
  }

  @Override
  public NamespacePolicies updateNamespacePolicies(
      EnvironmentDetails environment,
      String tenant,
      String namespace,
      NamespacePolicies policies) {
    String base = "/admin/v2/namespaces/" + tenant + "/" + namespace;
    try {
      putJson(environment, base + "/retention", objectMapper.writeValueAsString(Map.of(
          "retentionTimeInMinutes", coalesceInt(policies.retentionTimeInMinutes()),
          "retentionSizeInMB", coalesceInt(policies.retentionSizeInMb()))));
      postJson(environment, base + "/messageTTL", String.valueOf(coalesceInt(policies.messageTtlInSeconds())));
      postJson(environment, base + "/deduplication", String.valueOf(Boolean.TRUE.equals(policies.deduplicationEnabled())));
      putJson(environment, base + "/backlogQuota", objectMapper.writeValueAsString(Map.of(
          "limitSize", coalesceLong(policies.backlogQuotaLimitInBytes()),
          "limitTime", coalesceInt(policies.backlogQuotaLimitTimeInSeconds()),
          "policy", "producer_request_hold")));
      putJson(environment, base + "/dispatchRate", objectMapper.writeValueAsString(Map.of(
          "dispatchThrottlingRateInMsg", coalesceInt(policies.dispatchRatePerTopicInMsg()),
          "dispatchThrottlingRateInByte", coalesceLong(policies.dispatchRatePerTopicInByte()),
          "ratePeriodInSecond", 1)));
      putJson(environment, base + "/publishRate", objectMapper.writeValueAsString(Map.of(
          "publishThrottlingRateInMsg", coalesceInt(policies.publishRateInMsg()),
          "publishThrottlingRateInByte", coalesceLong(policies.publishRateInByte()))));
      return getNamespacePolicies(environment, tenant, namespace);
    } catch (IOException | RestClientException exception) {
      throw new BadRequestException("Unable to update namespace policies via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public PublishMessageResponse publishMessage(EnvironmentDetails environment, PublishMessageRequest request) {
    try {
      PulsarClient client = getOrCreateClient(environment);
      try (Producer<byte[]> producer = client.newProducer()
          .topic(request.topicName())
          .create()) {
        var builder = producer.newMessage()
            .value(request.payload().getBytes(StandardCharsets.UTF_8));
        if (request.key() != null && !request.key().isBlank()) {
          builder.key(request.key());
        }
        if (request.properties() != null && !request.properties().isEmpty()) {
          builder.properties(request.properties());
        }
        var messageId = builder.sendAsync().get(CLIENT_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return new PublishMessageResponse(
            environment.id(),
            request.topicName(),
            String.valueOf(messageId),
            request.key(),
            request.properties() == null ? Map.of() : request.properties(),
            request.schemaMode() == null || request.schemaMode().isBlank() ? "RAW" : request.schemaMode(),
            Instant.now(),
            "Published a test message through the Pulsar client.",
            List.of());
      }
    } catch (Exception exception) {
      throw new BadRequestException("Unable to publish a test message through the Pulsar client: " + exception.getMessage());
    }
  }

  @Override
  public ConsumeMessagesResponse consumeMessages(EnvironmentDetails environment, ConsumeMessagesRequest request) {
    try {
      PulsarClient client = getOrCreateClient(environment);
      String subscriptionName = request.ephemeral()
          ? "ui-test-" + Math.abs((request.topicName() + Instant.now()).hashCode())
          : request.subscriptionName();

      try (Consumer<byte[]> consumer = client.newConsumer()
          .topic(request.topicName())
          .subscriptionName(subscriptionName)
          .subscriptionType(SubscriptionType.Exclusive)
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
          .subscribe()) {
        List<ConsumedMessage> messages = new ArrayList<>();
        long waitMillis = TimeUnit.SECONDS.toMillis(request.waitTimeSeconds());
        long deadline = System.currentTimeMillis() + waitMillis;

        while (messages.size() < request.maxMessages() && System.currentTimeMillis() < deadline) {
          long remainingMillis = Math.max(1, deadline - System.currentTimeMillis());
          int receiveTimeoutMillis = (int) Math.min(500L, remainingMillis);
          Message<byte[]> message = consumer.receive(receiveTimeoutMillis, TimeUnit.MILLISECONDS);
          if (message == null) {
            continue;
          }
          messages.add(new ConsumedMessage(
              String.valueOf(message.getMessageId()),
              message.hasKey() ? message.getKey() : null,
              message.getPublishTime() > 0 ? Instant.ofEpochMilli(message.getPublishTime()) : null,
              message.getEventTime() > 0 ? Instant.ofEpochMilli(message.getEventTime()) : null,
              message.getProperties(),
              message.getProducerName(),
              formatPayload(message.getData())));
          consumer.acknowledge(message);
        }

        return new ConsumeMessagesResponse(
            environment.id(),
            request.topicName(),
            subscriptionName,
            request.ephemeral(),
            request.maxMessages(),
            messages.size(),
            request.waitTimeSeconds(),
            true,
            Instant.now(),
            messages.isEmpty()
                ? "No messages were available within the bounded consume window."
                : "Consumed " + messages.size() + " messages through the Pulsar client.",
            messages,
            List.of());
      }
    } catch (Exception exception) {
      throw new BadRequestException("Unable to consume test messages through the Pulsar client: " + exception.getMessage());
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
              + "/skipMessages",
          "/admin/v2/" + topicName.domain()
              + "/" + topicName.tenant()
              + "/" + topicName.namespace()
              + "/" + topicName.topic()
              + "/subscription/" + request.subscriptionName()
              + "/skip/" + request.messageCount());

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

  @Override
  public void deleteTopic(EnvironmentDetails environment, String topicName) {
    PulsarTopicName parsed = PulsarTopicName.parse(topicName);
    try {
      PulsarTopicName canonical = PulsarTopicName.parse(parsed.canonicalFullName());
      int partitionCount = readPartitionCount(safeGetJson(environment, partitionedMetadataPath(canonical)));
      if (partitionCount > 0) {
        deleteWithoutBody(
            environment,
            "/admin/v2/" + canonical.domain()
                + "/" + canonical.tenant()
                + "/" + canonical.namespace()
                + "/" + canonical.topic()
                + "/partitions");
      } else {
        deleteWithoutBody(
            environment,
            "/admin/v2/" + canonical.domain()
                + "/" + canonical.tenant()
                + "/" + canonical.namespace()
                + "/" + canonical.topic());
      }
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to delete topic via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void deleteNamespace(EnvironmentDetails environment, String tenant, String namespace) {
    try {
      deleteWithoutBody(environment, "/admin/v2/namespaces/" + tenant + "/" + namespace);
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to delete namespace via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public PlatformSummary getPlatformSummary(EnvironmentDetails environment, List<String> namespaces) {
    List<PlatformArtifactSummary> functions = new ArrayList<>();
    List<PlatformArtifactSummary> sources = new ArrayList<>();
    List<PlatformArtifactSummary> sinks = new ArrayList<>();

    for (String fullNamespace : namespaces) {
      String[] segments = fullNamespace.split("/", 2);
      if (segments.length != 2) {
        continue;
      }
      String tenant = segments[0];
      String namespace = segments[1];
      functions.addAll(readPlatformArtifacts(environment, "/admin/v3/functions/" + tenant + "/" + namespace, tenant, namespace, "Configured function"));
      sources.addAll(readPlatformArtifacts(environment, "/admin/v3/source/" + tenant + "/" + namespace, tenant, namespace, "Configured source"));
      sinks.addAll(readPlatformArtifacts(environment, "/admin/v3/sink/" + tenant + "/" + namespace, tenant, namespace, "Configured sink"));
    }

    return new PlatformSummary(
        environment.id(),
        functions,
        sources,
        sinks,
        readConnectorArtifacts(environment));
  }

  @Override
  public PlatformArtifactDetails getPlatformArtifactDetails(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    String normalizedType = normalizeArtifactType(artifactType);
    if ("CONNECTOR".equals(normalizedType)) {
      JsonNode node = safeGetJson(environment, "/admin/v3/functions/connectors");
      for (JsonNode item : iterable(node.elements())) {
        String connectorName = item.path("name").asText(item.asText(""));
        if (name.equals(connectorName)) {
          return new PlatformArtifactDetails(
              environment.id(),
              normalizedType,
              connectorName,
              null,
              null,
              "AVAILABLE",
              item.path("description").asText("Connector plugin is registered in the Pulsar cluster catalog."),
              item.path("archive").asText("builtin://" + connectorName),
              item.path("className").asText(null),
              null,
              null,
              null,
              stringifyNode(item),
              false);
        }
      }
      throw new BadRequestException("Unknown connector: " + name);
    }

    requireArtifactScope(tenant, namespace, name);
    String basePath = platformArtifactBasePath(normalizedType, tenant, namespace, name);
    JsonNode config = safeGetJson(environment, basePath);
    JsonNode status = safeGetJson(environment, basePath + "/status");
    return toPlatformArtifactDetails(environment, normalizedType, tenant, namespace, name, config, status);
  }

  @Override
  public PlatformArtifactDetails upsertPlatformArtifact(
      EnvironmentDetails environment,
      PlatformArtifactMutationRequest request) {
    String normalizedType = normalizeArtifactType(request.artifactType());
    if ("CONNECTOR".equals(normalizedType)) {
      throw new BadRequestException("Connector catalog entries are read-only in live mode.");
    }

    requireArtifactScope(request.tenant(), request.namespace(), request.name());
    String path = platformArtifactBasePath(normalizedType, request.tenant(), request.namespace(), request.name());
    String body = buildPlatformArtifactBody(normalizedType, request);
    try {
      JsonNode existing = safeGetJson(environment, path);
      boolean exists = existing != null && existing.isObject() && existing.size() > 0;
      if (exists) {
        putJson(environment, path, body);
      } else {
        postJson(environment, path, body);
      }
      return getPlatformArtifactDetails(
          environment,
          normalizedType,
          request.tenant(),
          request.namespace(),
          request.name());
    } catch (RestClientException exception) {
      throw new BadRequestException(
          "Unable to save " + normalizedType.toLowerCase()
              + " via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public void deletePlatformArtifact(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    String normalizedType = normalizeArtifactType(artifactType);
    if ("CONNECTOR".equals(normalizedType)) {
      throw new BadRequestException("Connector catalog entries are read-only in live mode.");
    }

    requireArtifactScope(tenant, namespace, name);
    try {
      deleteWithoutBody(environment, platformArtifactBasePath(normalizedType, tenant, namespace, name));
    } catch (RestClientException exception) {
      throw new BadRequestException(
          "Unable to delete " + normalizedType.toLowerCase()
              + " via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  @Override
  public SchemaDetails getSchemaDetails(EnvironmentDetails environment, String topicName) {
    PulsarTopicName parsedTopic = PulsarTopicName.parse(topicName);
    TopicDetails topic = toTopicDetails(environment, topicName, parsedTopic.tenant(), parsedTopic.namespace());
    JsonNode schemaNode = safeGetJson(environment, schemaPath(parsedTopic));
    return toSchemaDetails(environment.id(), topic.fullName(), schemaNode, readSchemaCompatibility(environment, parsedTopic));
  }

  @Override
  public SchemaDetails upsertSchema(EnvironmentDetails environment, SchemaUpdateRequest request) {
    PulsarTopicName topic = PulsarTopicName.parse(request.topicName());
    String body;
    try {
      body = objectMapper.writeValueAsString(Map.of(
          "type", request.schemaType().trim().toUpperCase(),
          "schema", request.definition()));
      postJson(environment, schemaPath(topic), body);
      if (request.compatibility() != null && !request.compatibility().isBlank()) {
        putJson(
            environment,
            schemaCompatibilityPath(topic),
            objectMapper.writeValueAsString(request.compatibility().trim().toUpperCase()));
      }
      return toSchemaDetails(
          environment.id(),
          topic.canonicalFullName(),
          getJson(environment, schemaPath(topic)),
          readSchemaCompatibility(environment, topic));
    } catch (IOException | RestClientException exception) {
      throw new BadRequestException(
          "Unable to save schema via Pulsar admin REST API: "
              + exception.getMessage()
              + ". For JSON and AVRO schemas, provide a Pulsar-compatible record definition instead of generic JSON Schema.");
    }
  }

  @Override
  public void deleteSchema(EnvironmentDetails environment, String topicName) {
    try {
      deleteWithoutBody(environment, schemaPath(PulsarTopicName.parse(topicName)));
    } catch (RestClientException exception) {
      throw new BadRequestException("Unable to delete schema via Pulsar admin REST API: " + exception.getMessage());
    }
  }

  private TopicDetails toTopicDetails(
      EnvironmentDetails environment,
      String fullTopicName,
      String tenant,
      String namespace) {
    PulsarTopicName parsed = PulsarTopicName.parse(fullTopicName);
    int partitionCount = readPartitionCount(safeGetJson(environment, partitionedMetadataPath(parsed)));
    boolean partitioned = partitionCount > 0;
    JsonNode statsNode = safeGetJson(environment, partitioned ? partitionedStatsPath(parsed) : statsPath(parsed));
    JsonNode schemaNode = safeGetJson(environment, schemaPath(parsed));

    TopicStatsSummary stats = toTopicStats(statsNode);
    List<String> subscriptions = readSubscriptions(statsNode);
    List<TopicPartitionSummary> partitionSummaries = completePartitionSummaries(
        parsed.canonicalFullName(),
        readPartitionSummaries(statsNode),
        partitionCount);
    if (!partitioned) {
      partitioned = !partitionSummaries.isEmpty() || parsed.partitionIndex() != null;
    }
    SchemaSummary schema = toSchemaSummary(schemaNode);
    TopicHealth health = deriveTopicHealth(stats, subscriptions);
    String notes = buildTopicNotes(schema, statsNode, subscriptions);

    return new TopicDetails(
        parsed.canonicalFullName(),
        tenant,
        namespace,
        parsed.canonicalTopic(),
        partitioned,
        partitioned ? Math.max(Math.max(1, partitionCount), partitionSummaries.size()) : 0,
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

  private List<String> sanitizeStringList(List<String> values) {
    if (values == null) {
      return List.of();
    }

    return values.stream()
        .filter(value -> value != null && !value.isBlank())
        .map(String::trim)
        .distinct()
        .sorted()
        .toList();
  }

  private JsonNode safeGetJson(EnvironmentDetails environment, String path) {
    try {
      return getJson(environment, path);
    } catch (IOException | RestClientException exception) {
      return objectMapper.createObjectNode();
    }
  }

  private void postWithoutBody(EnvironmentDetails environment, String path) {
    postWithoutBody(environment, path, new String[0]);
  }

  private void postWithoutBody(EnvironmentDetails environment, String path, String... fallbackPaths) {
    RestClientException firstFailure = null;
    for (String candidatePath : candidatePaths(path, fallbackPaths)) {
      try {
        var request = restClient.post()
            .uri(normalizeAdminUrl(environment.adminUrl()) + candidatePath);

        applyAuthHeaders(request, environment);

        request.retrieve().toBodilessEntity();
        return;
      } catch (RestClientException exception) {
        if (firstFailure == null) {
          firstFailure = exception;
        }
      }
    }

    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private JsonNode postForJson(EnvironmentDetails environment, String path, String body) throws IOException {
    var request = restClient.post()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path)
        .contentType(org.springframework.http.MediaType.APPLICATION_JSON);

    applyAuthHeaders(request, environment);

    String rawBody = (body == null ? request : request.body(body)).retrieve().body(String.class);
    if (rawBody == null || rawBody.isBlank()) {
      return objectMapper.nullNode();
    }
    return objectMapper.readTree(rawBody);
  }

  private void postJson(EnvironmentDetails environment, String path, String body) {
    var request = restClient.post()
        .uri(normalizeAdminUrl(environment.adminUrl()) + path)
        .contentType(org.springframework.http.MediaType.APPLICATION_JSON);

    applyAuthHeaders(request, environment);

    request.body(body).retrieve().toBodilessEntity();
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

  private List<PlatformArtifactSummary> readPlatformArtifacts(
      EnvironmentDetails environment,
      String path,
      String tenant,
      String namespace,
      String details) {
    JsonNode node = safeGetJson(environment, path);
    if (!node.isArray()) {
      return List.of();
    }

    List<PlatformArtifactSummary> values = new ArrayList<>();
    for (JsonNode item : node) {
      String name = item.isTextual() ? item.asText() : item.path("name").asText("");
      if (!name.isBlank()) {
        values.add(new PlatformArtifactSummary(name, tenant, namespace, "CONFIGURED", details));
      }
    }
    return values;
  }

  private PlatformArtifactDetails toPlatformArtifactDetails(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name,
      JsonNode config,
      JsonNode status) {
    String runningStatus = status.path("running").asBoolean(false) ? "RUNNING" : status.path("instances").size() > 0 ? "DEPLOYED" : "CONFIGURED";
    String details = status.path("error").asText(
        config.path("tenant").isMissingNode()
            ? "Artifact details loaded from Pulsar admin REST."
            : "Artifact configuration loaded from Pulsar admin REST.");
    return new PlatformArtifactDetails(
        environment.id(),
        artifactType,
        name,
        tenant,
        namespace,
        runningStatus,
        details,
        config.path("archive").asText(null),
        config.path("className").asText(null),
        firstText(config, "inputSpecsTopic", "inputs", "sourceTopic"),
        firstText(config, "output", "outputTopic", "sinkTopic"),
        config.path("parallelism").isNumber() ? config.path("parallelism").asInt() : null,
        stringifyNode(config),
        !"CONNECTOR".equals(artifactType));
  }

  private List<PlatformArtifactSummary> readConnectorArtifacts(EnvironmentDetails environment) {
    JsonNode node = safeGetJson(environment, "/admin/v3/functions/connectors");
    if (!node.isArray()) {
      return List.of();
    }

    List<PlatformArtifactSummary> values = new ArrayList<>();
    for (JsonNode item : node) {
      String name = item.path("name").asText("");
      if (name.isBlank() && item.isTextual()) {
        name = item.asText("");
      }
      if (!name.isBlank()) {
        values.add(new PlatformArtifactSummary(
            name,
            null,
            null,
            "AVAILABLE",
            "Connector plugin is registered in the Pulsar cluster catalog."));
      }
    }
    return values;
  }

  private List<String> canonicalTopicNames(List<String> fullTopicNames) {
    LinkedHashSet<String> canonicalNames = new LinkedHashSet<>();
    for (String fullTopicName : fullTopicNames) {
      canonicalNames.add(PulsarTopicName.parse(fullTopicName).canonicalFullName());
    }
    return new ArrayList<>(canonicalNames);
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

  private String partitionedStatsPath(PulsarTopicName topicName) {
    return "/admin/v2/" + topicName.domain()
        + "/" + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.canonicalTopic()
        + "/partitioned-stats?perPartition=true";
  }

  private String partitionedMetadataPath(PulsarTopicName topicName) {
    return "/admin/v2/" + topicName.domain()
        + "/" + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.canonicalTopic()
        + "/partitions";
  }

  private String schemaPath(PulsarTopicName topicName) {
    return "/admin/v2/schemas/"
        + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.topic()
        + "/schema";
  }

  private String platformArtifactBasePath(
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    return platformArtifactCollectionPath(artifactType, tenant, namespace) + "/" + name;
  }

  private String platformArtifactCollectionPath(
      String artifactType,
      String tenant,
      String namespace) {
    String segment = switch (normalizeArtifactType(artifactType)) {
      case "FUNCTION" -> "functions";
      case "SOURCE" -> "source";
      case "SINK" -> "sink";
      default -> throw new BadRequestException("Unsupported platform artifact type: " + artifactType);
    };
    return "/admin/v3/" + segment + "/" + tenant + "/" + namespace;
  }

  private void requireArtifactScope(String tenant, String namespace, String name) {
    if (tenant == null || tenant.isBlank() || namespace == null || namespace.isBlank() || name == null || name.isBlank()) {
      throw new BadRequestException("Platform artifact tenant, namespace, and name are required.");
    }
  }

  private String normalizeArtifactType(String artifactType) {
    String normalized = artifactType == null ? "" : artifactType.trim().toUpperCase();
    return switch (normalized) {
      case "FUNCTION", "FUNCTIONS" -> "FUNCTION";
      case "SOURCE", "SOURCES" -> "SOURCE";
      case "SINK", "SINKS" -> "SINK";
      case "CONNECTOR", "CONNECTORS" -> "CONNECTOR";
      default -> throw new BadRequestException("Unsupported platform artifact type: " + artifactType);
    };
  }

  private String buildPlatformArtifactBody(String artifactType, PlatformArtifactMutationRequest request) {
    try {
      return objectMapper.writeValueAsString(Map.of(
          "tenant", request.tenant(),
          "namespace", request.namespace(),
          "name", request.name(),
          "className", request.className() == null ? "" : request.className(),
          "archive", request.archive() == null ? "" : request.archive(),
          "parallelism", request.parallelism() == null ? 1 : request.parallelism(),
          "inputTopic", request.inputTopic() == null ? "" : request.inputTopic(),
          "outputTopic", request.outputTopic() == null ? "" : request.outputTopic(),
          "configs", parseConfigs(request.configs()),
          "artifactType", normalizeArtifactType(artifactType)));
    } catch (IOException exception) {
      throw new BadRequestException("Unable to serialize platform artifact configuration: " + exception.getMessage());
    }
  }

  private Object parseConfigs(String configs) throws IOException {
    if (configs == null || configs.isBlank()) {
      return Map.of();
    }
    JsonNode node = objectMapper.readTree(configs);
    if (node.isObject()) {
      return objectMapper.convertValue(node, Map.class);
    }
    return Map.of("raw", configs.trim());
  }

  private SchemaDetails toSchemaDetails(
      String environmentId,
      String topicName,
      JsonNode schemaNode,
      String compatibilityOverride) {
    SchemaSummary summary = toSchemaSummary(schemaNode);
    JsonNode dataNode = schemaNode.path("data");
    String definition = dataNode.isTextual()
        ? dataNode.asText("")
        : dataNode.path("schema").asText(
            schemaNode.path("schema").asText(stringifyNode(dataNode)));
    return new SchemaDetails(
        environmentId,
        topicName,
        summary.present(),
        summary.type(),
        summary.version(),
        compatibilityOverride == null || compatibilityOverride.isBlank()
            ? summary.compatibility()
            : compatibilityOverride,
        definition == null ? "" : definition,
        true,
        summary.present()
            ? "Schema definition loaded from Pulsar admin REST."
            : "No schema definition is currently registered for this topic.");
  }

  private String stringifyNode(JsonNode node) {
    try {
      if (node == null || node.isMissingNode() || node.isNull()) {
        return "";
      }
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
    } catch (IOException exception) {
      return "";
    }
  }

  private String firstText(JsonNode node, String... fields) {
    for (String field : fields) {
      JsonNode value = node.path(field);
      if (value.isTextual() && !value.asText().isBlank()) {
        return value.asText();
      }
    }
    return null;
  }

  private NamespacePolicies getNamespacePolicies(EnvironmentDetails environment, String tenant, String namespace) {
    String base = "/admin/v2/namespaces/" + tenant + "/" + namespace;
    JsonNode retention = safeGetJson(environment, base + "/retention");
    JsonNode backlogQuota = safeGetJson(environment, base + "/backlogQuota");
    JsonNode dispatchRate = safeGetJson(environment, base + "/dispatchRate");
    JsonNode publishRate = safeGetJson(environment, base + "/publishRate");

    return new NamespacePolicies(
        readRetentionMinutes(retention),
        readRetentionSizeMb(retention),
        safeGetJson(environment, base + "/messageTTL").asInt(0),
        safeGetJson(environment, base + "/deduplication").asBoolean(false),
        backlogQuota.path("limitSize").asLong(0),
        backlogQuota.path("limitTime").asInt(0),
        dispatchRate.path("dispatchThrottlingRateInMsg").asInt(0),
        dispatchRate.path("dispatchThrottlingRateInByte").asLong(0),
        publishRate.path("publishThrottlingRateInMsg").asInt(0),
        publishRate.path("publishThrottlingRateInByte").asLong(0));
  }

  private int readPartitionCount(JsonNode metadataNode) {
    if (metadataNode == null || metadataNode.isMissingNode()) {
      return 0;
    }

    if (metadataNode.isNumber()) {
      return metadataNode.asInt(0);
    }

    return metadataNode.path("partitions").asInt(0);
  }

  private int readRetentionMinutes(JsonNode retention) {
    return retention.path("retentionTimeInMinutes").asInt(
        retention.isArray() && retention.size() > 0 ? retention.get(0).asInt(0) : 0);
  }

  private int readRetentionSizeMb(JsonNode retention) {
    return retention.path("retentionSizeInMB").asInt(
        retention.path("retentionSizeInMb").asInt(
            retention.isArray() && retention.size() > 1 ? retention.get(1).asInt(0) : 0));
  }

  private int coalesceInt(Integer value) {
    return value == null ? 0 : value;
  }

  private long coalesceLong(Long value) {
    return value == null ? 0L : value;
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
      PulsarTopicName partitionName = PulsarTopicName.parse(entry.getKey());
      if (partitionName.partitionIndex() == null) {
        return;
      }
      JsonNode partitionNode = entry.getValue();
      long backlog = sumPartitionBacklog(partitionNode.path("subscriptions"));
      int consumers = sumPartitionConsumers(partitionNode.path("subscriptions"));
      partitionSummaries.add(new TopicPartitionSummary(
          partitionName.fullName(),
          backlog,
          consumers,
          partitionNode.path("msgRateIn").asDouble(0),
          partitionNode.path("msgRateOut").asDouble(0),
          derivePartitionHealth(backlog, consumers)));
    });

    partitionSummaries.sort(Comparator.comparing(TopicPartitionSummary::partitionName));
    return partitionSummaries;
  }

  private List<TopicPartitionSummary> completePartitionSummaries(
      String canonicalFullTopicName,
      List<TopicPartitionSummary> existingSummaries,
      int partitionCount) {
    if (partitionCount <= 0) {
      return existingSummaries;
    }

    Map<String, TopicPartitionSummary> byName = new java.util.LinkedHashMap<>();
    for (TopicPartitionSummary summary : existingSummaries) {
      byName.put(summary.partitionName(), summary);
    }

    for (int index = 0; index < partitionCount; index++) {
      String partitionName = canonicalFullTopicName + "-partition-" + index;
      byName.putIfAbsent(
          partitionName,
          new TopicPartitionSummary(
              partitionName,
              0,
              0,
              0,
              0,
              TopicHealth.INACTIVE));
    }

    return byName.values().stream()
        .sorted(Comparator.comparing(TopicPartitionSummary::partitionName))
        .toList();
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
            + "/skip_all/skipAllMessages",
        "/admin/v2/" + topicName.domain()
            + "/" + topicName.tenant()
            + "/" + topicName.namespace()
            + "/" + topicName.topic()
            + "/subscription/" + subscriptionName
            + "/skip_all");

    return new ResetCursorResponse(
        environment.id(),
        topicName.fullName(),
        subscriptionName,
        "LATEST",
        null,
        "Cursor moved to the latest position by clearing backlog via Pulsar admin REST API for subscription "
            + subscriptionName + ".");
  }

  private List<String> resolveAllowedClusters(EnvironmentDetails environment, List<String> requestedClusters) {
    List<String> allowedClusters = sanitizeStringList(requestedClusters);
    if (!allowedClusters.isEmpty()) {
      return allowedClusters;
    }

    try {
      List<String> discoveredClusters = sanitizeStringList(readStringArray(getJson(environment, "/admin/v2/clusters")));
      if (discoveredClusters.isEmpty()) {
        throw new BadRequestException(
            "Unable to discover any Pulsar clusters from /admin/v2/clusters. Specify Allowed Clusters explicitly or verify the admin endpoint.");
      }
      return discoveredClusters;
    } catch (IOException | RestClientException exception) {
      throw new BadRequestException(
          "Unable to discover Pulsar clusters via /admin/v2/clusters: " + exception.getMessage());
    }
  }

  private String readSchemaCompatibility(EnvironmentDetails environment, PulsarTopicName topicName) {
    JsonNode compatibilityNode = safeGetJson(environment, schemaCompatibilityPath(topicName));
    if (compatibilityNode.isTextual() && !compatibilityNode.asText().isBlank()) {
      return compatibilityNode.asText();
    }
    String compatibility = firstText(compatibilityNode, "compatibilityStrategy", "compatibility");
    return compatibility == null || compatibility.isBlank() ? null : compatibility;
  }

  private String schemaCompatibilityPath(PulsarTopicName topicName) {
    return "/admin/v2/" + topicName.domain()
        + "/" + topicName.tenant()
        + "/" + topicName.namespace()
        + "/" + topicName.topic()
        + "/schemaCompatibilityStrategy";
  }

  private List<String> candidatePaths(String primaryPath, String... fallbackPaths) {
    List<String> paths = new ArrayList<>();
    paths.add(primaryPath);
    for (String fallbackPath : fallbackPaths) {
      if (fallbackPath != null && !fallbackPath.isBlank()) {
        paths.add(fallbackPath);
      }
    }
    return paths;
  }

  @FunctionalInterface
  interface PulsarClientFactory {
    PulsarClient create(EnvironmentDetails environment) throws PulsarClientException;
  }
}
