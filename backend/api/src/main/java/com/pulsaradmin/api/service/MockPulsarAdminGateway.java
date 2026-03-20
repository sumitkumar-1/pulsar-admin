package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.PeekMessage;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MockPulsarAdminGateway implements PulsarAdminGateway {
  private final ConcurrentMap<String, List<TopicDetails>> createdTopicsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<String>> createdTenantsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<String>> createdNamespacesByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, List<String>>> subscriptionsByEnvironment =
      new ConcurrentHashMap<>();

  @Override
  public EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment) {
    boolean validBroker = environment.brokerUrl().startsWith("pulsar://") || environment.brokerUrl().startsWith("pulsar+ssl://");
    boolean validAdmin = environment.adminUrl().startsWith("http://") || environment.adminUrl().startsWith("https://");
    boolean flaggedFailure = environment.brokerUrl().contains("invalid")
        || environment.adminUrl().contains("invalid")
        || environment.brokerUrl().contains("fail")
        || environment.adminUrl().contains("fail");

    boolean successful = validBroker && validAdmin && !flaggedFailure;

    return new EnvironmentConnectionTestResult(
        environment.id(),
        successful,
        successful ? "SUCCESS" : "FAILED",
        successful ? "Connection verified. Metadata sync was triggered." : "Unable to validate the broker or admin endpoint details.",
        Instant.now(),
        successful);
  }

  @Override
  public EnvironmentSnapshot syncMetadata(EnvironmentDetails environment) {
    Map<String, List<TopicDetails>> topicsByEnvironment = seedTopics();
    List<TopicDetails> topics = new ArrayList<>(topicsByEnvironment.getOrDefault(environment.id(), List.of()));
    topics.addAll(createdTopicsByEnvironment.getOrDefault(environment.id(), List.of()));

    if (topics.isEmpty()) {
      topics = createCustomEnvironmentTopics(environment);
    }

    topics = topics.stream()
        .map(topic -> applySubscriptionOverrides(environment.id(), topic))
        .collect(Collectors.toList());

    LinkedHashSet<String> tenants = new LinkedHashSet<>(topics.stream().map(TopicDetails::tenant).toList());
    tenants.addAll(createdTenantsByEnvironment.getOrDefault(environment.id(), List.of()));

    LinkedHashSet<String> namespaces = new LinkedHashSet<>(topics.stream()
        .map(topic -> topic.tenant() + "/" + topic.namespace())
        .toList());
    namespaces.addAll(createdNamespacesByEnvironment.getOrDefault(environment.id(), List.of()));

    EnvironmentHealth health = new EnvironmentHealth(
        environment.id(),
        deriveStatus(environment, topics),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.kind().equals("prod") ? "4.0.2" : "4.0.1",
        topics.isEmpty() ? "Connected, but no topic metadata is available yet." : "Metadata sync completed successfully.");

    return new EnvironmentSnapshot(health, new ArrayList<>(tenants), new ArrayList<>(namespaces), topics);
  }

  @Override
  public void createTopic(EnvironmentDetails environment, CreateTopicRequest request) {
    String fullTopicName = request.fullTopicName();
    TopicDetails createdTopic = request.partitions() > 0
        ? createPartitionedTopic(
            fullTopicName,
            request.partitions(),
            TopicHealth.INACTIVE,
            new TopicStatsSummary(0, 0, 0, 0, 0, 0, 0, 0, 0),
            new SchemaSummary("NONE", "-", "N/A", false),
            "Unassigned",
            request.notes() == null || request.notes().isBlank()
                ? "Topic created from the admin console."
                : request.notes().trim())
        : createTopic(
            fullTopicName,
            false,
            0,
            TopicHealth.INACTIVE,
            new TopicStatsSummary(0, 0, 0, 0, 0, 0, 0, 0, 0),
            new SchemaSummary("NONE", "-", "N/A", false),
            "Unassigned",
            request.notes() == null || request.notes().isBlank()
                ? "Topic created from the admin console."
                : request.notes().trim(),
            List.of());

    createdTopicsByEnvironment.compute(environment.id(), (key, existing) -> {
      List<TopicDetails> topics = new ArrayList<>(existing == null ? List.of() : existing);
      boolean alreadyExists = topics.stream().anyMatch(topic -> topic.fullName().equals(fullTopicName));
      if (alreadyExists) {
        throw new BadRequestException("Topic already exists: " + fullTopicName);
      }
      topics.add(createdTopic);
      return topics;
    });
  }

  @Override
  public void createTenant(EnvironmentDetails environment, CreateTenantRequest request) {
    String tenantName = request.tenant().trim();
    if (syncMetadata(environment).tenants().stream().anyMatch(existing -> existing.equalsIgnoreCase(tenantName))) {
      throw new BadRequestException("Tenant already exists: " + tenantName);
    }

    createdTenantsByEnvironment.compute(environment.id(), (key, existing) -> {
      List<String> tenants = new ArrayList<>(existing == null ? List.of() : existing);
      tenants.add(tenantName);
      tenants.sort(String::compareTo);
      return tenants;
    });
  }

  @Override
  public void createNamespace(EnvironmentDetails environment, CreateNamespaceRequest request) {
    String fullNamespace = request.tenant().trim() + "/" + request.namespace().trim();
    EnvironmentSnapshot snapshot = syncMetadata(environment);

    if (snapshot.tenants().stream().noneMatch(existing -> existing.equalsIgnoreCase(request.tenant().trim()))) {
      throw new BadRequestException("Unknown tenant: " + request.tenant().trim());
    }

    if (snapshot.namespaces().stream().anyMatch(existing -> existing.equalsIgnoreCase(fullNamespace))) {
      throw new BadRequestException("Namespace already exists: " + fullNamespace);
    }

    createdNamespacesByEnvironment.compute(environment.id(), (key, existing) -> {
      List<String> namespaces = new ArrayList<>(existing == null ? List.of() : existing);
      namespaces.add(fullNamespace);
      namespaces.sort(String::compareTo);
      return namespaces;
    });
  }

  @Override
  public void createSubscription(EnvironmentDetails environment, CreateSubscriptionRequest request) {
    TopicDetails topic = findTopic(environment, request.topicName());
    String subscriptionName = request.subscriptionName().trim();

    subscriptionsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .compute(request.topicName(), (topicName, existing) -> {
          List<String> subscriptions = new ArrayList<>(existing == null ? topic.subscriptions() : existing);
          if (subscriptions.stream().anyMatch(subscription -> subscription.equals(subscriptionName))) {
            throw new BadRequestException("Subscription already exists: " + subscriptionName);
          }
          subscriptions.add(subscriptionName);
          subscriptions.sort(String::compareTo);
          return subscriptions;
        });
  }

  @Override
  public void deleteSubscription(EnvironmentDetails environment, String topicName, String subscriptionName) {
    TopicDetails topic = findTopic(environment, topicName);
    ConcurrentMap<String, List<String>> topicSubscriptions = subscriptionsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>());

    topicSubscriptions.compute(topicName, (currentTopicName, existing) -> {
      List<String> subscriptions = new ArrayList<>(existing == null ? topic.subscriptions() : existing);
      boolean removed = subscriptions.removeIf(subscription -> subscription.equals(subscriptionName));
      if (!removed) {
        throw new BadRequestException("Subscription does not exist: " + subscriptionName);
      }
      return subscriptions;
    });
  }

  @Override
  public PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit) {
    List<PeekMessage> seededMessages = switch (topicName) {
      case "persistent://acme/orders/payment-events" -> List.of(
          new PeekMessage(
              "ledger:91:2048",
              "payment-10412",
              "2026-03-17T17:18:42Z",
              "2026-03-17T17:18:41Z",
              "payments-producer-2",
              "Payment authorized but settlement consumer is lagging behind the live stream.",
              """
              {
                "paymentId": "10412",
                "orderId": "A-10412",
                "state": "AUTHORIZED",
                "amount": 149.95,
                "currency": "USD",
                "riskBand": "review"
              }
              """.strip(),
              "9"),
          new PeekMessage(
              "ledger:91:2049",
              "payment-10413",
              "2026-03-17T17:19:11Z",
              "2026-03-17T17:19:10Z",
              "payments-producer-2",
              "Settlement retry message carrying the retry counter and downstream routing headers.",
              """
              {
                "paymentId": "10413",
                "orderId": "A-10413",
                "state": "SETTLEMENT_PENDING",
                "retryCount": 3,
                "targetProcessor": "settlement-west",
                "lastFailure": "timeout"
              }
              """.strip(),
              "9"),
          new PeekMessage(
              "ledger:91:2050",
              "payment-10414",
              "2026-03-17T17:20:03Z",
              "2026-03-17T17:20:02Z",
              "payments-producer-1",
              "Compensation event emitted after a duplicate settlement callback was detected.",
              """
              {
                "paymentId": "10414",
                "orderId": "A-10414",
                "state": "COMPENSATE",
                "reason": "duplicate-callback",
                "source": "gateway-primary"
              }
              """.strip(),
              "9"));
      case "persistent://acme/orders/order-events" -> List.of(
          new PeekMessage(
              "ledger:33:1201",
              "order-10412",
              "2026-03-17T17:15:10Z",
              "2026-03-17T17:15:09Z",
              "orders-producer-1",
              "Fresh order created event with fulfillment and priority metadata.",
              """
              {
                "orderId": "10412",
                "eventType": "ORDER_CREATED",
                "priority": "high",
                "region": "us-east-1",
                "customerTier": "gold"
              }
              """.strip(),
              "18"),
          new PeekMessage(
              "ledger:33:1202",
              "order-10413",
              "2026-03-17T17:15:44Z",
              "2026-03-17T17:15:43Z",
              "orders-producer-1",
              "Validation completed event emitted after inventory and payment checks passed.",
              """
              {
                "orderId": "10413",
                "eventType": "ORDER_VALIDATED",
                "inventoryReserved": true,
                "paymentReady": true
              }
              """.strip(),
              "18"));
      default -> List.of(
          new PeekMessage(
              "ledger:14:2810",
              environment.id() + "-sample-1",
              "2026-03-17T17:10:00Z",
              "2026-03-17T17:09:59Z",
              "mock-producer-1",
              "Representative sample message generated from the current topic snapshot.",
              """
              {
                "topic": "sample",
                "environment": "mock",
                "state": "preview"
              }
              """.strip(),
              "1"),
          new PeekMessage(
              "ledger:14:2811",
              environment.id() + "-sample-2",
              "2026-03-17T17:10:45Z",
              "2026-03-17T17:10:44Z",
              "mock-producer-1",
              "Second sample message to show key metadata and payload readability.",
              """
              {
                "topic": "sample",
                "environment": "mock",
                "state": "preview-2"
              }
              """.strip(),
              "1"));
    };

    List<PeekMessage> messages = seededMessages.stream().limit(limit).toList();
    return new PeekMessagesResponse(environment.id(), topicName, limit, messages.size(), seededMessages.size() > limit, messages);
  }

  @Override
  public ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request) {
    String normalizedTarget = request.target().trim().toUpperCase();
    String effectiveTimestamp = null;

    if (normalizedTarget.equals("TIMESTAMP")) {
      try {
        effectiveTimestamp = OffsetDateTime.parse(request.timestamp()).toInstant().toString();
      } catch (DateTimeParseException exception) {
        throw new BadRequestException("Timestamp must use ISO-8601 format.");
      }
    }

    String message = switch (normalizedTarget) {
      case "EARLIEST" -> "Cursor reset to the earliest available position for subscription "
          + request.subscriptionName() + ".";
      case "LATEST" -> "Cursor reset to the latest position for subscription "
          + request.subscriptionName() + ".";
      default -> "Cursor reset to messages published after " + effectiveTimestamp
          + " for subscription " + request.subscriptionName() + ".";
    };

    return new ResetCursorResponse(
        environment.id(),
        request.topicName(),
        request.subscriptionName(),
        normalizedTarget,
        effectiveTimestamp,
        message);
  }

  @Override
  public SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request) {
    int skippedMessages = request.messageCount();
    String message = "Skipped " + skippedMessages + " messages for subscription "
        + request.subscriptionName() + ".";

    return new SkipMessagesResponse(
        environment.id(),
        request.topicName(),
        request.subscriptionName(),
        skippedMessages,
        message);
  }

  @Override
  public UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request) {
    TopicDetails topic = findTopic(environment, request.topicName());

    return new UnloadTopicResponse(
        environment.id(),
        request.topicName(),
        "Unloaded topic " + request.topicName()
            + ". Brokers can now rebalance ownership and refresh the live serving path.",
        topic);
  }

  private EnvironmentStatus deriveStatus(EnvironmentDetails environment, List<TopicDetails> topics) {
    if (topics.stream().anyMatch(topic -> topic.health() == TopicHealth.CRITICAL)) {
      return EnvironmentStatus.DEGRADED;
    }

    if ("prod".equalsIgnoreCase(environment.kind()) || "stable".equalsIgnoreCase(environment.kind())) {
      return EnvironmentStatus.HEALTHY;
    }

    return EnvironmentStatus.HEALTHY;
  }

  private Map<String, List<TopicDetails>> seedTopics() {
    Map<String, List<TopicDetails>> environments = new LinkedHashMap<>();
    environments.put("prod", List.of(
        createTopic(
            "persistent://acme/orders/order-events",
            false,
            0,
            TopicHealth.HEALTHY,
            new TopicStatsSummary(124, 8, 3, 12, 840.2, 835.4, 14_200, 13_900, 2_145_328),
            new SchemaSummary("AVRO", "18", "FULL", true),
            "Core Orders",
            "Healthy live traffic with steady subscriber throughput.",
            List.of("order-fulfillment", "order-analytics", "order-audit")),
        createTopic(
            "persistent://acme/orders/payment-events",
            false,
            0,
            TopicHealth.CRITICAL,
            new TopicStatsSummary(18_720, 5, 2, 3, 190.5, 48.3, 6_200, 2_100, 5_880_120),
            new SchemaSummary("JSON", "9", "BACKWARD", true),
            "Payments",
            "Backlog-heavy topic with slow consumer dispatch and high oldest-message age.",
            List.of("payment-settlement", "payment-alerts")),
        createPartitionedTopic(
            "persistent://acme/analytics/usage-rollups",
            6,
            TopicHealth.ATTENTION,
            new TopicStatsSummary(3_220, 2, 2, 6, 420.0, 310.8, 10_450, 7_930, 3_214_920),
            new SchemaSummary("AVRO", "7", "FULL", true),
            "Analytics",
            "Partitioned topic showing mild partition skew after peak usage."),
        createTopic(
            "persistent://acme/platform/schema-registry-sync",
            false,
            0,
            TopicHealth.HEALTHY,
            new TopicStatsSummary(0, 1, 1, 1, 12.4, 12.4, 150, 150, 48_120),
            new SchemaSummary("PROTOBUF", "4", "FORWARD", true),
            "Platform",
            "Schema-aware system topic for registry replication.",
            List.of("schema-replicator")),
        createTopic(
            "persistent://acme/support/incident-quarantine",
            false,
            0,
            TopicHealth.INACTIVE,
            new TopicStatsSummary(0, 0, 1, 0, 0.0, 0.0, 0, 0, 4_096),
            new SchemaSummary("NONE", "-", "N/A", false),
            "Support",
            "Quarantine topic standing by for incident response.",
            List.of("incident-review"))));

    environments.put("stable", environments.get("prod"));

    environments.put("qa", List.of(
        createTopic(
            "persistent://acme/qa/regression-run-events",
            false,
            0,
            TopicHealth.ATTENTION,
            new TopicStatsSummary(1_280, 2, 2, 2, 88.0, 52.3, 2_240, 1_390, 1_120_000),
            new SchemaSummary("JSON", "5", "BACKWARD", true),
            "Quality Engineering",
            "Regression events occasionally back up during release rehearsals.",
            List.of("regression-tracker", "regression-alerts")),
        createTopic(
            "persistent://acme/qa/api-contract-smoke",
            false,
            0,
            TopicHealth.HEALTHY,
            new TopicStatsSummary(12, 1, 1, 1, 6.0, 6.0, 120, 120, 120_000),
            new SchemaSummary("AVRO", "2", "FULL", true),
            "Quality Engineering",
            "Lightweight smoke checks for service contracts.",
            List.of("contract-checker"))));

    environments.put("dev", List.of(
        createTopic(
            "persistent://acme/dev/sandbox-smoke",
            false,
            0,
            TopicHealth.HEALTHY,
            new TopicStatsSummary(3, 1, 1, 1, 2.1, 2.0, 42, 40, 12_480),
            new SchemaSummary("JSON", "1", "NONE", true),
            "Developer Productivity",
            "Tiny sandbox topic for smoke tests and demos.",
            List.of("sandbox-consumer")),
        createTopic(
            "persistent://acme/dev/replay-lab",
            false,
            0,
            TopicHealth.INACTIVE,
            new TopicStatsSummary(0, 0, 1, 0, 0.0, 0.0, 0, 0, 2_048),
            new SchemaSummary("NONE", "-", "N/A", false),
            "Developer Productivity",
            "Safe destination topic for future replay and copy workflows.",
            List.of("replay-lab-review"))));

    return environments;
  }

  private List<TopicDetails> createCustomEnvironmentTopics(EnvironmentDetails environment) {
    String tenant = "custom";
    String namespace = environment.id();

    return List.of(
        createTopic(
            "persistent://" + tenant + "/" + namespace + "/operations-sandbox",
            false,
            0,
            TopicHealth.HEALTHY,
            new TopicStatsSummary(18, 1, 1, 1, 7.2, 7.0, 210, 204, 24_000),
            new SchemaSummary("JSON", "1", "NONE", true),
            "Environment Owners",
            "Auto-generated sandbox topic for the newly added environment.",
            List.of("sandbox-review")),
        createTopic(
            "persistent://" + tenant + "/" + namespace + "/replay-lab",
            false,
            0,
            TopicHealth.INACTIVE,
            new TopicStatsSummary(0, 0, 1, 0, 0.0, 0.0, 0, 0, 2_048),
            new SchemaSummary("NONE", "-", "N/A", false),
            "Environment Owners",
            "Safe starter topic generated during the first metadata sync.",
            List.of("replay-review")));
  }

  private TopicDetails applySubscriptionOverrides(String environmentId, TopicDetails topic) {
    List<String> overriddenSubscriptions = subscriptionsByEnvironment
        .getOrDefault(environmentId, new ConcurrentHashMap<>())
        .get(topic.fullName());

    if (overriddenSubscriptions == null) {
      return topic;
    }

    TopicStatsSummary stats = new TopicStatsSummary(
        topic.stats().backlog(),
        topic.stats().producers(),
        overriddenSubscriptions.size(),
        topic.stats().consumers(),
        topic.stats().publishRateIn(),
        topic.stats().dispatchRateOut(),
        topic.stats().throughputIn(),
        topic.stats().throughputOut(),
        topic.stats().storageSize());

    TopicHealth health = overriddenSubscriptions.isEmpty() && topic.stats().backlog() == 0
        ? TopicHealth.INACTIVE
        : topic.health();

    return new TopicDetails(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        topic.partitioned(),
        topic.partitions(),
        health,
        stats,
        topic.schema(),
        topic.ownerTeam(),
        topic.notes(),
        topic.partitionSummaries(),
        overriddenSubscriptions);
  }

  private TopicDetails findTopic(EnvironmentDetails environment, String topicName) {
    List<TopicDetails> topics = syncMetadata(environment).topics();
    return topics.stream()
        .filter(topic -> topic.fullName().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new BadRequestException("Unknown topic: " + topicName));
  }

  private TopicDetails createTopic(
      String fullName,
      boolean partitioned,
      int partitions,
      TopicHealth health,
      TopicStatsSummary stats,
      SchemaSummary schema,
      String ownerTeam,
      String notes,
      List<String> subscriptions) {
    String[] segments = fullName.replace("persistent://", "").split("/");
    String tenant = segments[0];
    String namespace = segments[1];
    String topic = segments[2];

    return new TopicDetails(
        fullName,
        tenant,
        namespace,
        topic,
        partitioned,
        partitions,
        health,
        stats,
        schema,
        ownerTeam,
        notes,
        List.of(),
        subscriptions);
  }

  private TopicDetails createPartitionedTopic(
      String fullName,
      int partitions,
      TopicHealth health,
      TopicStatsSummary stats,
      SchemaSummary schema,
      String ownerTeam,
      String notes) {
    List<TopicPartitionSummary> partitionSummaries = new ArrayList<>();

    for (int index = 0; index < partitions; index++) {
      partitionSummaries.add(new TopicPartitionSummary(
          fullName + "-partition-" + index,
          240L + (index * 110L),
          index % 2 == 0 ? 1 : 2,
          52.0 + index * 8,
          44.0 + index * 5,
          index == 4 ? TopicHealth.CRITICAL : TopicHealth.ATTENTION));
    }

    TopicDetails base = createTopic(fullName, true, partitions, health, stats, schema, ownerTeam, notes, List.of("usage-aggregator", "warehouse-sync"));
    return new TopicDetails(
        base.fullName(),
        base.tenant(),
        base.namespace(),
        base.topic(),
        true,
        partitions,
        base.health(),
        base.stats(),
        base.schema(),
        base.ownerTeam(),
        base.notes(),
        partitionSummaries,
        base.subscriptions());
  }

}
