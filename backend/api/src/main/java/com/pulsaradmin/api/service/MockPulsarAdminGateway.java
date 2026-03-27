package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.ConsumedMessage;
import com.pulsaradmin.shared.model.ClearBacklogRequest;
import com.pulsaradmin.shared.model.ClearBacklogResponse;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.PeekMessage;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MockPulsarAdminGateway implements PulsarAdminGateway {
  private final ConcurrentMap<String, List<TopicDetails>> createdTopicsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, TopicDetails>> updatedTopicsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<String>> createdTenantsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<String>> createdNamespacesByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, List<String>>> subscriptionsByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, TopicPolicies>> topicPoliciesByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, NamespacePolicies>> namespacePoliciesByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, List<ConsumedMessage>>> publishedMessagesByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<String>> deletedTopicsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<String>> deletedNamespacesByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<String>> deletedTenantsByEnvironment = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, TenantDetails>> tenantDetailsByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, PlatformArtifactDetails>> platformArtifactsByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<String>> deletedPlatformArtifactsByEnvironment =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, SchemaDetails>> schemaByEnvironment =
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

    Set<String> deletedTopics = deletedTopicsByEnvironment.getOrDefault(environment.id(), Set.of());
    Set<String> deletedNamespaces = deletedNamespacesByEnvironment.getOrDefault(environment.id(), Set.of());
    Set<String> deletedTenants = deletedTenantsByEnvironment.getOrDefault(environment.id(), Set.of());
    topics = topics.stream()
        .filter(topic -> !deletedTopics.contains(topic.fullName()))
        .filter(topic -> !deletedNamespaces.contains(topic.tenant() + "/" + topic.namespace()))
        .filter(topic -> !deletedTenants.contains(topic.tenant()))
        .toList();

    Map<String, TopicDetails> topicMap = new LinkedHashMap<>();
    for (TopicDetails topic : topics) {
      topicMap.put(topic.fullName(), topic);
    }
    topicMap.putAll(updatedTopicsByEnvironment.getOrDefault(environment.id(), new ConcurrentHashMap<>()));
    topics = new ArrayList<>(topicMap.values());

    if (topics.isEmpty()) {
      topics = createCustomEnvironmentTopics(environment);
    }

    topics = topics.stream()
        .map(topic -> applySubscriptionOverrides(environment.id(), topic))
        .map(topic -> applySchemaOverrides(environment.id(), topic))
        .collect(Collectors.toList());

    LinkedHashSet<String> tenants = new LinkedHashSet<>(topics.stream().map(TopicDetails::tenant).toList());
    tenants.addAll(createdTenantsByEnvironment.getOrDefault(environment.id(), List.of()));
    tenants.removeIf(deletedTenants::contains);

    LinkedHashSet<String> namespaces = new LinkedHashSet<>(topics.stream()
        .map(topic -> topic.tenant() + "/" + topic.namespace())
        .toList());
    namespaces.addAll(createdNamespacesByEnvironment.getOrDefault(environment.id(), List.of()));
    namespaces.removeIf(namespace -> deletedNamespaces.contains(namespace) || deletedTenants.contains(namespace.split("/", 2)[0]));

    EnvironmentHealth health = new EnvironmentHealth(
        environment.id(),
        deriveStatus(environment, topics),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.kind().equals("prod") ? "4.0.2" : "4.0.1",
        topics.isEmpty() ? "Connected, but no topic metadata is available yet." : "Metadata sync completed successfully.");

    return new EnvironmentSnapshot(health, new ArrayList<>(tenants), new ArrayList<>(namespaces), topics, List.of());
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
  public void updateTopicPartitions(EnvironmentDetails environment, String topicName, int partitions) {
    TopicDetails existing = findTopic(environment, topicName);
    if (!existing.partitioned()) {
      throw new BadRequestException("Only existing partitioned topics can be resized.");
    }
    if (partitions < existing.partitions()) {
      throw new BadRequestException("Partition count cannot be decreased for an existing partitioned topic.");
    }
    if (partitions == existing.partitions()) {
      return;
    }

    TopicDetails updated = withPartitionCount(existing, partitions);
    updatedTopicsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(topicName, updated);
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
  public TenantDetails getTenantDetails(EnvironmentDetails environment, String tenant) {
    EnvironmentSnapshot snapshot = syncMetadata(environment);
    boolean exists = snapshot.tenants().stream().anyMatch(existing -> existing.equalsIgnoreCase(tenant));
    if (!exists) {
      throw new BadRequestException("Unknown tenant: " + tenant);
    }

    TopicListItemCounts counts = summarizeTenant(snapshot, tenant);
    return tenantDetailsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .computeIfAbsent(tenant, ignored -> defaultTenantDetails(environment, tenant, counts.namespaceCount(), counts.topicCount()));
  }

  @Override
  public TenantDetails updateTenant(EnvironmentDetails environment, TenantUpdateRequest request) {
    getTenantDetails(environment, request.tenant().trim());
    TopicListItemCounts counts = summarizeTenant(syncMetadata(environment), request.tenant().trim());
    TenantDetails updated = new TenantDetails(
        environment.id(),
        request.tenant().trim(),
        sanitizeStringList(request.adminRoles()),
        sanitizeStringList(request.allowedClusters()).isEmpty()
            ? List.of(environment.clusterLabel())
            : sanitizeStringList(request.allowedClusters()),
        counts.namespaceCount(),
        counts.topicCount(),
        Instant.now());
    tenantDetailsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(request.tenant().trim(), updated);
    return updated;
  }

  @Override
  public void deleteTenant(EnvironmentDetails environment, String tenant) {
    getTenantDetails(environment, tenant);
    deletedTenantsByEnvironment.computeIfAbsent(environment.id(), ignored -> new HashSet<>()).add(tenant);
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
    List<ConsumedMessage> published = publishedMessagesByEnvironment
        .getOrDefault(environment.id(), new ConcurrentHashMap<>())
        .getOrDefault(topicName, List.of());
    if (!published.isEmpty()) {
      List<PeekMessage> fromPublished = published.stream()
          .limit(limit)
          .map(message -> new PeekMessage(
              message.messageId(),
              message.key() == null ? "-" : message.key(),
              message.publishTime() == null ? Instant.now().toString() : message.publishTime().toString(),
              message.eventTime() == null ? Instant.now().toString() : message.eventTime().toString(),
              message.producerName(),
              summarizePayload(message.payload()),
              message.payload(),
              "mock-live"))
          .toList();
      return new PeekMessagesResponse(
          environment.id(),
          topicName,
          limit,
          fromPublished.size(),
          published.size() > limit,
          1,
          fromPublished.isEmpty()
              ? "No retained messages were found for " + topicName + "."
              : "Peeked " + fromPublished.size() + " messages for " + topicName + ".",
          fromPublished);
    }

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
    return new PeekMessagesResponse(
        environment.id(),
        topicName,
        limit,
        messages.size(),
        seededMessages.size() > limit,
        1,
        messages.isEmpty()
            ? "No retained messages were found for " + topicName + "."
            : "Peeked " + messages.size() + " messages for " + topicName + ".",
        messages);
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
  public TerminateTopicResponse terminateTopic(EnvironmentDetails environment, TerminateTopicRequest request) {
    TopicDetails topic = findTopic(environment, request.topicName());
    return new TerminateTopicResponse(
        environment.id(),
        request.topicName(),
        "mock-" + Math.abs(request.topicName().hashCode()),
        "Terminated topic " + request.topicName()
            + ". Producers can no longer append after the current retained tail.",
        topic);
  }

  @Override
  public TopicPolicies getTopicPolicies(EnvironmentDetails environment, String topicName) {
    findTopic(environment, topicName);
    return topicPoliciesByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .computeIfAbsent(topicName, ignored -> defaultTopicPolicies(topicName));
  }

  @Override
  public TopicPolicies updateTopicPolicies(EnvironmentDetails environment, String topicName, TopicPolicies policies) {
    findTopic(environment, topicName);
    topicPoliciesByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(topicName, policies);
    return policies;
  }

  @Override
  public NamespaceDetails getNamespaceDetails(EnvironmentDetails environment, String tenant, String namespace) {
    EnvironmentSnapshot snapshot = syncMetadata(environment);
    String fullNamespace = tenant + "/" + namespace;
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
        .toList();

    if (snapshot.namespaces().stream().noneMatch(item -> item.equals(fullNamespace))) {
      throw new BadRequestException("Unknown namespace: " + fullNamespace);
    }

    return new NamespaceDetails(
        environment.id(),
        tenant,
        namespace,
        topics.size(),
        topics,
        namespacePoliciesByEnvironment
            .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
            .computeIfAbsent(fullNamespace, ignored -> defaultNamespacePolicies(fullNamespace)),
        Instant.now(),
        "Loaded namespace details from the mock catalog.");
  }

  @Override
  public NamespacePolicies updateNamespacePolicies(
      EnvironmentDetails environment,
      String tenant,
      String namespace,
      NamespacePolicies policies) {
    getNamespaceDetails(environment, tenant, namespace);
    namespacePoliciesByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(tenant + "/" + namespace, policies);
    return policies;
  }

  @Override
  public TopicDetails getTopicDetails(EnvironmentDetails environment, String topicName) {
    return findTopic(environment, topicName);
  }

  @Override
  public PublishMessageResponse publishMessage(EnvironmentDetails environment, PublishMessageRequest request) {
    findTopic(environment, request.topicName());
    ConsumedMessage message = new ConsumedMessage(
        "mock:" + Math.abs((request.topicName() + request.payload() + Instant.now()).hashCode()),
        request.key(),
        Instant.now(),
        Instant.now(),
        request.properties() == null ? Map.of() : request.properties(),
        "mock-console",
        request.payload());

    publishedMessagesByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .compute(request.topicName(), (topicName, existing) -> {
          List<ConsumedMessage> messages = new ArrayList<>(existing == null ? List.of() : existing);
          messages.add(0, message);
          return messages.stream().limit(100).toList();
        });

    return new PublishMessageResponse(
        environment.id(),
        request.topicName(),
        message.messageId(),
        request.key(),
        request.properties() == null ? Map.of() : request.properties(),
        request.schemaMode() == null || request.schemaMode().isBlank() ? "RAW" : request.schemaMode(),
        Instant.now(),
        "Published a bounded test message to " + request.topicName() + ".",
        List.of());
  }

  @Override
  public ConsumeMessagesResponse consumeMessages(EnvironmentDetails environment, ConsumeMessagesRequest request) {
    findTopic(environment, request.topicName());
    List<ConsumedMessage> available = new ArrayList<>(publishedMessagesByEnvironment
        .getOrDefault(environment.id(), new ConcurrentHashMap<>())
        .getOrDefault(request.topicName(), List.of()));

    if (available.isEmpty()) {
      available = seededConsumedMessages(environment, request.topicName());
    }

    List<ConsumedMessage> messages = available.stream().limit(request.maxMessages()).toList();
    String subscriptionName = request.ephemeral()
        ? "ephemeral-preview"
        : request.subscriptionName();

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
            : "Consumed " + messages.size() + " messages from " + request.topicName() + ".",
        messages,
        List.of());
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
  public ClearBacklogResponse clearBacklog(EnvironmentDetails environment, ClearBacklogRequest request) {
    return new ClearBacklogResponse(
        environment.id(),
        request.topicName(),
        request.subscriptionName(),
        true,
        "Cleared backlog for subscription " + request.subscriptionName() + ".",
        Instant.now());
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

  @Override
  public void deleteTopic(EnvironmentDetails environment, String topicName) {
    findTopic(environment, topicName);
    deletedTopicsByEnvironment.computeIfAbsent(environment.id(), ignored -> new HashSet<>()).add(topicName);
  }

  @Override
  public void deleteNamespace(EnvironmentDetails environment, String tenant, String namespace) {
    String fullNamespace = tenant + "/" + namespace;
    EnvironmentSnapshot snapshot = syncMetadata(environment);
    boolean exists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    if (!exists) {
      throw new BadRequestException("Unknown namespace: " + fullNamespace);
    }
    deletedNamespacesByEnvironment.computeIfAbsent(environment.id(), ignored -> new HashSet<>()).add(fullNamespace);
  }

  @Override
  public PlatformSummary getPlatformSummary(EnvironmentDetails environment, List<String> namespaces) {
    List<PlatformArtifactDetails> artifacts = readPlatformArtifacts(environment.id());
    return new PlatformSummary(
        environment.id(),
        summarizePlatformArtifacts(artifacts, "FUNCTION"),
        summarizePlatformArtifacts(artifacts, "SOURCE"),
        summarizePlatformArtifacts(artifacts, "SINK"),
        summarizePlatformArtifacts(artifacts, "CONNECTOR"));
  }

  @Override
  public PlatformArtifactDetails getPlatformArtifactDetails(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    return readPlatformArtifacts(environment.id()).stream()
        .filter(item -> item.artifactType().equalsIgnoreCase(artifactType))
        .filter(item -> item.name().equals(name))
        .filter(item -> tenant == null || tenant.equals(item.tenant()))
        .filter(item -> namespace == null || namespace.equals(item.namespace()))
        .findFirst()
        .orElseThrow(() -> new BadRequestException("Unknown platform artifact: " + name));
  }

  @Override
  public PlatformArtifactDetails upsertPlatformArtifact(
      EnvironmentDetails environment,
      PlatformArtifactMutationRequest request) {
    PlatformArtifactDetails details = new PlatformArtifactDetails(
        environment.id(),
        request.artifactType().trim().toUpperCase(),
        request.name().trim(),
        trimToNull(request.tenant()),
        trimToNull(request.namespace()),
        "RUNNING",
        buildPlatformDetails(request),
        trimToNull(request.archive()),
        trimToNull(request.className()),
        trimToNull(request.inputTopic()),
        trimToNull(request.outputTopic()),
        request.parallelism() == null ? 1 : request.parallelism(),
        request.configs() == null ? "{}" : request.configs().trim(),
        true);
    platformArtifactsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(platformArtifactKey(details.artifactType(), details.tenant(), details.namespace(), details.name()), details);
    deletedPlatformArtifactsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> ConcurrentHashMap.newKeySet())
        .remove(platformArtifactKey(details.artifactType(), details.tenant(), details.namespace(), details.name()));
    return details;
  }

  @Override
  public void deletePlatformArtifact(
      EnvironmentDetails environment,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    String key = platformArtifactKey(artifactType, tenant, namespace, name);
    platformArtifactsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .remove(key);
    deletedPlatformArtifactsByEnvironment
        .computeIfAbsent(environment.id(), ignored -> ConcurrentHashMap.newKeySet())
        .add(key);
  }

  @Override
  public SchemaDetails getSchemaDetails(EnvironmentDetails environment, String topicName) {
    TopicDetails topic = findTopic(environment, topicName);
    SchemaDetails override = schemaByEnvironment
        .getOrDefault(environment.id(), new ConcurrentHashMap<>())
        .get(topic.fullName());
    if (override != null) {
      return override;
    }
    return new SchemaDetails(
        environment.id(),
        topic.fullName(),
        topic.schema().present(),
        topic.schema().type(),
        topic.schema().version(),
        topic.schema().compatibility(),
        defaultSchemaDefinition(topic.schema()),
        true,
        topic.schema().present()
            ? "Mock schema metadata is available for this topic."
            : "This topic does not currently expose schema metadata.");
  }

  @Override
  public SchemaDetails upsertSchema(EnvironmentDetails environment, SchemaUpdateRequest request) {
    TopicDetails topic = findTopic(environment, request.topicName());
    SchemaDetails details = new SchemaDetails(
        environment.id(),
        topic.fullName(),
        true,
        request.schemaType().trim().toUpperCase(),
        String.valueOf(Math.abs((request.definition() + request.reason()).hashCode())),
        trimToNull(request.compatibility()) == null ? "FULL" : request.compatibility().trim().toUpperCase(),
        request.definition().trim(),
        true,
        "Saved schema definition in mock mode.");
    schemaByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(topic.fullName(), details);
    return details;
  }

  @Override
  public void deleteSchema(EnvironmentDetails environment, String topicName) {
    TopicDetails topic = findTopic(environment, topicName);
    schemaByEnvironment
        .computeIfAbsent(environment.id(), ignored -> new ConcurrentHashMap<>())
        .put(
            topic.fullName(),
            new SchemaDetails(
                environment.id(),
                topic.fullName(),
                false,
                "NONE",
                "-",
                "N/A",
                "",
                true,
                "Schema removed in mock mode."));
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

  private TopicPolicies defaultTopicPolicies(String topicName) {
    if (topicName.contains("payment")) {
      return new TopicPolicies(10080, 2048, 86400, 104857600L, 16, 48, 24);
    }
    return new TopicPolicies(1440, 1024, 43200, 67108864L, 8, 24, 12);
  }

  private NamespacePolicies defaultNamespacePolicies(String namespace) {
    if (namespace.contains("orders")) {
      return new NamespacePolicies(10080, 4096, 86400, true, 2147483648L, 86400, 5000, 10485760L, 5000, 10485760L);
    }
    return new NamespacePolicies(1440, 1024, 43200, false, 536870912L, 3600, 1000, 1048576L, 1000, 1048576L);
  }

  private List<ConsumedMessage> seededConsumedMessages(EnvironmentDetails environment, String topicName) {
    return List.of(
        new ConsumedMessage(
            "mock-consume-" + Math.abs((environment.id() + topicName + "-1").hashCode()),
            "sample-1",
            Instant.now().minusSeconds(90),
            Instant.now().minusSeconds(90),
            Map.of("mode", "mock", "source", "seed"),
            "mock-consumer-seed",
            "{\"topic\":\"" + topicName + "\",\"environment\":\"" + environment.id() + "\",\"state\":\"seeded-1\"}"),
        new ConsumedMessage(
            "mock-consume-" + Math.abs((environment.id() + topicName + "-2").hashCode()),
            "sample-2",
            Instant.now().minusSeconds(30),
            Instant.now().minusSeconds(30),
            Map.of("mode", "mock", "source", "seed"),
            "mock-consumer-seed",
            "{\"topic\":\"" + topicName + "\",\"environment\":\"" + environment.id() + "\",\"state\":\"seeded-2\"}"));
  }

  private String summarizePayload(String payload) {
    if (payload == null || payload.isBlank()) {
      return "No payload preview available.";
    }
    String compact = payload.replaceAll("\\s+", " ").trim();
    return compact.length() <= 120 ? compact : compact.substring(0, 117) + "...";
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

  private TenantDetails defaultTenantDetails(
      EnvironmentDetails environment,
      String tenant,
      int namespaceCount,
      int topicCount) {
    return new TenantDetails(
        environment.id(),
        tenant,
        List.of("platform-admin"),
        List.of(environment.clusterLabel()),
        namespaceCount,
        topicCount,
        Instant.now());
  }

  private TopicListItemCounts summarizeTenant(EnvironmentSnapshot snapshot, String tenant) {
    int namespaceCount = (int) snapshot.namespaces().stream()
        .filter(namespace -> namespace.startsWith(tenant + "/"))
        .count();
    int topicCount = (int) snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(tenant))
        .count();
    return new TopicListItemCounts(namespaceCount, topicCount);
  }

  private record TopicListItemCounts(int namespaceCount, int topicCount) {
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

  private TopicDetails applySchemaOverrides(String environmentId, TopicDetails topic) {
    SchemaDetails details = schemaByEnvironment
        .getOrDefault(environmentId, new ConcurrentHashMap<>())
        .get(topic.fullName());
    if (details == null) {
      return topic;
    }

    return new TopicDetails(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        topic.partitioned(),
        topic.partitions(),
        topic.health(),
        topic.stats(),
        new SchemaSummary(details.type(), details.version(), details.compatibility(), details.present()),
        topic.ownerTeam(),
        topic.notes(),
        topic.partitionSummaries(),
        topic.subscriptions());
  }

  private TopicDetails findTopic(EnvironmentDetails environment, String topicName) {
    List<TopicDetails> topics = syncMetadata(environment).topics();
    return topics.stream()
        .filter(topic -> topic.fullName().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + topicName));
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

  private TopicDetails withPartitionCount(TopicDetails topic, int partitions) {
    if (!topic.partitioned()) {
      return topic;
    }

    List<TopicPartitionSummary> partitionSummaries = new ArrayList<>();
    for (int index = 0; index < partitions; index++) {
      if (index < topic.partitionSummaries().size()) {
        partitionSummaries.add(topic.partitionSummaries().get(index));
      } else {
        partitionSummaries.add(new TopicPartitionSummary(
            topic.fullName() + "-partition-" + index,
            0,
            0,
            0,
            0,
            TopicHealth.INACTIVE));
      }
    }

    return new TopicDetails(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        true,
        partitions,
        topic.health(),
        topic.stats(),
        topic.schema(),
        topic.ownerTeam(),
        topic.notes(),
        partitionSummaries,
        topic.subscriptions());
  }

  private List<PlatformArtifactDetails> readPlatformArtifacts(String environmentId) {
    Map<String, PlatformArtifactDetails> values = new LinkedHashMap<>();
    for (PlatformArtifactDetails item : seededPlatformArtifacts(environmentId)) {
      values.put(platformArtifactKey(item.artifactType(), item.tenant(), item.namespace(), item.name()), item);
    }
    values.putAll(platformArtifactsByEnvironment.getOrDefault(environmentId, new ConcurrentHashMap<>()));
    for (String deleted : deletedPlatformArtifactsByEnvironment.getOrDefault(environmentId, Set.of())) {
      values.remove(deleted);
    }
    return new ArrayList<>(values.values());
  }

  private List<PlatformArtifactSummary> summarizePlatformArtifacts(
      List<PlatformArtifactDetails> artifacts,
      String artifactType) {
    return artifacts.stream()
        .filter(item -> artifactType.equals(item.artifactType()))
        .map(item -> new PlatformArtifactSummary(
            item.name(),
            item.tenant(),
            item.namespace(),
            item.status(),
            item.details()))
        .toList();
  }

  private List<PlatformArtifactDetails> seededPlatformArtifacts(String environmentId) {
    return List.of(
        new PlatformArtifactDetails(environmentId, "FUNCTION", "invoice-normalizer", "acme", "orders",
            "RUNNING", "Function deployed for event normalization.", "builtin://function", "com.acme.fn.InvoiceNormalizer",
            "persistent://acme/orders/invoices", "persistent://acme/orders/invoices-normalized", 1, "{\"runtime\":\"java\"}", true),
        new PlatformArtifactDetails(environmentId, "FUNCTION", "payment-ledger-fanout", "acme", "finance",
            "STOPPED", "Function is configured but not currently running.", "builtin://function", "com.acme.fn.PaymentLedgerFanout",
            "persistent://acme/finance/payments", "persistent://acme/finance/payment-ledger", 1, "{\"runtime\":\"java\"}", true),
        new PlatformArtifactDetails(environmentId, "SOURCE", "orders-jdbc-source", "acme", "orders",
            "RUNNING", "Source pulls bounded test data into Pulsar.", "builtin://jdbc", "org.apache.pulsar.io.jdbc.JdbcSource",
            null, "persistent://acme/orders/order-events", 1, "{\"connector\":\"jdbc\"}", true),
        new PlatformArtifactDetails(environmentId, "SINK", "payments-elastic-sink", "acme", "finance",
            "RUNNING", "Sink forwards selected payment events to search storage.", "builtin://elastic-search", "org.apache.pulsar.io.elasticsearch.ElasticSearchSink",
            "persistent://acme/finance/payments", null, 1, "{\"connector\":\"elastic-search\"}", true),
        new PlatformArtifactDetails(environmentId, "CONNECTOR", "jdbc", null, null,
            "AVAILABLE", "Built-in connector available in this mock environment.", "builtin://jdbc", null, null, null, null, "{\"category\":\"source\"}", false),
        new PlatformArtifactDetails(environmentId, "CONNECTOR", "elastic-search", null, null,
            "AVAILABLE", "Connector catalog entry ready for guided deployment.", "builtin://elastic-search", null, null, null, null, "{\"category\":\"sink\"}", false));
  }

  private String platformArtifactKey(String artifactType, String tenant, String namespace, String name) {
    return String.join("::",
        artifactType == null ? "" : artifactType.trim().toUpperCase(),
        tenant == null ? "" : tenant.trim(),
        namespace == null ? "" : namespace.trim(),
        name == null ? "" : name.trim());
  }

  private String buildPlatformDetails(PlatformArtifactMutationRequest request) {
    String type = request.artifactType().trim().toUpperCase();
    if ("FUNCTION".equals(type)) {
      return "Function " + request.name().trim() + " is configured for " + request.tenant() + "/" + request.namespace() + ".";
    }
    if ("SOURCE".equals(type)) {
      return "Source " + request.name().trim() + " is ready to publish into " + trimToNull(request.outputTopic()) + ".";
    }
    return "Sink " + request.name().trim() + " is ready to drain " + trimToNull(request.inputTopic()) + ".";
  }

  private String defaultSchemaDefinition(SchemaSummary schema) {
    if (schema == null || !schema.present()) {
      return "";
    }
    if ("JSON".equalsIgnoreCase(schema.type())) {
      return "{\n  \"type\": \"object\",\n  \"title\": \"MockTopicEvent\",\n  \"properties\": {\n    \"id\": { \"type\": \"string\" }\n  }\n}";
    }
    return "type MockTopicEvent {\n  string id = 1;\n}";
  }

  private String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

}
