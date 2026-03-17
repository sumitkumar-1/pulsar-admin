package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicHealth;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicPartitionSummary;
import com.pulsaradmin.shared.model.TopicStatsSummary;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MockPulsarAdminGateway implements PulsarAdminGateway {
  private final List<EnvironmentSummary> environments;
  private final Map<String, EnvironmentHealth> environmentHealth;
  private final Map<String, List<TopicDetails>> topicsByEnvironment;

  public MockPulsarAdminGateway() {
    this.environments = seedEnvironments();
    this.environmentHealth = seedEnvironmentHealth();
    this.topicsByEnvironment = seedTopics();
  }

  @Override
  public List<EnvironmentSummary> getEnvironments() {
    return environments;
  }

  @Override
  public Optional<EnvironmentHealth> getEnvironmentHealth(String environmentId) {
    return Optional.ofNullable(environmentHealth.get(environmentId));
  }

  @Override
  public PagedResult<TopicListItem> getTopics(
      String environmentId,
      String tenant,
      String namespace,
      String search,
      int page,
      int pageSize) {
    List<TopicListItem> filtered = topicsByEnvironment.getOrDefault(environmentId, List.of()).stream()
        .filter(topic -> matches(topic.tenant(), tenant))
        .filter(topic -> matches(topic.namespace(), namespace))
        .filter(topic -> search == null || search.isBlank() || matchesSearch(topic, search))
        .map(this::toListItem)
        .sorted(Comparator.comparing(TopicListItem::namespace).thenComparing(TopicListItem::topic))
        .toList();

    int fromIndex = Math.min(page * pageSize, filtered.size());
    int toIndex = Math.min(fromIndex + pageSize, filtered.size());

    return new PagedResult<>(filtered.subList(fromIndex, toIndex), page, pageSize, filtered.size());
  }

  @Override
  public Optional<TopicDetails> getTopicDetails(String environmentId, String fullTopicName) {
    return topicsByEnvironment.getOrDefault(environmentId, List.of()).stream()
        .filter(topic -> topic.fullName().equals(fullTopicName))
        .findFirst();
  }

  private boolean matches(String value, String filter) {
    return filter == null || filter.isBlank() || value.equalsIgnoreCase(filter);
  }

  private boolean matchesSearch(TopicDetails topic, String search) {
    String normalized = search.toLowerCase(Locale.ROOT);
    return topic.fullName().toLowerCase(Locale.ROOT).contains(normalized)
        || topic.namespace().toLowerCase(Locale.ROOT).contains(normalized)
        || topic.topic().toLowerCase(Locale.ROOT).contains(normalized);
  }

  private TopicListItem toListItem(TopicDetails topic) {
    return new TopicListItem(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        topic.partitioned(),
        topic.partitions(),
        topic.schema().present(),
        topic.health(),
        topic.stats(),
        topic.notes());
  }

  private List<EnvironmentSummary> seedEnvironments() {
    return List.of(
        new EnvironmentSummary("prod", "Production", "prod", EnvironmentStatus.HEALTHY, "us-east-1", "cluster-a", "Primary customer traffic"),
        new EnvironmentSummary("stable", "Stable", "stable", EnvironmentStatus.HEALTHY, "us-east-1", "cluster-b", "Pre-release validation"),
        new EnvironmentSummary("qa", "QA", "qa", EnvironmentStatus.DEGRADED, "us-east-2", "cluster-c", "Regression and release testing"),
        new EnvironmentSummary("dev", "Development", "dev", EnvironmentStatus.HEALTHY, "local", "cluster-dev", "Everyday engineering workflows"));
  }

  private Map<String, EnvironmentHealth> seedEnvironmentHealth() {
    Map<String, EnvironmentHealth> health = new LinkedHashMap<>();
    health.put("prod", new EnvironmentHealth("prod", EnvironmentStatus.HEALTHY, "pulsar+ssl://prod-brokers:6651", "https://prod-admin.internal", "4.0.2", "All brokers reporting healthy."));
    health.put("stable", new EnvironmentHealth("stable", EnvironmentStatus.HEALTHY, "pulsar+ssl://stable-brokers:6651", "https://stable-admin.internal", "4.0.2", "Stable cluster available for release validation."));
    health.put("qa", new EnvironmentHealth("qa", EnvironmentStatus.DEGRADED, "pulsar://qa-brokers:6650", "https://qa-admin.internal", "4.0.1", "Backlog growth detected on two namespaces."));
    health.put("dev", new EnvironmentHealth("dev", EnvironmentStatus.HEALTHY, "pulsar://dev-brokers:6650", "https://dev-admin.internal", "4.0.1", "Good for day-to-day testing."));
    return health;
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

    environments.put("stable", environments.get("prod").stream()
        .map(topic -> cloneForEnvironment(topic, "stable", "Stable mirror topic for release checks."))
        .collect(Collectors.toList()));

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

  private TopicDetails cloneForEnvironment(TopicDetails original, String environmentId, String noteSuffix) {
    return new TopicDetails(
        original.fullName().replace("persistent://acme/", "persistent://acme/" + environmentId + "-"),
        original.tenant(),
        original.namespace(),
        original.topic(),
        original.partitioned(),
        original.partitions(),
        original.health(),
        original.stats(),
        original.schema(),
        original.ownerTeam(),
        noteSuffix,
        original.partitionSummaries(),
        original.subscriptions());
  }
}
