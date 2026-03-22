package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.NamespaceYamlCurrentResponse;
import com.pulsaradmin.shared.model.TenantYamlApplyResultEntry;
import com.pulsaradmin.shared.model.TenantYamlApplyRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyResponse;
import com.pulsaradmin.shared.model.TenantYamlDiffEntry;
import com.pulsaradmin.shared.model.TenantYamlFieldChange;
import com.pulsaradmin.shared.model.TenantYamlPreviewRequest;
import com.pulsaradmin.shared.model.TenantYamlPreviewResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

@Service
public class TenantYamlSyncService {
  private final EnvironmentCatalogService environmentCatalogService;
  private final ConcurrentMap<String, StoredPreview> previews = new ConcurrentHashMap<>();
  private final Yaml yaml = new Yaml();

  public TenantYamlSyncService(EnvironmentCatalogService environmentCatalogService) {
    this.environmentCatalogService = environmentCatalogService;
  }

  public TenantYamlPreviewResponse preview(String environmentId, TenantYamlPreviewRequest request, boolean storePreview) {
    EnvironmentRecord environment = environmentCatalogService.getEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = environmentCatalogService.refreshEnvironment(environment);

    DesiredNamespaceState desiredState;
    try {
      desiredState = parseDesiredState(request);
    } catch (IllegalArgumentException exception) {
      return new TenantYamlPreviewResponse(
          null,
          environmentId,
          request.tenant(),
          request.namespace(),
          false,
          "YAML validation failed.",
          List.of(exception.getMessage()),
          0,
          0,
          0,
          0,
          0,
          List.of(),
          List.of());
    }

    CurrentNamespaceState currentState = loadCurrentState(environmentId, desiredState.tenant(), desiredState.namespace(), snapshot);
    List<String> semanticErrors = validateDesiredState(currentState, desiredState);
    if (!semanticErrors.isEmpty()) {
      return new TenantYamlPreviewResponse(
          null,
          environmentId,
          desiredState.tenant(),
          desiredState.namespace(),
          false,
          "YAML preview found unsupported or unsafe changes that must be resolved first.",
          semanticErrors,
          0,
          0,
          0,
          0,
          0,
          List.of(),
          List.of());
    }

    List<TenantYamlDiffEntry> changes = diff(currentState, desiredState);
    List<String> requiredConfirmations = changes.stream()
        .filter(TenantYamlDiffEntry::requiresConfirmation)
        .map(TenantYamlDiffEntry::confirmationKey)
        .filter(Objects::nonNull)
        .toList();
    String previewId = storePreview ? UUID.randomUUID().toString() : null;
    if (storePreview) {
      previews.put(previewId, new StoredPreview(
          environmentId,
          desiredState.tenant(),
          desiredState.namespace(),
          desiredState,
          changes,
          requiredConfirmations));
    }

    return new TenantYamlPreviewResponse(
        previewId,
        environmentId,
        desiredState.tenant(),
        desiredState.namespace(),
        true,
        changes.isEmpty()
            ? "No changes detected for namespace " + desiredState.tenant() + "/" + desiredState.namespace() + "."
            : "Preview generated for namespace " + desiredState.tenant() + "/" + desiredState.namespace() + ".",
        List.of(),
        countChanges(changes, "CREATE"),
        countChanges(changes, "UPDATE"),
        countChanges(changes, "REMOVE"),
        (int) changes.stream().filter(change -> "REMOVE".equals(change.action()) && "danger".equals(change.severity())).count(),
        requiredConfirmations.size(),
        requiredConfirmations,
        changes);
  }

  public TenantYamlApplyResponse apply(String environmentId, TenantYamlApplyRequest request) {
    StoredPreview preview = previews.get(request.previewId());
    if (preview == null || !preview.environmentId().equals(environmentId)) {
      throw new BadRequestException("Unknown or expired YAML preview. Generate a fresh preview before applying.");
    }

    List<String> confirmedChangeKeys = request.confirmedChangeKeys() == null ? List.of() : request.confirmedChangeKeys();
    List<String> missingConfirmations = preview.requiredConfirmations().stream()
        .filter(key -> confirmedChangeKeys.stream().noneMatch(key::equals))
        .toList();
    if (!missingConfirmations.isEmpty()) {
      throw new BadRequestException("Confirm the destructive YAML removals before applying: " + String.join(", ", missingConfirmations));
    }

    EnvironmentRecord environment = environmentCatalogService.getEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = environmentCatalogService.refreshEnvironment(environment);

    List<TenantYamlApplyResultEntry> applyResults = applyDesiredState(environment, snapshot, preview.desiredState(), preview.changes());
    environmentCatalogService.refreshEnvironment(environment);
    CatalogSummary catalogSummary = environmentCatalogService.getCatalogSummary(environmentId);
    previews.remove(request.previewId());

    int appliedCount = (int) applyResults.stream().filter(result -> "APPLIED".equals(result.status())).count();
    int skippedCount = (int) applyResults.stream().filter(result -> "SKIPPED".equals(result.status())).count();
    int failedCount = (int) applyResults.stream().filter(result -> "FAILED".equals(result.status())).count();
    String message = failedCount == 0
        ? "Applied the previewed YAML changes for namespace " + preview.tenant() + "/" + preview.namespace() + "."
        : "Applied the YAML changes with some failures for namespace " + preview.tenant() + "/" + preview.namespace() + ".";

    return new TenantYamlApplyResponse(
        request.previewId(),
        environmentId,
        preview.tenant(),
        preview.namespace(),
        message,
        preview.changes(),
        applyResults,
        appliedCount,
        skippedCount,
        failedCount,
        catalogSummary);
  }

  public NamespaceYamlCurrentResponse currentYaml(String environmentId, String tenant, String namespace) {
    if (tenant == null || tenant.isBlank() || namespace == null || namespace.isBlank()) {
      throw new BadRequestException("A tenant and namespace are required to generate namespace YAML.");
    }

    NamespaceDetails namespaceDetails = environmentCatalogService.getNamespaceDetails(environmentId, tenant, namespace);
    String yamlText = renderCurrentYaml(environmentId, tenant, namespace, namespaceDetails);

    return new NamespaceYamlCurrentResponse(
        environmentId,
        tenant,
        namespace,
        yamlText,
        "Loaded the current namespace state into editable YAML.",
        Instant.now());
  }

  private List<TenantYamlApplyResultEntry> applyDesiredState(
      EnvironmentRecord environment,
      EnvironmentSnapshotRecord snapshot,
      DesiredNamespaceState desiredState,
      List<TenantYamlDiffEntry> previewChanges) {
    List<TenantYamlApplyResultEntry> results = new ArrayList<>();
    String fullNamespace = desiredState.tenant() + "/" + desiredState.namespace();
    boolean namespaceExists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    if (!namespaceExists) {
      try {
        environmentCatalogService.createNamespace(environment.id(), new CreateNamespaceRequest(desiredState.tenant(), desiredState.namespace()));
        results.add(new TenantYamlApplyResultEntry("CREATE", "NAMESPACE", fullNamespace, "APPLIED", "Namespace created."));
      } catch (RuntimeException exception) {
        results.add(new TenantYamlApplyResultEntry("CREATE", "NAMESPACE", fullNamespace, "FAILED", exception.getMessage()));
        return results;
      }
    }

    if (previewChanges.stream().anyMatch(change -> "NAMESPACE_POLICY".equals(change.resourceType()))) {
      try {
        environmentCatalogService.updateNamespacePolicies(
            environment.id(),
            new NamespacePoliciesUpdateRequest(
                desiredState.tenant(),
                desiredState.namespace(),
                desiredState.policies(),
                "Apply namespace YAML sync"));
        results.add(new TenantYamlApplyResultEntry("UPDATE", "NAMESPACE_POLICY", fullNamespace, "APPLIED", "Namespace policies aligned."));
      } catch (RuntimeException exception) {
        results.add(new TenantYamlApplyResultEntry("UPDATE", "NAMESPACE_POLICY", fullNamespace, "FAILED", exception.getMessage()));
      }
    }

    for (DesiredTopicState topic : desiredState.topics().values()) {
      String fullTopicName = topic.fullTopicName(desiredState.tenant(), desiredState.namespace());
      boolean topicExists = snapshot.topics().stream().anyMatch(item -> item.fullName().equals(fullTopicName));
      if (!topicExists) {
        try {
          environmentCatalogService.createTopic(environment.id(), new CreateTopicRequest(
              topic.domain(),
              desiredState.tenant(),
              desiredState.namespace(),
              topic.name(),
              topic.partitions(),
              topic.notes()));
          results.add(new TenantYamlApplyResultEntry("CREATE", "TOPIC", fullTopicName, "APPLIED", "Topic created."));
        } catch (RuntimeException exception) {
          results.add(new TenantYamlApplyResultEntry("CREATE", "TOPIC", fullTopicName, "FAILED", exception.getMessage()));
          continue;
        }
      }

      TopicDetails existingTopic = snapshot.topics().stream()
          .filter(item -> item.fullName().equals(fullTopicName))
          .findFirst()
          .orElse(null);
      if (existingTopic != null && existingTopic.partitioned() && topic.partitions() > existingTopic.partitions()) {
        try {
          environmentCatalogService.updateTopicPartitionsForSync(environment, fullTopicName, topic.partitions());
          results.add(new TenantYamlApplyResultEntry(
              "UPDATE",
              "TOPIC",
              fullTopicName,
              "APPLIED",
              "Partition count increased from " + existingTopic.partitions() + " to " + topic.partitions() + "."));
        } catch (RuntimeException exception) {
          results.add(new TenantYamlApplyResultEntry("UPDATE", "TOPIC", fullTopicName, "FAILED", exception.getMessage()));
          continue;
        }
      }

      if (previewChanges.stream().anyMatch(change -> fullTopicName.equals(change.resourceName()) && "TOPIC_POLICY".equals(change.resourceType()))) {
        try {
          environmentCatalogService.updateTopicPolicies(
              environment.id(),
              new TopicPoliciesUpdateRequest(fullTopicName, topic.policies(), "Apply namespace YAML sync"));
          results.add(new TenantYamlApplyResultEntry("UPDATE", "TOPIC_POLICY", fullTopicName, "APPLIED", "Topic policies aligned."));
        } catch (RuntimeException exception) {
          results.add(new TenantYamlApplyResultEntry("UPDATE", "TOPIC_POLICY", fullTopicName, "FAILED", exception.getMessage()));
        }
      }
    }

    List<TopicDetails> namespaceTopics = snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(desiredState.tenant()))
        .filter(topic -> topic.namespace().equals(desiredState.namespace()))
        .toList();

    for (TopicDetails existingTopic : namespaceTopics) {
      boolean shouldExist = desiredState.topics().containsKey(existingTopic.topic());
      if (!shouldExist) {
        try {
          environmentCatalogService.deleteTopicForSync(environment, existingTopic.fullName());
          results.add(new TenantYamlApplyResultEntry("REMOVE", "TOPIC", existingTopic.fullName(), "APPLIED", "Topic removed from namespace sync scope."));
        } catch (RuntimeException exception) {
          results.add(new TenantYamlApplyResultEntry("REMOVE", "TOPIC", existingTopic.fullName(), "FAILED", exception.getMessage()));
        }
      }
    }

    return results;
  }

  private List<TenantYamlDiffEntry> diff(CurrentNamespaceState currentState, DesiredNamespaceState desiredState) {
    List<TenantYamlDiffEntry> changes = new ArrayList<>();
    String fullNamespace = desiredState.tenant() + "/" + desiredState.namespace();
    if (!currentState.exists()) {
      changes.add(new TenantYamlDiffEntry(
          "CREATE",
          "NAMESPACE",
          fullNamespace,
          fullNamespace,
          "Namespace will be created before topic changes are applied.",
          "info",
          "add",
          List.of(),
          false,
          null,
          List.of()));
    }

    List<TenantYamlFieldChange> namespacePolicyChanges = namespacePolicyChanges(
        currentState.namespacePolicies(),
        desiredState.policies());
    if (!namespacePolicyChanges.isEmpty()) {
      changes.add(new TenantYamlDiffEntry(
          "UPDATE",
          "NAMESPACE_POLICY",
          fullNamespace,
          fullNamespace + " policies",
          "Namespace policies will be aligned to the YAML state.",
          "info",
          "edit",
          List.of(),
          false,
          null,
          namespacePolicyChanges));
    }

    for (DesiredTopicState topic : desiredState.topics().values()) {
      String fullTopicName = topic.fullTopicName(desiredState.tenant(), desiredState.namespace());
      CurrentTopicState currentTopic = currentState.topics().get(topic.name());
      if (currentTopic == null) {
        changes.add(new TenantYamlDiffEntry(
            "CREATE",
            "TOPIC",
            fullTopicName,
            topic.name(),
            "Topic will be created from the namespace YAML document.",
            "info",
            "add",
            List.of(),
            false,
            null,
            newTopicFieldChanges(topic)));
        continue;
      }

      List<TenantYamlFieldChange> topicLifecycleChanges = topicLifecycleChanges(currentTopic.details(), topic);
      if (!topicLifecycleChanges.isEmpty()) {
        changes.add(new TenantYamlDiffEntry(
            "UPDATE",
            "TOPIC",
            fullTopicName,
            topic.name(),
            "Topic lifecycle settings will be aligned to the YAML state.",
            "info",
            "edit",
            List.of(),
            false,
            null,
            topicLifecycleChanges));
      }

      List<TenantYamlFieldChange> topicPolicyChanges = topicPolicyChanges(currentTopic.policies(), topic.policies());
      if (!topicPolicyChanges.isEmpty()) {
        changes.add(new TenantYamlDiffEntry(
            "UPDATE",
            "TOPIC_POLICY",
            fullTopicName,
            topic.name() + " policies",
            "Topic policies will be aligned to the YAML state.",
            "info",
            "edit",
            List.of(),
            false,
            null,
            topicPolicyChanges));
      }
    }

    currentState.topics().values().stream()
        .filter(topic -> !desiredState.topics().containsKey(topic.details().topic()))
        .sorted(Comparator.comparing(topic -> topic.details().fullName()))
        .forEach(topic -> changes.add(removalEntry(topic)));

    return changes;
  }

  private String renderCurrentYaml(
      String environmentId,
      String tenant,
      String namespace,
      NamespaceDetails namespaceDetails) {
    StringBuilder builder = new StringBuilder();
    builder.append("tenant: ").append(tenant).append('\n');
    builder.append("namespace: ").append(namespace).append('\n');
    builder.append("policies:\n");
    appendNamespacePolicies(builder, namespaceDetails.policies());
    builder.append("topics:\n");

    List<TopicDetails> sortedTopics = namespaceDetails.topics().stream()
        .sorted((left, right) -> left.topic().compareTo(right.topic()))
        .map(topic -> environmentCatalogService.getTopicDetails(environmentId, topic.fullName()))
        .toList();

    for (TopicDetails topic : sortedTopics) {
      TopicPoliciesResponse topicPolicies = environmentCatalogService.getTopicPolicies(environmentId, topic.fullName());
      builder.append("  - name: ").append(topic.topic()).append('\n');
      builder.append("    domain: ").append(topic.fullName().startsWith("non-persistent://") ? "non-persistent" : "persistent").append('\n');
      builder.append("    partitions: ").append(Math.max(topic.partitions(), 0)).append('\n');
      if (topic.notes() != null && !topic.notes().isBlank()) {
        builder.append("    notes: ").append(yamlScalar(topic.notes())).append('\n');
      }
      builder.append("    policies:\n");
      appendTopicPolicies(builder, topicPolicies.policies(), "      ");
    }

    return builder.toString();
  }

  private void appendNamespacePolicies(StringBuilder builder, NamespacePolicies policies) {
    appendNamespacePolicy(builder, "retentionTimeInMinutes", policies.retentionTimeInMinutes());
    appendNamespacePolicy(builder, "retentionSizeInMb", policies.retentionSizeInMb());
    appendNamespacePolicy(builder, "messageTtlInSeconds", policies.messageTtlInSeconds());
    appendNamespacePolicy(builder, "deduplicationEnabled", policies.deduplicationEnabled());
    appendNamespacePolicy(builder, "backlogQuotaLimitInBytes", policies.backlogQuotaLimitInBytes());
    appendNamespacePolicy(builder, "backlogQuotaLimitTimeInSeconds", policies.backlogQuotaLimitTimeInSeconds());
    appendNamespacePolicy(builder, "dispatchRatePerTopicInMsg", policies.dispatchRatePerTopicInMsg());
    appendNamespacePolicy(builder, "dispatchRatePerTopicInByte", policies.dispatchRatePerTopicInByte());
    appendNamespacePolicy(builder, "publishRateInMsg", policies.publishRateInMsg());
    appendNamespacePolicy(builder, "publishRateInByte", policies.publishRateInByte());
  }

  private void appendTopicPolicies(StringBuilder builder, TopicPolicies policies, String indent) {
    appendTopicPolicy(builder, indent, "retentionTimeInMinutes", policies.retentionTimeInMinutes());
    appendTopicPolicy(builder, indent, "retentionSizeInMb", policies.retentionSizeInMb());
    appendTopicPolicy(builder, indent, "ttlInSeconds", policies.ttlInSeconds());
    appendTopicPolicy(builder, indent, "compactionThresholdInBytes", policies.compactionThresholdInBytes());
    appendTopicPolicy(builder, indent, "maxProducers", policies.maxProducers());
    appendTopicPolicy(builder, indent, "maxConsumers", policies.maxConsumers());
    appendTopicPolicy(builder, indent, "maxSubscriptions", policies.maxSubscriptions());
  }

  private void appendNamespacePolicy(StringBuilder builder, String key, Object value) {
    builder.append("  ").append(key).append(": ").append(value == null ? "null" : value).append('\n');
  }

  private void appendTopicPolicy(StringBuilder builder, String indent, String key, Object value) {
    builder.append(indent).append(key).append(": ").append(value == null ? "null" : value).append('\n');
  }

  private String yamlScalar(String value) {
    if (value == null) {
      return "\"\"";
    }
    String sanitized = value.replace("\"", "\\\"");
    return '"' + sanitized + '"';
  }

  @SuppressWarnings("unchecked")
  private DesiredNamespaceState parseDesiredState(TenantYamlPreviewRequest request) {
    Object parsed = yaml.load(request.yaml());
    if (!(parsed instanceof Map<?, ?> root)) {
      throw new IllegalArgumentException("The YAML root must be an object.");
    }

    requireOnlySupportedKeys("YAML root", root.keySet(), Set.of("tenant", "namespace", "policies", "topics"));

    String tenant = stringValue(root.get("tenant"));
    String namespace = stringValue(root.get("namespace"));
    if (!request.tenant().equals(tenant)) {
      throw new IllegalArgumentException("The YAML tenant must match the selected tenant.");
    }
    if (!request.namespace().equals(namespace)) {
      throw new IllegalArgumentException("The YAML namespace must match the selected namespace.");
    }

    NamespacePolicies namespacePolicies = parseNamespacePolicies((Map<String, Object>) root.get("policies"));
    Map<String, DesiredTopicState> topics = new LinkedHashMap<>();
    Object topicValue = root.get("topics");
    if (topicValue instanceof List<?> topicList) {
      for (Object topicItem : topicList) {
        if (!(topicItem instanceof Map<?, ?> topicMap)) {
          throw new IllegalArgumentException("Each topic entry must be an object.");
        }
        requireOnlySupportedKeys("Topic entry", topicMap.keySet(), Set.of("name", "domain", "partitions", "notes", "policies"));
        String topicName = stringValue(topicMap.get("name"));
        topics.put(topicName, new DesiredTopicState(
            stringValueOrDefault(topicMap.get("domain"), "persistent"),
            topicName,
            intValue(topicMap.get("partitions"), 0),
            nullableString(topicMap.get("notes")),
            parseTopicPolicies((Map<String, Object>) topicMap.get("policies"))));
      }
    }

    return new DesiredNamespaceState(tenant, namespace, namespacePolicies, topics);
  }

  private NamespacePolicies parseNamespacePolicies(Map<String, Object> map) {
    Map<String, Object> source = map == null ? Map.of() : map;
    requireOnlySupportedKeys(
        "Namespace policies",
        source.keySet(),
        Set.of(
            "retentionTimeInMinutes",
            "retentionSizeInMb",
            "messageTtlInSeconds",
            "deduplicationEnabled",
            "backlogQuotaLimitInBytes",
            "backlogQuotaLimitTimeInSeconds",
            "dispatchRatePerTopicInMsg",
            "dispatchRatePerTopicInByte",
            "publishRateInMsg",
            "publishRateInByte"));
    return new NamespacePolicies(
        intValue(source.get("retentionTimeInMinutes"), 0),
        intValue(source.get("retentionSizeInMb"), 0),
        intValue(source.get("messageTtlInSeconds"), 0),
        booleanValue(source.get("deduplicationEnabled"), false),
        longValue(source.get("backlogQuotaLimitInBytes"), 0L),
        intValue(source.get("backlogQuotaLimitTimeInSeconds"), 0),
        intValue(source.get("dispatchRatePerTopicInMsg"), 0),
        longValue(source.get("dispatchRatePerTopicInByte"), 0L),
        intValue(source.get("publishRateInMsg"), 0),
        longValue(source.get("publishRateInByte"), 0L));
  }

  private TopicPolicies parseTopicPolicies(Map<String, Object> map) {
    Map<String, Object> source = map == null ? Map.of() : map;
    requireOnlySupportedKeys(
        "Topic policies",
        source.keySet(),
        Set.of(
            "retentionTimeInMinutes",
            "retentionSizeInMb",
            "ttlInSeconds",
            "compactionThresholdInBytes",
            "maxProducers",
            "maxConsumers",
            "maxSubscriptions"));
    return new TopicPolicies(
        intValue(source.get("retentionTimeInMinutes"), 0),
        intValue(source.get("retentionSizeInMb"), 0),
        intValue(source.get("ttlInSeconds"), 0),
        longValue(source.get("compactionThresholdInBytes"), 0L),
        intValue(source.get("maxProducers"), 0),
        intValue(source.get("maxConsumers"), 0),
        intValue(source.get("maxSubscriptions"), 0));
  }

  private String stringValue(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("A required YAML field is missing.");
    }
    String text = String.valueOf(value).trim();
    if (text.isBlank()) {
      throw new IllegalArgumentException("A required YAML field is blank.");
    }
    return text;
  }

  private String nullableString(Object value) {
    if (value == null) {
      return null;
    }
    String text = String.valueOf(value).trim();
    return text.isBlank() ? null : text;
  }

  private String stringValueOrDefault(Object value, String defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    String text = String.valueOf(value).trim();
    return text.isBlank() ? defaultValue : text;
  }

  private int intValue(Object value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Integer.parseInt(String.valueOf(value));
  }

  private long longValue(Object value, long defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(String.valueOf(value));
  }

  private boolean booleanValue(Object value, boolean defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(String.valueOf(value));
  }

  private void requireOnlySupportedKeys(String section, Set<?> keys, Set<String> supportedKeys) {
    List<String> unsupported = keys.stream()
        .map(String::valueOf)
        .filter(key -> !supportedKeys.contains(key))
        .sorted()
        .toList();
    if (!unsupported.isEmpty()) {
      throw new IllegalArgumentException(section + " contains unsupported fields: " + String.join(", ", unsupported));
    }
  }

  private CurrentNamespaceState loadCurrentState(
      String environmentId,
      String tenant,
      String namespace,
      EnvironmentSnapshotRecord snapshot) {
    String fullNamespace = tenant + "/" + namespace;
    boolean exists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    if (!exists) {
      return new CurrentNamespaceState(false, null, Map.of());
    }

    NamespaceDetails namespaceDetails = environmentCatalogService.getNamespaceDetails(environmentId, tenant, namespace);
    Map<String, CurrentTopicState> topics = new LinkedHashMap<>();
    snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(tenant))
        .filter(topic -> topic.namespace().equals(namespace))
        .sorted(Comparator.comparing(TopicDetails::fullName))
        .forEach(topic -> {
          TopicPoliciesResponse policies = environmentCatalogService.getTopicPolicies(environmentId, topic.fullName());
          topics.put(topic.topic(), new CurrentTopicState(topic, policies.policies()));
        });

    return new CurrentNamespaceState(true, namespaceDetails.policies(), topics);
  }

  private List<String> validateDesiredState(CurrentNamespaceState currentState, DesiredNamespaceState desiredState) {
    List<String> errors = new ArrayList<>();

    for (DesiredTopicState desiredTopic : desiredState.topics().values()) {
      CurrentTopicState currentTopic = currentState.topics().get(desiredTopic.name());
      if (currentTopic == null) {
        continue;
      }

      String currentDomain = currentTopic.details().fullName().startsWith("non-persistent://") ? "non-persistent" : "persistent";
      if (!currentDomain.equals(desiredTopic.domain())) {
        errors.add("Changing the domain for existing topic " + desiredTopic.name() + " is not supported in namespace YAML sync.");
      }
      if (currentTopic.details().partitioned() && desiredTopic.partitions() < currentTopic.details().partitions()) {
        errors.add("Partition count for existing partitioned topic " + desiredTopic.name() + " can only be increased, not decreased.");
      }
      if (!currentTopic.details().partitioned() && desiredTopic.partitions() > 0) {
        errors.add("Changing existing non-partitioned topic " + desiredTopic.name() + " into a partitioned topic is not supported in namespace YAML sync.");
      }
      String currentNotes = currentTopic.details().notes() == null ? "" : currentTopic.details().notes();
      String desiredNotes = desiredTopic.notes() == null ? "" : desiredTopic.notes();
      if (!currentNotes.equals(desiredNotes)) {
        errors.add("Changing notes for existing topic " + desiredTopic.name() + " is not supported in namespace YAML sync.");
      }
    }

    return errors;
  }

  private TenantYamlDiffEntry removalEntry(CurrentTopicState topic) {
    List<String> riskFlags = new ArrayList<>();
    if (topic.details().stats().backlog() > 0) {
      riskFlags.add("Retained backlog: " + topic.details().stats().backlog() + " messages");
    }
    if (!topic.details().subscriptions().isEmpty()) {
      riskFlags.add("Subscriptions present: " + String.join(", ", topic.details().subscriptions()));
    }
    if (topic.details().partitioned()) {
      riskFlags.add("Partitioned topic with " + topic.details().partitions() + " partitions");
    }
    if (topic.details().schema().present()) {
      riskFlags.add("Schema metadata is present");
    }
    if (topic.details().stats().producers() > 0 || topic.details().stats().consumers() > 0 || topic.details().stats().publishRateIn() > 0) {
      riskFlags.add("Topic appears active based on producers, consumers, or publish rate");
    }

    boolean requiresConfirmation = !riskFlags.isEmpty();
    return new TenantYamlDiffEntry(
        "REMOVE",
        "TOPIC",
        topic.details().fullName(),
        topic.details().topic(),
        requiresConfirmation
            ? "Topic will be removed from the namespace and requires explicit confirmation first."
            : "Topic will be removed from the namespace.",
        requiresConfirmation ? "danger" : "caution",
        "delete",
        riskFlags,
        requiresConfirmation,
        requiresConfirmation ? "remove-topic:" + topic.details().fullName() : null,
        List.of());
  }

  private List<TenantYamlFieldChange> newTopicFieldChanges(DesiredTopicState topic) {
    List<TenantYamlFieldChange> changes = new ArrayList<>();
    changes.add(new TenantYamlFieldChange("domain", null, topic.domain()));
    changes.add(new TenantYamlFieldChange("partitions", null, String.valueOf(topic.partitions())));
    if (topic.notes() != null && !topic.notes().isBlank()) {
      changes.add(new TenantYamlFieldChange("notes", null, topic.notes()));
    }
    changes.addAll(topicPolicyChanges(null, topic.policies()));
    return changes;
  }

  private List<TenantYamlFieldChange> topicLifecycleChanges(TopicDetails current, DesiredTopicState desired) {
    List<TenantYamlFieldChange> changes = new ArrayList<>();
    if (current.partitioned() && desired.partitions() > current.partitions()) {
      changes.add(new TenantYamlFieldChange("partitions", String.valueOf(current.partitions()), String.valueOf(desired.partitions())));
    }
    return changes;
  }

  private List<TenantYamlFieldChange> namespacePolicyChanges(NamespacePolicies current, NamespacePolicies desired) {
    List<TenantYamlFieldChange> changes = new ArrayList<>();
    compareField(changes, "retentionTimeInMinutes", current == null ? null : current.retentionTimeInMinutes(), desired.retentionTimeInMinutes());
    compareField(changes, "retentionSizeInMb", current == null ? null : current.retentionSizeInMb(), desired.retentionSizeInMb());
    compareField(changes, "messageTtlInSeconds", current == null ? null : current.messageTtlInSeconds(), desired.messageTtlInSeconds());
    compareField(changes, "deduplicationEnabled", current == null ? null : current.deduplicationEnabled(), desired.deduplicationEnabled());
    compareField(changes, "backlogQuotaLimitInBytes", current == null ? null : current.backlogQuotaLimitInBytes(), desired.backlogQuotaLimitInBytes());
    compareField(changes, "backlogQuotaLimitTimeInSeconds", current == null ? null : current.backlogQuotaLimitTimeInSeconds(), desired.backlogQuotaLimitTimeInSeconds());
    compareField(changes, "dispatchRatePerTopicInMsg", current == null ? null : current.dispatchRatePerTopicInMsg(), desired.dispatchRatePerTopicInMsg());
    compareField(changes, "dispatchRatePerTopicInByte", current == null ? null : current.dispatchRatePerTopicInByte(), desired.dispatchRatePerTopicInByte());
    compareField(changes, "publishRateInMsg", current == null ? null : current.publishRateInMsg(), desired.publishRateInMsg());
    compareField(changes, "publishRateInByte", current == null ? null : current.publishRateInByte(), desired.publishRateInByte());
    return changes;
  }

  private List<TenantYamlFieldChange> topicPolicyChanges(TopicPolicies current, TopicPolicies desired) {
    List<TenantYamlFieldChange> changes = new ArrayList<>();
    compareField(changes, "retentionTimeInMinutes", current == null ? null : current.retentionTimeInMinutes(), desired.retentionTimeInMinutes());
    compareField(changes, "retentionSizeInMb", current == null ? null : current.retentionSizeInMb(), desired.retentionSizeInMb());
    compareField(changes, "ttlInSeconds", current == null ? null : current.ttlInSeconds(), desired.ttlInSeconds());
    compareField(changes, "compactionThresholdInBytes", current == null ? null : current.compactionThresholdInBytes(), desired.compactionThresholdInBytes());
    compareField(changes, "maxProducers", current == null ? null : current.maxProducers(), desired.maxProducers());
    compareField(changes, "maxConsumers", current == null ? null : current.maxConsumers(), desired.maxConsumers());
    compareField(changes, "maxSubscriptions", current == null ? null : current.maxSubscriptions(), desired.maxSubscriptions());
    return changes;
  }

  private void compareField(List<TenantYamlFieldChange> changes, String field, Object currentValue, Object desiredValue) {
    if (Objects.equals(currentValue, desiredValue)) {
      return;
    }
    changes.add(new TenantYamlFieldChange(field, stringify(currentValue), stringify(desiredValue)));
  }

  private String stringify(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private int countChanges(List<TenantYamlDiffEntry> changes, String action) {
    return (int) changes.stream().filter(change -> action.equals(change.action())).count();
  }

  private record DesiredNamespaceState(
      String tenant,
      String namespace,
      NamespacePolicies policies,
      Map<String, DesiredTopicState> topics) {
  }

  private record CurrentNamespaceState(
      boolean exists,
      NamespacePolicies namespacePolicies,
      Map<String, CurrentTopicState> topics) {
  }

  private record CurrentTopicState(
      TopicDetails details,
      TopicPolicies policies) {
  }

  private record DesiredTopicState(
      String domain,
      String name,
      int partitions,
      String notes,
      TopicPolicies policies) {
    private String fullTopicName(String tenant, String namespace) {
      return domain + "://" + tenant + "/" + namespace + "/" + name;
    }
  }

  private record StoredPreview(
      String environmentId,
      String tenant,
      String namespace,
      DesiredNamespaceState desiredState,
      List<TenantYamlDiffEntry> changes,
      List<String> requiredConfirmations) {
  }
}
