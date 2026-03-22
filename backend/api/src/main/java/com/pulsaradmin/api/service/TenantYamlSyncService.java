package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.NamespaceYamlCurrentResponse;
import com.pulsaradmin.shared.model.TenantYamlApplyRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyResponse;
import com.pulsaradmin.shared.model.TenantYamlDiffEntry;
import com.pulsaradmin.shared.model.TenantYamlPreviewRequest;
import com.pulsaradmin.shared.model.TenantYamlPreviewResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
          List.of());
    }

    List<TenantYamlDiffEntry> changes = diff(snapshot, desiredState);
    String previewId = storePreview ? UUID.randomUUID().toString() : null;
    if (storePreview) {
      previews.put(previewId, new StoredPreview(environmentId, desiredState.tenant(), desiredState.namespace(), desiredState, changes));
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
        changes);
  }

  public TenantYamlApplyResponse apply(String environmentId, TenantYamlApplyRequest request) {
    StoredPreview preview = previews.get(request.previewId());
    if (preview == null || !preview.environmentId().equals(environmentId)) {
      throw new BadRequestException("Unknown or expired YAML preview. Generate a fresh preview before applying.");
    }

    EnvironmentRecord environment = environmentCatalogService.getEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = environmentCatalogService.refreshEnvironment(environment);

    applyDesiredState(environment, snapshot, preview.desiredState());
    environmentCatalogService.refreshEnvironment(environment);
    CatalogSummary catalogSummary = environmentCatalogService.getCatalogSummary(environmentId);
    previews.remove(request.previewId());

    return new TenantYamlApplyResponse(
        request.previewId(),
        environmentId,
        preview.tenant(),
        preview.namespace(),
        "Applied the previewed YAML changes for namespace " + preview.tenant() + "/" + preview.namespace() + ".",
        preview.changes(),
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

  private void applyDesiredState(
      EnvironmentRecord environment,
      EnvironmentSnapshotRecord snapshot,
      DesiredNamespaceState desiredState) {
    String fullNamespace = desiredState.tenant() + "/" + desiredState.namespace();
    boolean namespaceExists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    if (!namespaceExists) {
      environmentCatalogService.createNamespace(environment.id(), new CreateNamespaceRequest(desiredState.tenant(), desiredState.namespace()));
    }

    environmentCatalogService.updateNamespacePolicies(
        environment.id(),
        new NamespacePoliciesUpdateRequest(
            desiredState.tenant(),
            desiredState.namespace(),
            desiredState.policies(),
            "Apply namespace YAML sync"));

    for (DesiredTopicState topic : desiredState.topics().values()) {
      String fullTopicName = topic.fullTopicName(desiredState.tenant(), desiredState.namespace());
      boolean topicExists = snapshot.topics().stream().anyMatch(item -> item.fullName().equals(fullTopicName));
      if (!topicExists) {
        environmentCatalogService.createTopic(environment.id(), new CreateTopicRequest(
            topic.domain(),
            desiredState.tenant(),
            desiredState.namespace(),
            topic.name(),
            topic.partitions(),
            topic.notes()));
      }

      environmentCatalogService.updateTopicPolicies(
          environment.id(),
          new TopicPoliciesUpdateRequest(fullTopicName, topic.policies(), "Apply namespace YAML sync"));
    }

    List<TopicDetails> namespaceTopics = snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(desiredState.tenant()))
        .filter(topic -> topic.namespace().equals(desiredState.namespace()))
        .toList();

    for (TopicDetails existingTopic : namespaceTopics) {
      boolean shouldExist = desiredState.topics().containsKey(existingTopic.topic());
      if (!shouldExist) {
        environmentCatalogService.deleteTopicForSync(environment, existingTopic.fullName());
      }
    }
  }

  private List<TenantYamlDiffEntry> diff(EnvironmentSnapshotRecord snapshot, DesiredNamespaceState desiredState) {
    List<TenantYamlDiffEntry> changes = new ArrayList<>();
    String fullNamespace = desiredState.tenant() + "/" + desiredState.namespace();
    boolean namespaceExists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    changes.add(new TenantYamlDiffEntry(
        namespaceExists ? "UPDATE" : "CREATE",
        "NAMESPACE",
        fullNamespace,
        namespaceExists ? "Namespace exists and its policies will be aligned." : "Namespace will be created."));

    for (DesiredTopicState topic : desiredState.topics().values()) {
      String fullTopicName = topic.fullTopicName(desiredState.tenant(), desiredState.namespace());
      boolean topicExists = snapshot.topics().stream().anyMatch(item -> item.fullName().equals(fullTopicName));
      changes.add(new TenantYamlDiffEntry(
          topicExists ? "UPDATE" : "CREATE",
          "TOPIC",
          fullTopicName,
          topicExists ? "Topic exists and its policies will be aligned." : "Topic will be created."));
    }

    snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(desiredState.tenant()))
        .filter(topic -> topic.namespace().equals(desiredState.namespace()))
        .filter(topic -> !desiredState.topics().containsKey(topic.topic()))
        .sorted((left, right) -> left.fullName().compareTo(right.fullName()))
        .forEach(topic -> changes.add(new TenantYamlDiffEntry(
            "REMOVE",
            "TOPIC",
            topic.fullName(),
            "Topic is present in the selected namespace but absent from the desired YAML state.")));

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

  private record DesiredNamespaceState(
      String tenant,
      String namespace,
      NamespacePolicies policies,
      Map<String, DesiredTopicState> topics) {
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
      List<TenantYamlDiffEntry> changes) {
  }
}
