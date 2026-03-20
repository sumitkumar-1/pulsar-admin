package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyResponse;
import com.pulsaradmin.shared.model.TenantYamlDiffEntry;
import com.pulsaradmin.shared.model.TenantYamlPreviewRequest;
import com.pulsaradmin.shared.model.TenantYamlPreviewResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
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

    DesiredTenantState desiredState;
    try {
      desiredState = parseDesiredState(request);
    } catch (IllegalArgumentException exception) {
      return new TenantYamlPreviewResponse(
          null,
          environmentId,
          request.tenant(),
          false,
          "YAML validation failed.",
          List.of(exception.getMessage()),
          List.of());
    }

    List<TenantYamlDiffEntry> changes = diff(snapshot, desiredState);
    String previewId = storePreview ? UUID.randomUUID().toString() : null;
    if (storePreview) {
      previews.put(previewId, new StoredPreview(environmentId, desiredState.tenant(), desiredState, changes));
    }

    return new TenantYamlPreviewResponse(
        previewId,
        environmentId,
        desiredState.tenant(),
        true,
        changes.isEmpty()
            ? "No changes detected for tenant " + desiredState.tenant() + "."
            : "Preview generated for tenant " + desiredState.tenant() + ".",
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

    if (snapshot.tenants().stream().noneMatch(item -> item.equals(preview.tenant()))) {
      throw new BadRequestException("The selected tenant is not present in the current environment snapshot.");
    }

    applyDesiredState(environment, snapshot, preview.desiredState());
    environmentCatalogService.refreshEnvironment(environment);
    CatalogSummary catalogSummary = environmentCatalogService.getCatalogSummary(environmentId);
    previews.remove(request.previewId());

    return new TenantYamlApplyResponse(
        request.previewId(),
        environmentId,
        preview.tenant(),
        "Applied the previewed YAML changes for tenant " + preview.tenant() + ".",
        preview.changes(),
        catalogSummary);
  }

  private void applyDesiredState(
      EnvironmentRecord environment,
      EnvironmentSnapshotRecord snapshot,
      DesiredTenantState desiredState) {
    for (DesiredNamespaceState namespace : desiredState.namespaces().values()) {
      String fullNamespace = desiredState.tenant() + "/" + namespace.name();
      boolean namespaceExists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
      if (!namespaceExists) {
        environmentCatalogService.createNamespace(environment.id(), new CreateNamespaceRequest(desiredState.tenant(), namespace.name()));
      }

      environmentCatalogService.updateNamespacePolicies(
          environment.id(),
          new NamespacePoliciesUpdateRequest(desiredState.tenant(), namespace.name(), namespace.policies(), "Apply tenant YAML sync"));

      for (DesiredTopicState topic : namespace.topics().values()) {
        String fullTopicName = topic.fullTopicName(desiredState.tenant(), namespace.name());
        boolean topicExists = snapshot.topics().stream().anyMatch(item -> item.fullName().equals(fullTopicName));
        if (!topicExists) {
          environmentCatalogService.createTopic(environment.id(), new CreateTopicRequest(
              topic.domain(),
              desiredState.tenant(),
              namespace.name(),
              topic.name(),
              topic.partitions(),
              topic.notes()));
        }

        environmentCatalogService.updateTopicPolicies(
            environment.id(),
            new TopicPoliciesUpdateRequest(fullTopicName, topic.policies(), "Apply tenant YAML sync"));
      }
    }

    List<TopicDetails> tenantTopics = snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(desiredState.tenant()))
        .toList();

    for (TopicDetails existingTopic : tenantTopics) {
      DesiredNamespaceState namespace = desiredState.namespaces().get(existingTopic.namespace());
      boolean shouldExist = namespace != null && namespace.topics().containsKey(existingTopic.topic());
      if (!shouldExist) {
        environmentCatalogService.deleteTopicForSync(environment, existingTopic.fullName());
      }
    }

    for (String existingNamespace : snapshot.namespaces()) {
      if (!existingNamespace.startsWith(desiredState.tenant() + "/")) {
        continue;
      }
      String namespaceName = existingNamespace.substring(existingNamespace.indexOf('/') + 1);
      if (!desiredState.namespaces().containsKey(namespaceName)) {
        environmentCatalogService.deleteNamespaceForSync(environment, desiredState.tenant(), namespaceName);
      }
    }
  }

  private List<TenantYamlDiffEntry> diff(EnvironmentSnapshotRecord snapshot, DesiredTenantState desiredState) {
    List<TenantYamlDiffEntry> changes = new ArrayList<>();
    changes.add(new TenantYamlDiffEntry("KEEP", "TENANT", desiredState.tenant(), "Tenant scope selected for preview/apply."));

    for (DesiredNamespaceState namespace : desiredState.namespaces().values()) {
      String fullNamespace = desiredState.tenant() + "/" + namespace.name();
      boolean namespaceExists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
      changes.add(new TenantYamlDiffEntry(
          namespaceExists ? "UPDATE" : "CREATE",
          "NAMESPACE",
          fullNamespace,
          namespaceExists ? "Namespace exists and its policies will be aligned." : "Namespace will be created."));

      for (DesiredTopicState topic : namespace.topics().values()) {
        String fullTopicName = topic.fullTopicName(desiredState.tenant(), namespace.name());
        boolean topicExists = snapshot.topics().stream().anyMatch(item -> item.fullName().equals(fullTopicName));
        changes.add(new TenantYamlDiffEntry(
            topicExists ? "UPDATE" : "CREATE",
            "TOPIC",
            fullTopicName,
            topicExists ? "Topic exists and its policies will be aligned." : "Topic will be created."));
      }
    }

    snapshot.topics().stream()
        .filter(topic -> topic.tenant().equals(desiredState.tenant()))
        .filter(topic -> {
          DesiredNamespaceState namespace = desiredState.namespaces().get(topic.namespace());
          return namespace == null || !namespace.topics().containsKey(topic.topic());
        })
        .sorted((left, right) -> left.fullName().compareTo(right.fullName()))
        .forEach(topic -> changes.add(new TenantYamlDiffEntry(
            "REMOVE",
            "TOPIC",
            topic.fullName(),
            "Topic is present in the environment but absent from the desired YAML state.")));

    snapshot.namespaces().stream()
        .filter(namespace -> namespace.startsWith(desiredState.tenant() + "/"))
        .filter(namespace -> {
          String namespaceName = namespace.substring(namespace.indexOf('/') + 1);
          return !desiredState.namespaces().containsKey(namespaceName);
        })
        .sorted()
        .forEach(namespace -> changes.add(new TenantYamlDiffEntry(
            "REMOVE",
            "NAMESPACE",
            namespace,
            "Namespace is present in the environment but absent from the desired YAML state.")));

    return changes;
  }

  @SuppressWarnings("unchecked")
  private DesiredTenantState parseDesiredState(TenantYamlPreviewRequest request) {
    Object parsed = yaml.load(request.yaml());
    if (!(parsed instanceof Map<?, ?> root)) {
      throw new IllegalArgumentException("The YAML root must be an object.");
    }

    String tenant = stringValue(root.get("tenant"));
    if (!request.tenant().equals(tenant)) {
      throw new IllegalArgumentException("The YAML tenant must match the selected tenant.");
    }

    Map<String, DesiredNamespaceState> namespaces = new LinkedHashMap<>();
    Object namespaceValue = root.get("namespaces");
    if (namespaceValue instanceof List<?> namespaceList) {
      for (Object item : namespaceList) {
        if (!(item instanceof Map<?, ?> namespaceMap)) {
          throw new IllegalArgumentException("Each namespace entry must be an object.");
        }
        String namespaceName = stringValue(namespaceMap.get("name"));
        NamespacePolicies namespacePolicies = parseNamespacePolicies((Map<String, Object>) namespaceMap.get("policies"));

        Map<String, DesiredTopicState> topics = new LinkedHashMap<>();
        Object topicValue = namespaceMap.get("topics");
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

        namespaces.put(namespaceName, new DesiredNamespaceState(namespaceName, namespacePolicies, topics));
      }
    }

    return new DesiredTenantState(tenant, namespaces);
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

  private record DesiredTenantState(String tenant, Map<String, DesiredNamespaceState> namespaces) {
  }

  private record DesiredNamespaceState(
      String name,
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
      DesiredTenantState desiredState,
      List<TenantYamlDiffEntry> changes) {
  }
}
