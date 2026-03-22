package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CatalogMutationResponse;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.ExportMessagesRequest;
import com.pulsaradmin.shared.model.ExportMessagesResponse;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.NamespacePoliciesResponse;
import com.pulsaradmin.shared.model.NamespaceDeleteRequest;
import com.pulsaradmin.shared.model.NamespaceMutationResponse;
import com.pulsaradmin.shared.model.NamespaceSummary;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PlatformArtifactDeleteRequest;
import com.pulsaradmin.shared.model.PlatformArtifactDetails;
import com.pulsaradmin.shared.model.PlatformArtifactMutationRequest;
import com.pulsaradmin.shared.model.PlatformArtifactMutationResponse;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.SchemaDeleteRequest;
import com.pulsaradmin.shared.model.SchemaDetails;
import com.pulsaradmin.shared.model.SchemaMutationResponse;
import com.pulsaradmin.shared.model.TenantDeleteRequest;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantMutationResponse;
import com.pulsaradmin.shared.model.TenantSummary;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.SchemaUpdateRequest;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.SubscriptionMutationResponse;
import com.pulsaradmin.shared.model.TopicHealth;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateResponse;
import com.pulsaradmin.shared.model.TopicDeleteRequest;
import com.pulsaradmin.shared.model.TopicDeleteResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicStatsSummary;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class EnvironmentCatalogService {
  private static final Set<Integer> PAGE_SIZES = Set.of(10, 25, 50, 100);

  private final EnvironmentRepository environmentRepository;
  private final EnvironmentSnapshotRepository snapshotRepository;
  private final PulsarAdminGateway pulsarAdminGateway;
  private final GatewayModeResolver gatewayModeResolver;
  private final MockEnvironmentStore mockEnvironmentStore;

  public EnvironmentCatalogService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      PulsarAdminGateway pulsarAdminGateway,
      GatewayModeResolver gatewayModeResolver,
      MockEnvironmentStore mockEnvironmentStore) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.pulsarAdminGateway = pulsarAdminGateway;
    this.gatewayModeResolver = gatewayModeResolver;
    this.mockEnvironmentStore = mockEnvironmentStore;
  }

  public EnvironmentHealth getEnvironmentHealth(String environmentId) {
    requireEnvironment(environmentId);
    return loadSnapshot(environmentId).toHealth();
  }

  public CatalogSummary getCatalogSummary(String environmentId) {
    requireEnvironment(environmentId);
    return toCatalogSummary(loadSnapshot(environmentId));
  }

  public PagedResult<TopicListItem> getTopics(
      String environmentId,
      String tenant,
      String namespace,
      String search,
      int page,
      int pageSize) {
    requireEnvironment(environmentId);
    validatePaging(page, pageSize);

    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    var filtered = collapseTopics(snapshot.topics()).stream()
        .filter(topic -> tenant == null || tenant.isBlank() || topic.tenant().equalsIgnoreCase(tenant))
        .filter(topic -> namespace == null || namespace.isBlank() || topic.namespace().equalsIgnoreCase(namespace))
        .filter(topic -> {
          if (search == null || search.isBlank()) {
            return true;
          }
          String normalized = search.toLowerCase();
          return topic.fullName().toLowerCase().contains(normalized)
              || topic.topic().toLowerCase().contains(normalized)
              || topic.namespace().toLowerCase().contains(normalized);
        })
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
        .sorted(java.util.Comparator.comparing(TopicListItem::namespace).thenComparing(TopicListItem::topic))
        .toList();

    int fromIndex = Math.min(page * pageSize, filtered.size());
    int toIndex = Math.min(fromIndex + pageSize, filtered.size());

    return new PagedResult<>(filtered.subList(fromIndex, toIndex), page, pageSize, filtered.size());
  }

  public TopicDetails getTopicDetails(String environmentId, String topicName) {
    requireEnvironment(environmentId);

    if (topicName == null || topicName.isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    return requireTopic(snapshot, topicName);
  }

  public TopicDetails createTopic(String environmentId, CreateTopicRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    validateCreateTopicRequest(snapshot, request);

    pulsarAdminGateway.createTopic(environment.toDetails(), request);

    EnvironmentSnapshotRecord refreshedSnapshot = refreshSnapshot(environment);

    return refreshedSnapshot.topics().stream()
        .filter(topic -> topic.fullName().equals(request.fullTopicName()))
        .findFirst()
        .orElseGet(() -> fallbackCreatedTopic(request));
  }

  public CatalogMutationResponse createTenant(String environmentId, CreateTenantRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    validateTopicSegment("tenant", request.tenant());

    if (snapshot.tenants().stream().anyMatch(existing -> existing.equalsIgnoreCase(request.tenant()))) {
      throw new BadRequestException("Tenant already exists: " + request.tenant());
    }

    pulsarAdminGateway.createTenant(environment.toDetails(), request);
    CatalogSummary catalogSummary = toCatalogSummary(refreshSnapshot(environment));

    return new CatalogMutationResponse(
        environmentId,
        "TENANT",
        request.tenant(),
        "Created tenant " + request.tenant() + " and refreshed the environment catalog.",
        catalogSummary);
  }

  public CatalogMutationResponse createNamespace(String environmentId, CreateNamespaceRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    validateTopicSegment("tenant", request.tenant());
    validateTopicSegment("namespace", request.namespace());

    if (snapshot.tenants().stream().noneMatch(existing -> existing.equalsIgnoreCase(request.tenant()))) {
      throw new NotFoundException("Unknown tenant: " + request.tenant());
    }

    String fullNamespace = request.tenant() + "/" + request.namespace();
    if (snapshot.namespaces().stream().anyMatch(existing -> existing.equalsIgnoreCase(fullNamespace))) {
      throw new BadRequestException("Namespace already exists: " + fullNamespace);
    }

    pulsarAdminGateway.createNamespace(environment.toDetails(), request);
    CatalogSummary catalogSummary = toCatalogSummary(refreshSnapshot(environment));

    return new CatalogMutationResponse(
        environmentId,
        "NAMESPACE",
        fullNamespace,
        "Created namespace " + fullNamespace + " and refreshed the environment catalog.",
        catalogSummary);
  }

  public TenantDetails getTenantDetails(String environmentId, String tenant) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    requireTenant(snapshot, tenant);
    TenantDetails gatewayDetails = pulsarAdminGateway.getTenantDetails(environment.toDetails(), tenant);

    return new TenantDetails(
        environmentId,
        tenant,
        gatewayDetails.adminRoles(),
        gatewayDetails.allowedClusters(),
        (int) snapshot.namespaces().stream().filter(namespace -> namespace.startsWith(tenant + "/")).count(),
        (int) snapshot.topics().stream().filter(topic -> topic.tenant().equals(tenant)).count(),
        snapshot.updatedAt());
  }

  public TenantMutationResponse updateTenant(String environmentId, TenantUpdateRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    requireTenant(snapshot, request.tenant());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when updating a tenant.");
    }

    TenantDetails updated = pulsarAdminGateway.updateTenant(environment.toDetails(), request);
    EnvironmentSnapshotRecord refreshed = refreshSnapshot(environment);
    CatalogSummary catalogSummary = toCatalogSummary(refreshed);

    return new TenantMutationResponse(
        environmentId,
        request.tenant(),
        "UPDATE",
        "Updated tenant " + request.tenant() + " and refreshed the environment catalog.",
        new TenantDetails(
            environmentId,
            updated.tenant(),
            updated.adminRoles(),
            updated.allowedClusters(),
            (int) refreshed.namespaces().stream().filter(namespace -> namespace.startsWith(request.tenant() + "/")).count(),
            (int) refreshed.topics().stream().filter(topic -> topic.tenant().equals(request.tenant())).count(),
            refreshed.updatedAt()),
        catalogSummary);
  }

  public TenantMutationResponse deleteTenant(String environmentId, TenantDeleteRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    requireTenant(snapshot, request.tenant());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when deleting a tenant.");
    }

    boolean hasNamespaces = snapshot.namespaces().stream().anyMatch(namespace -> namespace.startsWith(request.tenant() + "/"));
    if (hasNamespaces) {
      throw new BadRequestException("Delete namespaces under tenant " + request.tenant() + " before deleting the tenant itself.");
    }

    TenantDetails details = getTenantDetails(environmentId, request.tenant());
    pulsarAdminGateway.deleteTenant(environment.toDetails(), request.tenant());
    CatalogSummary catalogSummary = toCatalogSummary(refreshSnapshot(environment));

    return new TenantMutationResponse(
        environmentId,
        request.tenant(),
        "DELETE",
        "Deleted tenant " + request.tenant() + " and refreshed the environment catalog.",
        details,
        catalogSummary);
  }

  public SubscriptionMutationResponse createSubscription(String environmentId, CreateSubscriptionRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    TopicDetails topic = requireTopic(snapshot, request.topicName());
    validateSubscriptionName(request.subscriptionName());

    if (topic.subscriptions().stream().anyMatch(subscription -> subscription.equals(request.subscriptionName()))) {
      throw new BadRequestException("Subscription already exists: " + request.subscriptionName());
    }

    String normalizedInitialPosition = normalizeInitialPosition(request.initialPosition());
    pulsarAdminGateway.createSubscription(environment.toDetails(), request);

    TopicDetails updatedTopic = refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElseGet(() -> fallbackTopicWithCreatedSubscription(topic, request.subscriptionName()));

    return new SubscriptionMutationResponse(
        environmentId,
        request.topicName(),
        request.subscriptionName(),
        "CREATE",
        normalizedInitialPosition,
        "Created subscription " + request.subscriptionName() + " at " + normalizedInitialPosition.toLowerCase()
            + " for topic " + request.topicName() + ".",
        updatedTopic);
  }

  public SubscriptionMutationResponse deleteSubscription(String environmentId, String topicName, String subscriptionName) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    if (topicName == null || topicName.isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    validateSubscriptionName(subscriptionName);

    TopicDetails topic = requireTopic(snapshot, topicName);
    if (topic.subscriptions().stream().noneMatch(subscription -> subscription.equals(subscriptionName))) {
      throw new NotFoundException("Unknown subscription: " + subscriptionName);
    }

    pulsarAdminGateway.deleteSubscription(environment.toDetails(), topicName, subscriptionName);

    TopicDetails updatedTopic = refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(topicName))
        .findFirst()
        .orElseGet(() -> fallbackTopicWithoutSubscription(topic, subscriptionName));

    return new SubscriptionMutationResponse(
        environmentId,
        topicName,
        subscriptionName,
        "DELETE",
        null,
        "Deleted subscription " + subscriptionName + " from topic " + topicName + ".",
        updatedTopic);
  }

  public PeekMessagesResponse peekMessages(String environmentId, String topicName, int limit) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);

    if (topicName == null || topicName.isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    if (limit < 1 || limit > 25) {
      throw new BadRequestException("Peek limit must be between 1 and 25.");
    }

    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    boolean exists = snapshot.topics().stream().anyMatch(topic -> topic.fullName().equals(topicName));

    if (!exists) {
      throw new NotFoundException("Unknown topic: " + topicName);
    }

    return pulsarAdminGateway.peekMessages(environment.toDetails(), topicName, limit);
  }

  public TerminateTopicResponse terminateTopic(String environmentId, TerminateTopicRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails existingTopic = requireTopicFromSnapshot(environmentId, request.topicName());

    if (existingTopic.partitioned()) {
      throw new BadRequestException("Termination is not supported for partitioned topics. Select a non-partitioned topic instead.");
    }

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when terminating a topic.");
    }

    TerminateTopicResponse gatewayResponse = pulsarAdminGateway.terminateTopic(environment.toDetails(), request);
    TopicDetails updatedTopic = refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElse(existingTopic);

    return new TerminateTopicResponse(
        environmentId,
        request.topicName(),
        gatewayResponse.lastMessageId(),
        gatewayResponse.message(),
        updatedTopic);
  }

  public TopicPoliciesResponse getTopicPolicies(String environmentId, String topicName) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    requireTopicFromSnapshot(environmentId, topicName);

    return new TopicPoliciesResponse(
        environmentId,
        topicName,
        pulsarAdminGateway.getTopicPolicies(environment.toDetails(), topicName),
        true,
        "Topic policy view loaded.");
  }

  public TopicPoliciesUpdateResponse updateTopicPolicies(String environmentId, TopicPoliciesUpdateRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when updating topic policies.");
    }

    TopicPolicies updatedPolicies = pulsarAdminGateway.updateTopicPolicies(
        environment.toDetails(),
        request.topicName(),
        sanitizeTopicPolicies(request.policies()));

    TopicDetails updatedTopic = requireTopic(refreshSnapshot(environment), request.topicName());

    return new TopicPoliciesUpdateResponse(
        environmentId,
        request.topicName(),
        updatedPolicies,
        "Updated policies for " + request.topicName() + ".",
        updatedTopic);
  }

  public NamespaceDetails getNamespaceDetails(String environmentId, String tenant, String namespace) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    requireNamespace(snapshot, tenant, namespace);

    NamespaceDetails gatewayDetails = pulsarAdminGateway.getNamespaceDetails(environment.toDetails(), tenant, namespace);
    List<TopicListItem> topics = collapseTopics(snapshot.topics()).stream()
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
        environmentId,
        tenant,
        namespace,
        topics.size(),
        topics,
        gatewayDetails.policies(),
        snapshot.updatedAt(),
        snapshot.healthMessage());
  }

  public NamespacePoliciesResponse updateNamespacePolicies(String environmentId, NamespacePoliciesUpdateRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    requireNamespace(snapshot, request.tenant(), request.namespace());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when updating namespace policies.");
    }

    NamespacePolicies updatedPolicies = pulsarAdminGateway.updateNamespacePolicies(
        environment.toDetails(),
        request.tenant(),
        request.namespace(),
        sanitizeNamespacePolicies(request.policies()));

    EnvironmentSnapshotRecord refreshed = refreshSnapshot(environment);
    NamespaceDetails details = getNamespaceDetails(environmentId, request.tenant(), request.namespace());

    return new NamespacePoliciesResponse(
        environmentId,
        request.tenant(),
        request.namespace(),
        updatedPolicies,
        "Updated policies for namespace " + request.tenant() + "/" + request.namespace() + ".",
        new NamespaceDetails(
            details.environmentId(),
            details.tenant(),
            details.namespace(),
            details.topicCount(),
            details.topics(),
            updatedPolicies,
            refreshed.updatedAt(),
            refreshed.healthMessage()));
  }

  public NamespaceMutationResponse deleteNamespace(String environmentId, NamespaceDeleteRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    requireNamespace(snapshot, request.tenant(), request.namespace());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when deleting a namespace.");
    }

    boolean hasTopics = snapshot.topics().stream()
        .anyMatch(topic -> topic.tenant().equals(request.tenant()) && topic.namespace().equals(request.namespace()));
    if (hasTopics) {
      throw new BadRequestException("Delete all topics in " + request.tenant() + "/" + request.namespace() + " before deleting the namespace.");
    }

    pulsarAdminGateway.deleteNamespace(environment.toDetails(), request.tenant(), request.namespace());
    CatalogSummary catalogSummary = toCatalogSummary(refreshSnapshot(environment));

    return new NamespaceMutationResponse(
        environmentId,
        request.tenant(),
        request.namespace(),
        "DELETE",
        "Deleted namespace " + request.tenant() + "/" + request.namespace() + " and refreshed the environment catalog.",
        catalogSummary);
  }

  public PublishMessageResponse publishMessage(String environmentId, PublishMessageRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.payload() == null || request.payload().isBlank()) {
      throw new BadRequestException("A payload is required.");
    }

    if (request.payload().length() > 20_000) {
      throw new BadRequestException("Payloads are limited to 20,000 characters in the test console.");
    }

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when publishing a test message.");
    }

    SchemaValidation schemaValidation = validatePublishSchema(topic, request);
    PublishMessageResponse response = pulsarAdminGateway.publishMessage(environment.toDetails(), request);
    return new PublishMessageResponse(
        response.environmentId(),
        response.topicName(),
        response.messageId(),
        response.key(),
        response.properties(),
        response.schemaMode(),
        response.publishedAt(),
        response.message(),
        schemaValidation.warnings());
  }

  public ConsumeMessagesResponse consumeMessages(String environmentId, ConsumeMessagesRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    requireTopicFromSnapshot(environmentId, request.topicName());

    if (!request.ephemeral() && (request.subscriptionName() == null || request.subscriptionName().isBlank())) {
      throw new BadRequestException("A subscription name is required unless the consume flow is ephemeral.");
    }

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when consuming test messages.");
    }

    ConsumeMessagesResponse response = pulsarAdminGateway.consumeMessages(environment.toDetails(), request);
    List<String> warnings = buildConsumeWarnings(requireTopicFromSnapshot(environmentId, request.topicName()), request);
    return new ConsumeMessagesResponse(
        response.environmentId(),
        response.topicName(),
        response.subscriptionName(),
        response.ephemeral(),
        response.requestedCount(),
        response.receivedCount(),
        response.waitTimeSeconds(),
        response.completed(),
        response.completedAt(),
        response.message(),
        response.messages(),
        warnings);
  }

  public ExportMessagesResponse exportMessages(String environmentId, ExportMessagesRequest request) {
    requireEnvironment(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when exporting messages.");
    }

    String source = request.source() == null ? "" : request.source().trim().toUpperCase();
    if (!source.equals("PEEK") && !source.equals("CONSUME")) {
      throw new BadRequestException("Export source must be PEEK or CONSUME.");
    }

    List<String> warnings = new ArrayList<>();
    String content;
    int exportedCount;

    if (source.equals("PEEK")) {
      PeekMessagesResponse response = peekMessages(environmentId, request.topicName(), request.maxMessages());
      warnings.addAll(buildPeekWarnings(topic));
      content = buildPeekExportJson(response);
      exportedCount = response.returnedCount();
    } else {
      ConsumeMessagesRequest consumeRequest = new ConsumeMessagesRequest(
          request.topicName(),
          request.ephemeral() ? null : request.subscriptionName(),
          request.ephemeral(),
          request.maxMessages(),
          request.waitTimeSeconds(),
          request.reason());
      ConsumeMessagesResponse response = consumeMessages(environmentId, consumeRequest);
      warnings.addAll(response.warnings());
      content = buildConsumeExportJson(response);
      exportedCount = response.receivedCount();
    }

    return new ExportMessagesResponse(
        environmentId,
        request.topicName(),
        source,
        exportedCount,
        exportFileName(topic, source),
        "application/json",
        content,
        Instant.now(),
        exportedCount == 0
            ? "No messages were available for export within the bounded window."
            : "Prepared a bounded " + source.toLowerCase() + " export for " + request.topicName() + ".",
        warnings.stream().distinct().toList());
  }

  public ResetCursorResponse resetCursor(String environmentId, ResetCursorRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);

    if (request.topicName() == null || request.topicName().isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    if (request.subscriptionName() == null || request.subscriptionName().isBlank()) {
      throw new BadRequestException("A subscription name is required.");
    }

    String normalizedTarget = request.target() == null ? "" : request.target().trim().toUpperCase();
    if (!normalizedTarget.equals("EARLIEST")
        && !normalizedTarget.equals("LATEST")
        && !normalizedTarget.equals("TIMESTAMP")) {
      throw new BadRequestException("Reset target must be EARLIEST, LATEST, or TIMESTAMP.");
    }

    if (normalizedTarget.equals("TIMESTAMP")
        && (request.timestamp() == null || request.timestamp().isBlank())) {
      throw new BadRequestException("A timestamp is required when reset target is TIMESTAMP.");
    }

    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    TopicDetails topic = snapshot.topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + request.topicName()));

    boolean subscriptionExists = topic.subscriptions().stream()
        .anyMatch(subscription -> subscription.equals(request.subscriptionName()));

    if (!subscriptionExists) {
      throw new NotFoundException("Unknown subscription: " + request.subscriptionName());
    }

    return pulsarAdminGateway.resetCursor(environment.toDetails(), request);
  }

  public SkipMessagesResponse skipMessages(String environmentId, SkipMessagesRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);

    if (request.topicName() == null || request.topicName().isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    if (request.subscriptionName() == null || request.subscriptionName().isBlank()) {
      throw new BadRequestException("A subscription name is required.");
    }

    if (request.messageCount() < 1 || request.messageCount() > 5000) {
      throw new BadRequestException("Message count must be between 1 and 5000.");
    }

    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);

    TopicDetails topic = snapshot.topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + request.topicName()));

    boolean subscriptionExists = topic.subscriptions().stream()
        .anyMatch(subscription -> subscription.equals(request.subscriptionName()));

    if (!subscriptionExists) {
      throw new NotFoundException("Unknown subscription: " + request.subscriptionName());
    }

    return pulsarAdminGateway.skipMessages(environment.toDetails(), request);
  }

  public UnloadTopicResponse unloadTopic(String environmentId, UnloadTopicRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when unloading a topic.");
    }

    UnloadTopicResponse gatewayResponse = pulsarAdminGateway.unloadTopic(environment.toDetails(), request);
    TopicDetails updatedTopic = refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElse(topic);

    return new UnloadTopicResponse(
        environmentId,
        request.topicName(),
        gatewayResponse.message(),
        updatedTopic);
  }

  public TopicDeleteResponse deleteTopic(String environmentId, TopicDeleteRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when deleting a topic.");
    }

    pulsarAdminGateway.deleteTopic(environment.toDetails(), topic.fullName());
    CatalogSummary catalogSummary = toCatalogSummary(refreshSnapshot(environment));

    return new TopicDeleteResponse(
        environmentId,
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        "Deleted topic " + topic.fullName() + " and refreshed the environment catalog.",
        catalogSummary);
  }

  public PlatformSummary getPlatformSummary(String environmentId) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    return pulsarAdminGateway.getPlatformSummary(environment.toDetails(), snapshot.namespaces());
  }

  public PlatformArtifactDetails getPlatformArtifactDetails(
      String environmentId,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    String normalizedType = normalizePlatformArtifactType(artifactType);
    validatePlatformScope(normalizedType, tenant, namespace, name);
    return pulsarAdminGateway.getPlatformArtifactDetails(
        environment.toDetails(),
        normalizedType,
        sanitizeNullable(tenant),
        sanitizeNullable(namespace),
        name.trim());
  }

  public PlatformArtifactMutationResponse upsertPlatformArtifact(
      String environmentId,
      PlatformArtifactMutationRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    String normalizedType = normalizePlatformArtifactType(request.artifactType());
    validatePlatformScope(normalizedType, request.tenant(), request.namespace(), request.name());
    requireReason(request.reason(), "saving a platform artifact");
    if ("CONNECTOR".equals(normalizedType)) {
      throw new BadRequestException(
          "Connector catalog entries are read-only. Deploy them as sources or sinks through the guided platform workflows.");
    }

    PlatformArtifactDetails details =
        pulsarAdminGateway.upsertPlatformArtifact(environment.toDetails(), request);
    PlatformSummary platformSummary = getPlatformSummary(environmentId);

    return new PlatformArtifactMutationResponse(
        environmentId,
        normalizedType,
        "UPSERT",
        request.name().trim(),
        "Saved " + normalizedType.toLowerCase() + " " + request.name().trim() + " and refreshed platform coverage.",
        details,
        platformSummary);
  }

  public PlatformArtifactMutationResponse deletePlatformArtifact(
      String environmentId,
      PlatformArtifactDeleteRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    String normalizedType = normalizePlatformArtifactType(request.artifactType());
    validatePlatformScope(normalizedType, request.tenant(), request.namespace(), request.name());
    requireReason(request.reason(), "deleting a platform artifact");
    if ("CONNECTOR".equals(normalizedType)) {
      throw new BadRequestException("Connector catalog entries cannot be deleted from the live cluster catalog.");
    }

    pulsarAdminGateway.deletePlatformArtifact(
        environment.toDetails(),
        normalizedType,
        sanitizeNullable(request.tenant()),
        sanitizeNullable(request.namespace()),
        request.name().trim());
    PlatformSummary platformSummary = getPlatformSummary(environmentId);

    return new PlatformArtifactMutationResponse(
        environmentId,
        normalizedType,
        "DELETE",
        request.name().trim(),
        "Deleted " + normalizedType.toLowerCase() + " " + request.name().trim() + " and refreshed platform coverage.",
        null,
        platformSummary);
  }

  public SchemaDetails getSchemaDetails(String environmentId, String topicName) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, topicName);
    return pulsarAdminGateway.getSchemaDetails(environment.toDetails(), topic.fullName());
  }

  public SchemaMutationResponse upsertSchema(String environmentId, SchemaUpdateRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());
    requireReason(request.reason(), "updating a topic schema");
    if (request.definition() == null || request.definition().isBlank()) {
      throw new BadRequestException("A schema definition is required.");
    }
    if (request.schemaType() == null || request.schemaType().isBlank()) {
      throw new BadRequestException("A schema type is required.");
    }

    SchemaDetails schema = pulsarAdminGateway.upsertSchema(environment.toDetails(), request);
    TopicDetails refreshedTopic = findTopicAfterRefresh(environment, topic.fullName(), topic);

    return new SchemaMutationResponse(
        environmentId,
        topic.fullName(),
        "UPSERT",
        "Saved schema for " + topic.fullName() + " and refreshed topic metadata.",
        schema,
        refreshedTopic);
  }

  public SchemaMutationResponse deleteSchema(String environmentId, SchemaDeleteRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails topic = requireTopicFromSnapshot(environmentId, request.topicName());
    requireReason(request.reason(), "deleting a topic schema");

    pulsarAdminGateway.deleteSchema(environment.toDetails(), topic.fullName());
    TopicDetails refreshedTopic = findTopicAfterRefresh(environment, topic.fullName(), topic);

    return new SchemaMutationResponse(
        environmentId,
        topic.fullName(),
        "DELETE",
        "Deleted schema for " + topic.fullName() + " and refreshed topic metadata.",
        new SchemaDetails(environmentId, topic.fullName(), false, "NONE", "-", "N/A", "", true,
            "Schema definition removed from the topic."),
        refreshedTopic);
  }

  private void requireEnvironment(String environmentId) {
    requireEnvironmentRecord(environmentId);
  }

  void deleteTopicForSync(EnvironmentRecord environment, String topicName) {
    pulsarAdminGateway.deleteTopic(environment.toDetails(), topicName);
  }

  void updateTopicPartitionsForSync(EnvironmentRecord environment, String topicName, int partitions) {
    pulsarAdminGateway.updateTopicPartitions(environment.toDetails(), topicName, partitions);
  }

  void deleteNamespaceForSync(EnvironmentRecord environment, String tenant, String namespace) {
    pulsarAdminGateway.deleteNamespace(environment.toDetails(), tenant, namespace);
  }

  EnvironmentSnapshotRecord refreshEnvironment(EnvironmentRecord environment) {
    return refreshSnapshot(environment);
  }

  private void validateCreateTopicRequest(EnvironmentSnapshotRecord snapshot, CreateTopicRequest request) {
    String domain = request.domain() == null ? "" : request.domain().trim().toLowerCase();
    if (!domain.equals("persistent") && !domain.equals("non-persistent")) {
      throw new BadRequestException("Topic domain must be either persistent or non-persistent.");
    }

    validateTopicSegment("tenant", request.tenant());
    validateTopicSegment("namespace", request.namespace());
    validateTopicSegment("topic", request.topic());

    if (request.partitions() < 0 || request.partitions() > 128) {
      throw new BadRequestException("Partition count must be between 0 and 128.");
    }

    String fullTopicName = request.fullTopicName();
    if (snapshot.topics().stream().anyMatch(topic -> topic.fullName().equals(fullTopicName))) {
      throw new BadRequestException("Topic already exists: " + fullTopicName);
    }
  }

  private EnvironmentRecord requireEnvironmentRecord(String environmentId) {
    if (isMockMode()) {
      return mockEnvironmentStore.findActiveById(environmentId);
    }
    return environmentRepository.findActiveById(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));
  }

  EnvironmentRecord getEnvironmentRecord(String environmentId) {
    return requireEnvironmentRecord(environmentId);
  }

  private void validatePaging(int page, int pageSize) {
    if (page < 0) {
      throw new BadRequestException("Page must be zero or greater.");
    }

    if (!PAGE_SIZES.contains(pageSize)) {
      throw new BadRequestException("Page size must be one of " + PAGE_SIZES + ".");
    }
  }

  private void validateTopicSegment(String fieldName, String value) {
    if (value == null || value.isBlank()) {
      throw new BadRequestException("Topic " + fieldName + " is required.");
    }

    if (!value.matches("[A-Za-z0-9._-]+")) {
      throw new BadRequestException(
          "Topic " + fieldName + " can contain only letters, numbers, dots, dashes, and underscores.");
    }
  }

  private void requireReason(String reason, String actionDescription) {
    if (reason == null || reason.isBlank()) {
      throw new BadRequestException("A reason is required when " + actionDescription + ".");
    }
  }

  private String normalizePlatformArtifactType(String artifactType) {
    String normalized = artifactType == null ? "" : artifactType.trim().toUpperCase();
    return switch (normalized) {
      case "FUNCTION", "FUNCTIONS" -> "FUNCTION";
      case "SOURCE", "SOURCES" -> "SOURCE";
      case "SINK", "SINKS" -> "SINK";
      case "CONNECTOR", "CONNECTORS" -> "CONNECTOR";
      default -> throw new BadRequestException(
          "Platform artifact type must be one of FUNCTION, SOURCE, SINK, or CONNECTOR.");
    };
  }

  private void validatePlatformScope(String artifactType, String tenant, String namespace, String name) {
    validateTopicSegment("artifact name", name);
    if (!"CONNECTOR".equals(artifactType)) {
      validateTopicSegment("tenant", tenant);
      validateTopicSegment("namespace", namespace);
    }
  }

  private String sanitizeNullable(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private void validateSubscriptionName(String value) {
    if (value == null || value.isBlank()) {
      throw new BadRequestException("A subscription name is required.");
    }

    if (!value.matches("[A-Za-z0-9._-]+")) {
      throw new BadRequestException(
          "Subscription name can contain only letters, numbers, dots, dashes, and underscores.");
    }
  }

  private String normalizeInitialPosition(String initialPosition) {
    String normalized = initialPosition == null ? "" : initialPosition.trim().toUpperCase();
    if (!normalized.equals("EARLIEST") && !normalized.equals("LATEST")) {
      throw new BadRequestException("Initial position must be EARLIEST or LATEST.");
    }
    return normalized;
  }

  private SchemaValidation validatePublishSchema(TopicDetails topic, PublishMessageRequest request) {
    List<String> warnings = new ArrayList<>();
    String schemaMode = request.schemaMode() == null ? "RAW" : request.schemaMode().trim().toUpperCase();
    SchemaSummary schema = safeSchema(topic);
    String schemaType = schema.type() == null ? "NONE" : schema.type().trim().toUpperCase();

    if ("JSON".equals(schemaMode)) {
      try {
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(request.payload());
      } catch (Exception exception) {
        throw new BadRequestException("JSON schema mode requires a valid JSON payload.");
      }
    }

    if (!schema.present()) {
      if ("JSON".equals(schemaMode)) {
        warnings.add("This topic does not expose schema metadata. JSON mode will still publish a bounded test message, but compatibility cannot be verified.");
      }
      return new SchemaValidation(warnings);
    }

    if (schemaType.contains("JSON") && !"JSON".equals(schemaMode)) {
      warnings.add("This topic advertises a JSON schema. RAW publish mode may bypass the expected payload shape.");
    } else if (!schemaType.contains("JSON") && "JSON".equals(schemaMode)) {
      warnings.add("This topic advertises schema type " + schema.type() + ". JSON mode may not match the live schema encoding.");
    }

    if (schema.compatibility() != null
        && !schema.compatibility().isBlank()
        && !"NONE".equalsIgnoreCase(schema.compatibility())) {
      warnings.add("Schema compatibility is " + schema.compatibility() + ". Validate your payload before publishing to production-like environments.");
    }

    return new SchemaValidation(warnings);
  }

  private List<String> buildConsumeWarnings(TopicDetails topic, ConsumeMessagesRequest request) {
    List<String> warnings = new ArrayList<>();
    if (request.ephemeral()) {
      warnings.add("Ephemeral consume mode is bounded and may not reflect committed subscription state.");
    } else if (request.subscriptionName() != null && !request.subscriptionName().isBlank()) {
      warnings.add("Consume results reflect subscription " + request.subscriptionName() + " within a bounded test window only.");
    }
    warnings.addAll(buildPeekWarnings(topic));
    return warnings.stream().distinct().toList();
  }

  private List<String> buildPeekWarnings(TopicDetails topic) {
    List<String> warnings = new ArrayList<>();
    SchemaSummary schema = safeSchema(topic);
    if (!schema.present()) {
      warnings.add("Schema metadata is unavailable for this topic, so payload compatibility cannot be verified.");
    } else {
      warnings.add("Payloads on this topic are governed by schema type " + schema.type() + ".");
    }
    if (topic.partitioned()) {
      warnings.add("Partitioned topics may return messages from only a subset of partitions during bounded reads.");
    }
    return warnings;
  }

  private String buildPeekExportJson(PeekMessagesResponse response) {
    StringBuilder builder = new StringBuilder();
    builder.append("{\n");
    builder.append("  \"environmentId\": \"").append(escapeJson(response.environmentId())).append("\",\n");
    builder.append("  \"topicName\": \"").append(escapeJson(response.topicName())).append("\",\n");
    builder.append("  \"source\": \"PEEK\",\n");
    builder.append("  \"requestedCount\": ").append(response.requestedCount()).append(",\n");
    builder.append("  \"returnedCount\": ").append(response.returnedCount()).append(",\n");
    builder.append("  \"truncated\": ").append(response.truncated()).append(",\n");
    builder.append("  \"messages\": [\n");
    for (int index = 0; index < response.messages().size(); index++) {
      var message = response.messages().get(index);
      builder.append("    {\n");
      builder.append("      \"messageId\": \"").append(escapeJson(message.messageId())).append("\",\n");
      builder.append("      \"key\": \"").append(escapeJson(message.key())).append("\",\n");
      builder.append("      \"publishTime\": \"").append(escapeJson(String.valueOf(message.publishTime()))).append("\",\n");
      builder.append("      \"eventTime\": \"").append(escapeJson(String.valueOf(message.eventTime()))).append("\",\n");
      builder.append("      \"producerName\": \"").append(escapeJson(message.producerName())).append("\",\n");
      builder.append("      \"summary\": \"").append(escapeJson(message.summary())).append("\",\n");
      builder.append("      \"payload\": \"").append(escapeJson(message.payload())).append("\",\n");
      builder.append("      \"schemaVersion\": \"").append(escapeJson(message.schemaVersion())).append("\"\n");
      builder.append("    }");
      if (index < response.messages().size() - 1) {
        builder.append(",");
      }
      builder.append("\n");
    }
    builder.append("  ]\n");
    builder.append("}");
    return builder.toString();
  }

  private String buildConsumeExportJson(ConsumeMessagesResponse response) {
    StringBuilder builder = new StringBuilder();
    builder.append("{\n");
    builder.append("  \"environmentId\": \"").append(escapeJson(response.environmentId())).append("\",\n");
    builder.append("  \"topicName\": \"").append(escapeJson(response.topicName())).append("\",\n");
    builder.append("  \"source\": \"CONSUME\",\n");
    builder.append("  \"subscriptionName\": \"").append(escapeJson(response.subscriptionName())).append("\",\n");
    builder.append("  \"requestedCount\": ").append(response.requestedCount()).append(",\n");
    builder.append("  \"receivedCount\": ").append(response.receivedCount()).append(",\n");
    builder.append("  \"messages\": [\n");
    for (int index = 0; index < response.messages().size(); index++) {
      var message = response.messages().get(index);
      builder.append("    {\n");
      builder.append("      \"messageId\": \"").append(escapeJson(message.messageId())).append("\",\n");
      builder.append("      \"key\": \"").append(escapeJson(String.valueOf(message.key()))).append("\",\n");
      builder.append("      \"publishTime\": \"").append(escapeJson(String.valueOf(message.publishTime()))).append("\",\n");
      builder.append("      \"eventTime\": \"").append(escapeJson(String.valueOf(message.eventTime()))).append("\",\n");
      builder.append("      \"producerName\": \"").append(escapeJson(message.producerName())).append("\",\n");
      builder.append("      \"payload\": \"").append(escapeJson(message.payload())).append("\",\n");
      builder.append("      \"properties\": ").append(mapToJson(message.properties())).append("\n");
      builder.append("    }");
      if (index < response.messages().size() - 1) {
        builder.append(",");
      }
      builder.append("\n");
    }
    builder.append("  ]\n");
    builder.append("}");
    return builder.toString();
  }

  private String mapToJson(Map<String, String> values) {
    StringBuilder builder = new StringBuilder("{");
    int index = 0;
    for (Map.Entry<String, String> entry : values.entrySet()) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append("\"").append(escapeJson(entry.getKey())).append("\": ");
      builder.append("\"").append(escapeJson(entry.getValue())).append("\"");
      index++;
    }
    builder.append("}");
    return builder.toString();
  }

  private String exportFileName(TopicDetails topic, String source) {
    return topic.tenant() + "-" + topic.namespace() + "-" + topic.topic() + "-" + source.toLowerCase() + "-export.json";
  }

  private String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r");
  }

  private record SchemaValidation(List<String> warnings) {
  }

  private EnvironmentSnapshotRecord refreshSnapshot(EnvironmentRecord environment) {
    var snapshot = pulsarAdminGateway.syncMetadata(environment.toDetails());
    if (isMockMode()) {
      return mockEnvironmentStore.storeSnapshot(environment.id(), snapshot);
    }
    snapshotRepository.upsert(environment.id(), snapshot);
    return snapshotRepository.findByEnvironmentId(environment.id())
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environment.id()));
  }

  private EnvironmentSnapshotRecord loadSnapshot(String environmentId) {
    if (isMockMode()) {
      EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
      return refreshSnapshot(environment);
    }

    return snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));
  }

  private CatalogSummary toCatalogSummary(EnvironmentSnapshotRecord snapshot) {
    List<NamespaceSummary> namespaceSummaries = snapshot.namespaces().stream()
        .map(namespacePath -> {
          String[] segments = namespacePath.split("/", 2);
          if (segments.length != 2) {
            return null;
          }
          long topicCount = snapshot.topics().stream()
              .filter(topic -> topic.tenant().equals(segments[0]) && topic.namespace().equals(segments[1]))
              .count();
          return new NamespaceSummary(segments[0], segments[1], (int) topicCount);
        })
        .filter(java.util.Objects::nonNull)
        .sorted(Comparator.comparing(NamespaceSummary::tenant).thenComparing(NamespaceSummary::namespace))
        .toList();

    LinkedHashSet<String> allTenants = new LinkedHashSet<>(snapshot.tenants());
    snapshot.topics().forEach(topic -> allTenants.add(topic.tenant()));

    List<TenantSummary> tenantSummaries = allTenants.stream()
        .sorted()
        .map(tenant -> new TenantSummary(
            tenant,
            (int) namespaceSummaries.stream().filter(namespace -> namespace.tenant().equals(tenant)).count(),
            (int) snapshot.topics().stream().filter(topic -> topic.tenant().equals(tenant)).count()))
        .toList();

    return new CatalogSummary(snapshot.environmentId(), tenantSummaries, namespaceSummaries);
  }

  private TopicDetails requireTopic(EnvironmentSnapshotRecord snapshot, String topicName) {
    if (topicName == null || topicName.isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    String canonicalTopicName = PulsarTopicName.parse(topicName).canonicalFullName();

    return collapseTopics(snapshot.topics()).stream()
        .filter(item -> item.fullName().equals(canonicalTopicName))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + canonicalTopicName));
  }

  private TopicDetails requireTopicFromSnapshot(String environmentId, String topicName) {
    EnvironmentSnapshotRecord snapshot = loadSnapshot(environmentId);
    return requireTopic(snapshot, topicName);
  }

  private boolean isMockMode() {
    return "mock".equals(gatewayModeResolver.resolveMode());
  }

  private void requireNamespace(EnvironmentSnapshotRecord snapshot, String tenant, String namespace) {
    if (tenant == null || tenant.isBlank()) {
      throw new BadRequestException("A tenant is required.");
    }

    if (namespace == null || namespace.isBlank()) {
      throw new BadRequestException("A namespace is required.");
    }

    String fullNamespace = tenant + "/" + namespace;
    boolean exists = snapshot.namespaces().stream().anyMatch(item -> item.equals(fullNamespace));
    if (!exists) {
      throw new NotFoundException("Unknown namespace: " + fullNamespace);
    }
  }

  private void requireTenant(EnvironmentSnapshotRecord snapshot, String tenant) {
    if (tenant == null || tenant.isBlank()) {
      throw new BadRequestException("A tenant is required.");
    }

    boolean exists = snapshot.tenants().stream().anyMatch(item -> item.equals(tenant));
    if (!exists) {
      throw new NotFoundException("Unknown tenant: " + tenant);
    }
  }

  private List<TopicDetails> collapseTopics(List<TopicDetails> topics) {
    java.util.LinkedHashMap<String, java.util.List<TopicDetails>> groupedTopics = new java.util.LinkedHashMap<>();

    for (TopicDetails topic : topics) {
      String canonicalFullName = PulsarTopicName.parse(topic.fullName()).canonicalFullName();
      groupedTopics.computeIfAbsent(canonicalFullName, ignored -> new ArrayList<>()).add(topic);
    }

    return groupedTopics.entrySet().stream()
        .map(entry -> mergeTopicGroup(entry.getKey(), entry.getValue()))
        .sorted(Comparator.comparing(TopicDetails::namespace).thenComparing(TopicDetails::topic))
        .toList();
  }

  private TopicDetails mergeTopicGroup(String canonicalFullName, List<TopicDetails> topicGroup) {
    if (topicGroup.size() == 1 && topicGroup.get(0).fullName().equals(canonicalFullName)) {
      return topicGroup.get(0);
    }

    TopicDetails preferredTopic = topicGroup.stream()
        .filter(topic -> topic.fullName().equals(canonicalFullName))
        .findFirst()
        .orElse(topicGroup.get(0));

    PulsarTopicName canonicalName = PulsarTopicName.parse(canonicalFullName);
    int derivedPartitionCount = topicGroup.stream()
        .map(topic -> PulsarTopicName.parse(topic.fullName()).partitionIndex())
        .filter(java.util.Objects::nonNull)
        .mapToInt(index -> index + 1)
        .max()
        .orElse(0);
    int partitionCount = Math.max(
        preferredTopic.partitioned() ? Math.max(1, preferredTopic.partitions()) : 0,
        derivedPartitionCount);

    long backlog = 0;
    int producers = 0;
    int subscriptions = 0;
    int consumers = 0;
    double publishRateIn = 0;
    double dispatchRateOut = 0;
    double throughputIn = 0;
    double throughputOut = 0;
    long storageSize = 0;
    java.util.LinkedHashSet<String> subscriptionsSet = new java.util.LinkedHashSet<>();
    java.util.LinkedHashMap<String, com.pulsaradmin.shared.model.TopicPartitionSummary> partitionSummaries = new java.util.LinkedHashMap<>();
    boolean schemaPresent = false;
    TopicHealth health = preferredTopic.health();

    for (TopicDetails topic : topicGroup) {
      backlog += topic.stats().backlog();
      producers = Math.max(producers, topic.stats().producers());
      subscriptions = Math.max(subscriptions, topic.stats().subscriptions());
      consumers += topic.stats().consumers();
      publishRateIn += topic.stats().publishRateIn();
      dispatchRateOut += topic.stats().dispatchRateOut();
      throughputIn += topic.stats().throughputIn();
      throughputOut += topic.stats().throughputOut();
      storageSize += topic.stats().storageSize();
      subscriptionsSet.addAll(topic.subscriptions());
      schemaPresent = schemaPresent || safeSchema(topic).present();
      health = moreSevereHealth(health, topic.health());

      for (var partitionSummary : topic.partitionSummaries()) {
        partitionSummaries.putIfAbsent(partitionSummary.partitionName(), partitionSummary);
      }

      Integer partitionIndex = PulsarTopicName.parse(topic.fullName()).partitionIndex();
      if (partitionIndex != null && topic.partitionSummaries().isEmpty()) {
        partitionSummaries.putIfAbsent(
            canonicalFullName + "-partition-" + partitionIndex,
            new com.pulsaradmin.shared.model.TopicPartitionSummary(
                canonicalFullName + "-partition-" + partitionIndex,
                topic.stats().backlog(),
                topic.stats().consumers(),
                topic.stats().publishRateIn(),
                topic.stats().dispatchRateOut(),
                topic.health()));
      }
    }

    subscriptionsSet.removeIf(String::isBlank);
    List<String> sortedSubscriptions = subscriptionsSet.stream().sorted().toList();
    List<com.pulsaradmin.shared.model.TopicPartitionSummary> sortedPartitionSummaries = partitionSummaries.values().stream()
        .sorted(Comparator.comparing(com.pulsaradmin.shared.model.TopicPartitionSummary::partitionName))
        .toList();

    return new TopicDetails(
        canonicalFullName,
        canonicalName.tenant(),
        canonicalName.namespace(),
        canonicalName.topic(),
        partitionCount > 0 || !sortedPartitionSummaries.isEmpty(),
        Math.max(partitionCount, sortedPartitionSummaries.size()),
        health,
        new TopicStatsSummary(
            backlog,
            producers,
            Math.max(subscriptions, sortedSubscriptions.size()),
            consumers,
            publishRateIn,
            dispatchRateOut,
            throughputIn,
            throughputOut,
            storageSize),
        new SchemaSummary(
            safeSchema(preferredTopic).type(),
            safeSchema(preferredTopic).version(),
            safeSchema(preferredTopic).compatibility(),
            schemaPresent),
        preferredTopic.ownerTeam(),
        preferredTopic.notes(),
        sortedPartitionSummaries,
        sortedSubscriptions);
  }

  private TopicHealth moreSevereHealth(TopicHealth left, TopicHealth right) {
    return healthRank(right) > healthRank(left) ? right : left;
  }

  private int healthRank(TopicHealth health) {
    return switch (health) {
      case CRITICAL -> 4;
      case ATTENTION -> 3;
      case HEALTHY -> 2;
      case INACTIVE -> 1;
    };
  }

  private TopicPolicies sanitizeTopicPolicies(TopicPolicies policies) {
    if (policies == null) {
      throw new BadRequestException("Topic policies are required.");
    }

    validateNullableRange("Topic TTL", policies.ttlInSeconds(), 0, 31_536_000);
    validateNullableRange("Topic max producers", policies.maxProducers(), 0, 10_000);
    validateNullableRange("Topic max consumers", policies.maxConsumers(), 0, 10_000);
    validateNullableRange("Topic max subscriptions", policies.maxSubscriptions(), 0, 10_000);
    validateNullableRange("Topic retention time", policies.retentionTimeInMinutes(), 0, 10_000_000);
    validateNullableRange("Topic retention size", policies.retentionSizeInMb(), 0, 10_000_000);
    validateNullableLongRange("Topic compaction threshold", policies.compactionThresholdInBytes(), 0L, Long.MAX_VALUE);
    return policies;
  }

  private NamespacePolicies sanitizeNamespacePolicies(NamespacePolicies policies) {
    if (policies == null) {
      throw new BadRequestException("Namespace policies are required.");
    }

    validateNullableRange("Namespace message TTL", policies.messageTtlInSeconds(), 0, 31_536_000);
    validateNullableRange("Namespace retention time", policies.retentionTimeInMinutes(), 0, 10_000_000);
    validateNullableRange("Namespace retention size", policies.retentionSizeInMb(), 0, 10_000_000);
    validateNullableLongRange("Namespace backlog quota", policies.backlogQuotaLimitInBytes(), 0L, Long.MAX_VALUE);
    validateNullableRange("Namespace backlog quota time", policies.backlogQuotaLimitTimeInSeconds(), 0, 31_536_000);
    validateNullableRange("Namespace dispatch rate", policies.dispatchRatePerTopicInMsg(), 0, 1_000_000);
    validateNullableLongRange("Namespace dispatch bytes", policies.dispatchRatePerTopicInByte(), 0L, Long.MAX_VALUE);
    validateNullableRange("Namespace publish rate", policies.publishRateInMsg(), 0, 1_000_000);
    validateNullableLongRange("Namespace publish bytes", policies.publishRateInByte(), 0L, Long.MAX_VALUE);
    return policies;
  }

  private void validateNullableRange(String fieldName, Integer value, int min, int max) {
    if (value != null && (value < min || value > max)) {
      throw new BadRequestException(fieldName + " must be between " + min + " and " + max + ".");
    }
  }

  private void validateNullableLongRange(String fieldName, Long value, long min, long max) {
    if (value != null && (value < min || value > max)) {
      throw new BadRequestException(fieldName + " must be between " + min + " and " + max + ".");
    }
  }

  private TopicDetails fallbackCreatedTopic(CreateTopicRequest request) {
    return new TopicDetails(
        request.fullTopicName(),
        request.tenant(),
        request.namespace(),
        request.topic(),
        request.partitions() > 0,
        request.partitions(),
        com.pulsaradmin.shared.model.TopicHealth.INACTIVE,
        new com.pulsaradmin.shared.model.TopicStatsSummary(0, 0, 0, 0, 0, 0, 0, 0, 0),
        new com.pulsaradmin.shared.model.SchemaSummary("NONE", "-", "N/A", false),
        "Unassigned",
        request.notes() == null || request.notes().isBlank()
            ? "Topic created from the admin console. Fresh metadata will populate after the next sync."
            : request.notes().trim(),
        new ArrayList<>(),
        new ArrayList<>());
  }

  private TopicDetails fallbackTopicWithCreatedSubscription(TopicDetails topic, String subscriptionName) {
    ArrayList<String> subscriptions = new ArrayList<>(topic.subscriptions());
    subscriptions.add(subscriptionName);
    subscriptions.sort(String::compareTo);

    TopicStatsSummary stats = new TopicStatsSummary(
        topic.stats().backlog(),
        topic.stats().producers(),
        subscriptions.size(),
        topic.stats().consumers(),
        topic.stats().publishRateIn(),
        topic.stats().dispatchRateOut(),
        topic.stats().throughputIn(),
        topic.stats().throughputOut(),
        topic.stats().storageSize());

    return new TopicDetails(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        topic.partitioned(),
        topic.partitions(),
        topic.health(),
        stats,
        topic.schema(),
        topic.ownerTeam(),
        topic.notes(),
        topic.partitionSummaries(),
        subscriptions);
  }

  private TopicDetails fallbackTopicWithoutSubscription(TopicDetails topic, String subscriptionName) {
    ArrayList<String> subscriptions = new ArrayList<>(topic.subscriptions().stream()
        .filter(subscription -> !subscription.equals(subscriptionName))
        .toList());

    TopicStatsSummary stats = new TopicStatsSummary(
        topic.stats().backlog(),
        topic.stats().producers(),
        subscriptions.size(),
        topic.stats().consumers(),
        topic.stats().publishRateIn(),
        topic.stats().dispatchRateOut(),
        topic.stats().throughputIn(),
        topic.stats().throughputOut(),
        topic.stats().storageSize());

    return new TopicDetails(
        topic.fullName(),
        topic.tenant(),
        topic.namespace(),
        topic.topic(),
        topic.partitioned(),
        topic.partitions(),
        subscriptions.isEmpty() ? TopicHealth.INACTIVE : topic.health(),
        stats,
        new SchemaSummary(
            safeSchema(topic).type(),
            safeSchema(topic).version(),
            safeSchema(topic).compatibility(),
            safeSchema(topic).present()),
        topic.ownerTeam(),
        topic.notes(),
        topic.partitionSummaries(),
        subscriptions);
  }

  private SchemaSummary safeSchema(TopicDetails topic) {
    if (topic.schema() != null) {
      return topic.schema();
    }
    return new SchemaSummary("NONE", "-", "N/A", false);
  }

  private TopicDetails findTopicAfterRefresh(
      EnvironmentRecord environment,
      String topicName,
      TopicDetails fallback) {
    return refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(topicName))
        .findFirst()
        .orElse(fallback);
  }
}
