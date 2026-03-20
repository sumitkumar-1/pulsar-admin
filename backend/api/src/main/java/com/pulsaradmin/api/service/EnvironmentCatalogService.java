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
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.NamespacePoliciesResponse;
import com.pulsaradmin.shared.model.NamespaceSummary;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TenantSummary;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.SubscriptionMutationResponse;
import com.pulsaradmin.shared.model.TopicHealth;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicStatsSummary;
import com.pulsaradmin.shared.model.SchemaSummary;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class EnvironmentCatalogService {
  private static final Set<Integer> PAGE_SIZES = Set.of(10, 25, 50, 100);

  private final EnvironmentRepository environmentRepository;
  private final EnvironmentSnapshotRepository snapshotRepository;
  private final PulsarAdminGateway pulsarAdminGateway;

  public EnvironmentCatalogService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      PulsarAdminGateway pulsarAdminGateway) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.pulsarAdminGateway = pulsarAdminGateway;
  }

  public EnvironmentHealth getEnvironmentHealth(String environmentId) {
    requireEnvironment(environmentId);

    return snapshotRepository.findByEnvironmentId(environmentId)
        .map(EnvironmentSnapshotRecord::toHealth)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));
  }

  public CatalogSummary getCatalogSummary(String environmentId) {
    requireEnvironment(environmentId);

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    return toCatalogSummary(snapshot);
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

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    var filtered = snapshot.topics().stream()
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

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    return snapshot.topics().stream()
        .filter(topic -> topic.fullName().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + topicName));
  }

  public TopicDetails createTopic(String environmentId, CreateTopicRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));
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
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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

  public SubscriptionMutationResponse createSubscription(String environmentId, CreateSubscriptionRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    boolean exists = snapshot.topics().stream().anyMatch(topic -> topic.fullName().equals(topicName));

    if (!exists) {
      throw new NotFoundException("Unknown topic: " + topicName);
    }

    return pulsarAdminGateway.peekMessages(environment.toDetails(), topicName, limit);
  }

  public TerminateTopicResponse terminateTopic(String environmentId, TerminateTopicRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    TopicDetails existingTopic = requireTopicFromSnapshot(environmentId, request.topicName());

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

    TopicDetails updatedTopic = refreshSnapshot(environment).topics().stream()
        .filter(item -> item.fullName().equals(request.topicName()))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + request.topicName()));

    return new TopicPoliciesUpdateResponse(
        environmentId,
        request.topicName(),
        updatedPolicies,
        "Updated policies for " + request.topicName() + ".",
        updatedTopic);
  }

  public NamespaceDetails getNamespaceDetails(String environmentId, String tenant, String namespace) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

    requireNamespace(snapshot, tenant, namespace);

    NamespaceDetails gatewayDetails = pulsarAdminGateway.getNamespaceDetails(environment.toDetails(), tenant, namespace);
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
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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

  public PublishMessageResponse publishMessage(String environmentId, PublishMessageRequest request) {
    EnvironmentRecord environment = requireEnvironmentRecord(environmentId);
    requireTopicFromSnapshot(environmentId, request.topicName());

    if (request.payload() == null || request.payload().isBlank()) {
      throw new BadRequestException("A payload is required.");
    }

    if (request.payload().length() > 20_000) {
      throw new BadRequestException("Payloads are limited to 20,000 characters in the test console.");
    }

    if (request.reason() == null || request.reason().isBlank()) {
      throw new BadRequestException("A reason is required when publishing a test message.");
    }

    return pulsarAdminGateway.publishMessage(environment.toDetails(), request);
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

    return pulsarAdminGateway.consumeMessages(environment.toDetails(), request);
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

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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

    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));

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

  private void requireEnvironment(String environmentId) {
    requireEnvironmentRecord(environmentId);
  }

  void deleteTopicForSync(EnvironmentRecord environment, String topicName) {
    pulsarAdminGateway.deleteTopic(environment.toDetails(), topicName);
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

  private EnvironmentSnapshotRecord refreshSnapshot(EnvironmentRecord environment) {
    var snapshot = pulsarAdminGateway.syncMetadata(environment.toDetails());
    snapshotRepository.upsert(environment.id(), snapshot);
    return snapshotRepository.findByEnvironmentId(environment.id())
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environment.id()));
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

    return snapshot.topics().stream()
        .filter(item -> item.fullName().equals(topicName))
        .findFirst()
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + topicName));
  }

  private TopicDetails requireTopicFromSnapshot(String environmentId, String topicName) {
    EnvironmentSnapshotRecord snapshot = snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));
    return requireTopic(snapshot, topicName);
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
            topic.schema().type(),
            topic.schema().version(),
            topic.schema().compatibility(),
            topic.schema().present()),
        topic.ownerTeam(),
        topic.notes(),
        topic.partitionSummaries(),
        subscriptions);
  }
}
