package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import java.util.ArrayList;
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

  private void requireEnvironment(String environmentId) {
    requireEnvironmentRecord(environmentId);
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

  private EnvironmentSnapshotRecord refreshSnapshot(EnvironmentRecord environment) {
    var snapshot = pulsarAdminGateway.syncMetadata(environment.toDetails());
    snapshotRepository.upsert(environment.id(), snapshot);
    return snapshotRepository.findByEnvironmentId(environment.id())
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environment.id()));
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
}
