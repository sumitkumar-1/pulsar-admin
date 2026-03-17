package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
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

  private void requireEnvironment(String environmentId) {
    requireEnvironmentRecord(environmentId);
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
}
