package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class PulsarCatalogService {
  private static final Set<Integer> PAGE_SIZES = Set.of(10, 25, 50, 100);

  private final PulsarAdminGateway gateway;

  public PulsarCatalogService(PulsarAdminGateway gateway) {
    this.gateway = gateway;
  }

  public List<EnvironmentSummary> getEnvironments() {
    return gateway.getEnvironments();
  }

  public EnvironmentHealth getEnvironmentHealth(String environmentId) {
    return gateway.getEnvironmentHealth(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));
  }

  public PagedResult<TopicListItem> getTopics(
      String environmentId,
      String tenant,
      String namespace,
      String search,
      int page,
      int pageSize) {
    ensureEnvironment(environmentId);
    validatePaging(page, pageSize);

    return gateway.getTopics(environmentId, tenant, namespace, search, page, pageSize);
  }

  public TopicDetails getTopicDetails(String environmentId, String topicName) {
    ensureEnvironment(environmentId);

    if (topicName == null || topicName.isBlank()) {
      throw new BadRequestException("A topic name is required.");
    }

    return gateway.getTopicDetails(environmentId, topicName)
        .orElseThrow(() -> new NotFoundException("Unknown topic: " + topicName));
  }

  private void ensureEnvironment(String environmentId) {
    boolean exists = gateway.getEnvironments().stream()
        .anyMatch(environment -> environment.id().equals(environmentId));

    if (!exists) {
      throw new NotFoundException("Unknown environment: " + environmentId);
    }
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
