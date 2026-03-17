package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import org.springframework.stereotype.Service;

@Service
public class PulsarCatalogService {
  private final EnvironmentManagementService environmentManagementService;
  private final EnvironmentCatalogService environmentCatalogService;

  public PulsarCatalogService(
      EnvironmentManagementService environmentManagementService,
      EnvironmentCatalogService environmentCatalogService) {
    this.environmentManagementService = environmentManagementService;
    this.environmentCatalogService = environmentCatalogService;
  }

  public EnvironmentHealth getEnvironmentHealth(String environmentId) {
    return environmentCatalogService.getEnvironmentHealth(environmentId);
  }

  public PagedResult<TopicListItem> getTopics(
      String environmentId,
      String tenant,
      String namespace,
      String search,
      int page,
      int pageSize) {
    return environmentCatalogService.getTopics(environmentId, tenant, namespace, search, page, pageSize);
  }

  public TopicDetails getTopicDetails(String environmentId, String topicName) {
    return environmentCatalogService.getTopicDetails(environmentId, topicName);
  }
}
