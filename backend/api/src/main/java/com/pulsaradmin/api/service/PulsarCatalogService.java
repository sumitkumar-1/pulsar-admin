package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
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

  public PeekMessagesResponse peekMessages(String environmentId, String topicName, int limit) {
    return environmentCatalogService.peekMessages(environmentId, topicName, limit);
  }

  public ResetCursorResponse resetCursor(String environmentId, ResetCursorRequest request) {
    return environmentCatalogService.resetCursor(environmentId, request);
  }

  public SkipMessagesResponse skipMessages(String environmentId, SkipMessagesRequest request) {
    return environmentCatalogService.skipMessages(environmentId, request);
  }
}
