package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.SubscriptionMutationResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import org.springframework.stereotype.Service;

@Service
public class PulsarCatalogService {
  private final EnvironmentManagementService environmentManagementService;
  private final EnvironmentCatalogService environmentCatalogService;
  private final ReplayCopyJobService replayCopyJobService;

  public PulsarCatalogService(
      EnvironmentManagementService environmentManagementService,
      EnvironmentCatalogService environmentCatalogService,
      ReplayCopyJobService replayCopyJobService) {
    this.environmentManagementService = environmentManagementService;
    this.environmentCatalogService = environmentCatalogService;
    this.replayCopyJobService = replayCopyJobService;
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

  public TopicDetails createTopic(String environmentId, CreateTopicRequest request) {
    return environmentCatalogService.createTopic(environmentId, request);
  }

  public SubscriptionMutationResponse createSubscription(String environmentId, CreateSubscriptionRequest request) {
    return environmentCatalogService.createSubscription(environmentId, request);
  }

  public SubscriptionMutationResponse deleteSubscription(String environmentId, String topicName, String subscriptionName) {
    return environmentCatalogService.deleteSubscription(environmentId, topicName, subscriptionName);
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

  public UnloadTopicResponse unloadTopic(String environmentId, UnloadTopicRequest request) {
    return environmentCatalogService.unloadTopic(environmentId, request);
  }

  public ReplayCopyJobStatusResponse createReplayCopyJob(String environmentId, ReplayCopyJobRequest request) {
    return replayCopyJobService.createJob(environmentId, request);
  }

  public ReplayCopyJobStatusResponse getReplayCopyJob(String environmentId, String jobId) {
    return replayCopyJobService.getJob(environmentId, jobId);
  }
}
