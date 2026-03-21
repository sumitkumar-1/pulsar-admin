package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.SubscriptionMutationResponse;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicDeleteRequest;
import com.pulsaradmin.shared.model.TopicDeleteResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateResponse;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/environments/{envId}/topics")
public class TopicController {
  private final PulsarCatalogService pulsarCatalogService;

  public TopicController(PulsarCatalogService pulsarCatalogService) {
    this.pulsarCatalogService = pulsarCatalogService;
  }

  @GetMapping
  public PagedResult<TopicListItem> getTopics(
      @PathVariable("envId") String envId,
      @RequestParam(name = "tenant", required = false) String tenant,
      @RequestParam(name = "namespace", required = false) String namespace,
      @RequestParam(name = "search", required = false) String search,
      @RequestParam(name = "page", defaultValue = "0") int page,
      @RequestParam(name = "pageSize", defaultValue = "25") int pageSize) {
    return pulsarCatalogService.getTopics(envId, tenant, namespace, search, page, pageSize);
  }

  @GetMapping("/detail")
  public TopicDetails getTopicDetails(
      @PathVariable("envId") String envId,
      @RequestParam("topic") String topicName) {
    return pulsarCatalogService.getTopicDetails(envId, topicName);
  }

  @PostMapping
  public TopicDetails createTopic(
      @PathVariable("envId") String envId,
      @Valid @RequestBody CreateTopicRequest request) {
    return pulsarCatalogService.createTopic(envId, request);
  }

  @PostMapping("/delete")
  public TopicDeleteResponse deleteTopic(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TopicDeleteRequest request) {
    return pulsarCatalogService.deleteTopic(envId, request);
  }

  @PostMapping("/subscriptions")
  public SubscriptionMutationResponse createSubscription(
      @PathVariable("envId") String envId,
      @Valid @RequestBody CreateSubscriptionRequest request) {
    return pulsarCatalogService.createSubscription(envId, request);
  }

  @DeleteMapping("/subscriptions")
  public SubscriptionMutationResponse deleteSubscription(
      @PathVariable("envId") String envId,
      @RequestParam("topic") String topicName,
      @RequestParam("subscription") String subscriptionName) {
    return pulsarCatalogService.deleteSubscription(envId, topicName, subscriptionName);
  }

  @GetMapping("/peek")
  public PeekMessagesResponse peekMessages(
      @PathVariable("envId") String envId,
      @RequestParam(name = "limit", defaultValue = "5") int limit,
      @RequestParam("topic") String topicName) {
    return pulsarCatalogService.peekMessages(envId, topicName, limit);
  }

  @PostMapping("/terminate")
  public TerminateTopicResponse terminateTopic(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TerminateTopicRequest request) {
    return pulsarCatalogService.terminateTopic(envId, request);
  }

  @GetMapping("/policies")
  public TopicPoliciesResponse getTopicPolicies(
      @PathVariable("envId") String envId,
      @RequestParam("topic") String topicName) {
    return pulsarCatalogService.getTopicPolicies(envId, topicName);
  }

  @PostMapping("/policies")
  public TopicPoliciesUpdateResponse updateTopicPolicies(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TopicPoliciesUpdateRequest request) {
    return pulsarCatalogService.updateTopicPolicies(envId, request);
  }

  @PostMapping("/publish")
  public PublishMessageResponse publishMessage(
      @PathVariable("envId") String envId,
      @Valid @RequestBody PublishMessageRequest request) {
    return pulsarCatalogService.publishMessage(envId, request);
  }

  @PostMapping("/consume")
  public ConsumeMessagesResponse consumeMessages(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ConsumeMessagesRequest request) {
    return pulsarCatalogService.consumeMessages(envId, request);
  }

  @PostMapping("/reset-cursor")
  public ResetCursorResponse resetCursor(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ResetCursorRequest request) {
    return pulsarCatalogService.resetCursor(envId, request);
  }

  @PostMapping("/skip-messages")
  public SkipMessagesResponse skipMessages(
      @PathVariable("envId") String envId,
      @Valid @RequestBody SkipMessagesRequest request) {
    return pulsarCatalogService.skipMessages(envId, request);
  }

  @PostMapping("/unload")
  public UnloadTopicResponse unloadTopic(
      @PathVariable("envId") String envId,
      @Valid @RequestBody UnloadTopicRequest request) {
    return pulsarCatalogService.unloadTopic(envId, request);
  }

  @PostMapping("/replay-copy")
  public ReplayCopyJobStatusResponse createReplayCopyJob(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ReplayCopyJobRequest request) {
    return pulsarCatalogService.createReplayCopyJob(envId, request);
  }

  @GetMapping("/jobs/{jobId}")
  public ReplayCopyJobStatusResponse getReplayCopyJob(
      @PathVariable("envId") String envId,
      @PathVariable("jobId") String jobId) {
    return pulsarCatalogService.getReplayCopyJob(envId, jobId);
  }
}
