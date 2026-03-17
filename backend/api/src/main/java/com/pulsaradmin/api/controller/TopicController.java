package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import jakarta.validation.Valid;
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

  @GetMapping("/peek")
  public PeekMessagesResponse peekMessages(
      @PathVariable("envId") String envId,
      @RequestParam(name = "limit", defaultValue = "5") int limit,
      @RequestParam("topic") String topicName) {
    return pulsarCatalogService.peekMessages(envId, topicName, limit);
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
