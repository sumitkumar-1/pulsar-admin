package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
}
