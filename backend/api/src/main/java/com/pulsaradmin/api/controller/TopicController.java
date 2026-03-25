package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.api.service.ReplayCopyCriteriaInput;
import com.pulsaradmin.shared.model.ClearBacklogRequest;
import com.pulsaradmin.shared.model.ClearBacklogResponse;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ExportMessagesRequest;
import com.pulsaradmin.shared.model.ExportMessagesResponse;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobEventResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopySearchExportResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobStatusResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SchemaDeleteRequest;
import com.pulsaradmin.shared.model.SchemaDetails;
import com.pulsaradmin.shared.model.SchemaMutationResponse;
import com.pulsaradmin.shared.model.SchemaUpdateRequest;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

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

  @GetMapping("/schema")
  public SchemaDetails getSchemaDetails(
      @PathVariable("envId") String envId,
      @RequestParam("topic") String topicName) {
    return pulsarCatalogService.getSchemaDetails(envId, topicName);
  }

  @PostMapping("/schema")
  public SchemaMutationResponse upsertSchema(
      @PathVariable("envId") String envId,
      @Valid @RequestBody SchemaUpdateRequest request) {
    return pulsarCatalogService.upsertSchema(envId, request);
  }

  @PostMapping("/schema/delete")
  public SchemaMutationResponse deleteSchema(
      @PathVariable("envId") String envId,
      @Valid @RequestBody SchemaDeleteRequest request) {
    return pulsarCatalogService.deleteSchema(envId, request);
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

  @PostMapping("/export")
  public ExportMessagesResponse exportMessages(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ExportMessagesRequest request) {
    return pulsarCatalogService.exportMessages(envId, request);
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

  @PostMapping(value = "/replay-copy", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ReplayCopyJobStatusResponse createReplayCopyJob(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ReplayCopyJobRequest request) {
    return pulsarCatalogService.createReplayCopyJob(envId, request);
  }

  @PostMapping(value = "/replay-copy", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ReplayCopyJobStatusResponse createReplayCopyJobMultipart(
      @PathVariable("envId") String envId,
      @Valid @RequestPart("request") ReplayCopyJobRequest request,
      @RequestPart(name = "idsFile", required = false) MultipartFile idsFile) {
    return pulsarCatalogService.createReplayCopyJob(envId, request, parseCriteria(idsFile));
  }

  @GetMapping("/jobs/{jobId}")
  public ReplayCopyJobStatusResponse getReplayCopyJob(
      @PathVariable("envId") String envId,
      @PathVariable("jobId") String jobId) {
    return pulsarCatalogService.getReplayCopyJob(envId, jobId);
  }

  @GetMapping("/jobs/{jobId}/events")
  public List<ReplayCopyJobEventResponse> getReplayCopyJobEvents(
      @PathVariable("envId") String envId,
      @PathVariable("jobId") String jobId) {
    return pulsarCatalogService.getReplayCopyJobEvents(envId, jobId);
  }

  @GetMapping("/jobs/{jobId}/search-export")
  public ReplayCopySearchExportResponse getReplayCopyJobSearchExport(
      @PathVariable("envId") String envId,
      @PathVariable("jobId") String jobId) {
    return pulsarCatalogService.getReplayCopyJobSearchExport(envId, jobId);
  }

  @PostMapping("/clear-backlog")
  public ClearBacklogResponse clearBacklog(
      @PathVariable("envId") String envId,
      @Valid @RequestBody ClearBacklogRequest request) {
    return pulsarCatalogService.clearBacklog(envId, request);
  }

  private ReplayCopyCriteriaInput parseCriteria(MultipartFile idsFile) {
    if (idsFile == null || idsFile.isEmpty()) {
      return ReplayCopyCriteriaInput.empty();
    }
    String filename = idsFile.getOriginalFilename();
    String contentType = idsFile.getContentType();
    boolean looksLikeCsv = (filename != null && filename.toLowerCase().endsWith(".csv"))
        || (contentType != null && contentType.toLowerCase().contains("csv"));
    if (!looksLikeCsv) {
      throw new BadRequestException("idsFile must be a CSV file.");
    }

    List<String> headers = new ArrayList<>();
    List<Map<String, String>> rows = new ArrayList<>();
    List<String> errors = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(idsFile.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      boolean headerRead = false;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        List<String> columns = parseCsvColumns(trimmed);
        if (!headerRead) {
          headers.addAll(columns.stream().map(String::trim).toList());
          headerRead = true;
          if (headers.isEmpty()) {
            errors.add("CSV header row is empty.");
          }
          if (headers.stream().anyMatch(String::isBlank)) {
            errors.add("CSV header fields must be non-empty.");
          }
          if (new LinkedHashSet<>(headers).size() != headers.size()) {
            errors.add("CSV header fields must be unique.");
          }
          continue;
        }

        if (columns.size() != headers.size()) {
          errors.add("CSV row has " + columns.size() + " values but expected " + headers.size() + ".");
          continue;
        }

        Map<String, String> row = new LinkedHashMap<>();
        for (int index = 0; index < headers.size(); index++) {
          String header = headers.get(index).trim();
          String value = columns.get(index).trim();
          if (!header.isBlank() && !value.isBlank()) {
            row.put(header, value);
          }
        }
        if (!row.isEmpty()) {
          rows.add(row);
        }
      }
    } catch (IOException exception) {
      throw new BadRequestException("Unable to parse IDs file: " + exception.getMessage());
    }
    if (headers.isEmpty()) {
      errors.add("CSV must include a header row.");
    }
    if (rows.isEmpty() && errors.isEmpty()) {
      errors.add("CSV must include at least one criteria row.");
    }
    return new ReplayCopyCriteriaInput(headers, rows, errors);
  }

  private List<String> parseCsvColumns(String line) {
    List<String> columns = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;
    for (int index = 0; index < line.length(); index++) {
      char character = line.charAt(index);
      if (character == '"') {
        if (inQuotes && index + 1 < line.length() && line.charAt(index + 1) == '"') {
          current.append('"');
          index++;
          continue;
        }
        inQuotes = !inQuotes;
        continue;
      }
      if (character == ',' && !inQuotes) {
        columns.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(character);
    }
    columns.add(current.toString());
    return columns;
  }
}
