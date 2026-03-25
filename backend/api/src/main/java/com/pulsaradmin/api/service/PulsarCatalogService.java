package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.CatalogMutationResponse;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.ClearBacklogRequest;
import com.pulsaradmin.shared.model.ClearBacklogResponse;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.ExportMessagesRequest;
import com.pulsaradmin.shared.model.ExportMessagesResponse;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespaceDeleteRequest;
import com.pulsaradmin.shared.model.NamespaceMutationResponse;
import com.pulsaradmin.shared.model.NamespacePoliciesResponse;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.NamespaceYamlCurrentResponse;
import com.pulsaradmin.shared.model.PagedResult;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PlatformArtifactDeleteRequest;
import com.pulsaradmin.shared.model.PlatformArtifactDetails;
import com.pulsaradmin.shared.model.PlatformArtifactMutationRequest;
import com.pulsaradmin.shared.model.PlatformArtifactMutationResponse;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ReplayCopyJobRequest;
import com.pulsaradmin.shared.model.ReplayCopyJobEventResponse;
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
import com.pulsaradmin.shared.model.TenantDeleteRequest;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantMutationResponse;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicDetails;
import com.pulsaradmin.shared.model.TopicListItem;
import com.pulsaradmin.shared.model.TopicPoliciesResponse;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateRequest;
import com.pulsaradmin.shared.model.TopicPoliciesUpdateResponse;
import com.pulsaradmin.shared.model.TopicDeleteRequest;
import com.pulsaradmin.shared.model.TopicDeleteResponse;
import com.pulsaradmin.shared.model.TenantYamlApplyRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyResponse;
import com.pulsaradmin.shared.model.TenantYamlPreviewRequest;
import com.pulsaradmin.shared.model.TenantYamlPreviewResponse;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class PulsarCatalogService {
  private final EnvironmentManagementService environmentManagementService;
  private final EnvironmentCatalogService environmentCatalogService;
  private final ReplayCopyJobService replayCopyJobService;
  private final TenantYamlSyncService tenantYamlSyncService;

  public PulsarCatalogService(
      EnvironmentManagementService environmentManagementService,
      EnvironmentCatalogService environmentCatalogService,
      ReplayCopyJobService replayCopyJobService,
      TenantYamlSyncService tenantYamlSyncService) {
    this.environmentManagementService = environmentManagementService;
    this.environmentCatalogService = environmentCatalogService;
    this.replayCopyJobService = replayCopyJobService;
    this.tenantYamlSyncService = tenantYamlSyncService;
  }

  public EnvironmentHealth getEnvironmentHealth(String environmentId) {
    return environmentCatalogService.getEnvironmentHealth(environmentId);
  }

  public CatalogSummary getCatalogSummary(String environmentId) {
    return environmentCatalogService.getCatalogSummary(environmentId);
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

  public CatalogMutationResponse createTenant(String environmentId, CreateTenantRequest request) {
    return environmentCatalogService.createTenant(environmentId, request);
  }

  public CatalogMutationResponse createNamespace(String environmentId, CreateNamespaceRequest request) {
    return environmentCatalogService.createNamespace(environmentId, request);
  }

  public TenantDetails getTenantDetails(String environmentId, String tenant) {
    return environmentCatalogService.getTenantDetails(environmentId, tenant);
  }

  public TenantMutationResponse updateTenant(String environmentId, TenantUpdateRequest request) {
    return environmentCatalogService.updateTenant(environmentId, request);
  }

  public TenantMutationResponse deleteTenant(String environmentId, TenantDeleteRequest request) {
    return environmentCatalogService.deleteTenant(environmentId, request);
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

  public TerminateTopicResponse terminateTopic(String environmentId, TerminateTopicRequest request) {
    return environmentCatalogService.terminateTopic(environmentId, request);
  }

  public TopicPoliciesResponse getTopicPolicies(String environmentId, String topicName) {
    return environmentCatalogService.getTopicPolicies(environmentId, topicName);
  }

  public TopicPoliciesUpdateResponse updateTopicPolicies(String environmentId, TopicPoliciesUpdateRequest request) {
    return environmentCatalogService.updateTopicPolicies(environmentId, request);
  }

  public NamespaceDetails getNamespaceDetails(String environmentId, String tenant, String namespace) {
    return environmentCatalogService.getNamespaceDetails(environmentId, tenant, namespace);
  }

  public NamespacePoliciesResponse updateNamespacePolicies(String environmentId, NamespacePoliciesUpdateRequest request) {
    return environmentCatalogService.updateNamespacePolicies(environmentId, request);
  }

  public NamespaceMutationResponse deleteNamespace(String environmentId, NamespaceDeleteRequest request) {
    return environmentCatalogService.deleteNamespace(environmentId, request);
  }

  public PublishMessageResponse publishMessage(String environmentId, PublishMessageRequest request) {
    return environmentCatalogService.publishMessage(environmentId, request);
  }

  public ConsumeMessagesResponse consumeMessages(String environmentId, ConsumeMessagesRequest request) {
    return environmentCatalogService.consumeMessages(environmentId, request);
  }

  public ExportMessagesResponse exportMessages(String environmentId, ExportMessagesRequest request) {
    return environmentCatalogService.exportMessages(environmentId, request);
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

  public TopicDeleteResponse deleteTopic(String environmentId, TopicDeleteRequest request) {
    return environmentCatalogService.deleteTopic(environmentId, request);
  }

  public ReplayCopyJobStatusResponse createReplayCopyJob(String environmentId, ReplayCopyJobRequest request) {
    return replayCopyJobService.createJob(environmentId, request);
  }

  public ReplayCopyJobStatusResponse createReplayCopyJob(
      String environmentId,
      ReplayCopyJobRequest request,
      ReplayCopyCriteriaInput criteriaInput) {
    return replayCopyJobService.createJob(environmentId, request, criteriaInput);
  }

  public ReplayCopyJobStatusResponse getReplayCopyJob(String environmentId, String jobId) {
    return replayCopyJobService.getJob(environmentId, jobId);
  }

  public List<ReplayCopyJobEventResponse> getReplayCopyJobEvents(String environmentId, String jobId) {
    return replayCopyJobService.getJobEvents(environmentId, jobId);
  }

  public ReplayCopySearchExportResponse getReplayCopyJobSearchExport(String environmentId, String jobId) {
    return replayCopyJobService.getSearchExport(environmentId, jobId);
  }

  public ClearBacklogResponse clearBacklog(String environmentId, ClearBacklogRequest request) {
    return environmentCatalogService.clearBacklog(environmentId, request);
  }

  public TenantYamlPreviewResponse validateYamlPreview(String environmentId, TenantYamlPreviewRequest request) {
    return tenantYamlSyncService.preview(environmentId, request, false);
  }

  public TenantYamlPreviewResponse previewYaml(String environmentId, TenantYamlPreviewRequest request) {
    return tenantYamlSyncService.preview(environmentId, request, true);
  }

  public TenantYamlApplyResponse applyYaml(String environmentId, TenantYamlApplyRequest request) {
    return tenantYamlSyncService.apply(environmentId, request);
  }

  public NamespaceYamlCurrentResponse getCurrentNamespaceYaml(
      String environmentId,
      String tenant,
      String namespace) {
    return tenantYamlSyncService.currentYaml(environmentId, tenant, namespace);
  }

  public PlatformSummary getPlatformSummary(String environmentId) {
    return environmentCatalogService.getPlatformSummary(environmentId);
  }

  public PlatformArtifactDetails getPlatformArtifactDetails(
      String environmentId,
      String artifactType,
      String tenant,
      String namespace,
      String name) {
    return environmentCatalogService.getPlatformArtifactDetails(environmentId, artifactType, tenant, namespace, name);
  }

  public PlatformArtifactMutationResponse upsertPlatformArtifact(
      String environmentId,
      PlatformArtifactMutationRequest request) {
    return environmentCatalogService.upsertPlatformArtifact(environmentId, request);
  }

  public PlatformArtifactMutationResponse deletePlatformArtifact(
      String environmentId,
      PlatformArtifactDeleteRequest request) {
    return environmentCatalogService.deletePlatformArtifact(environmentId, request);
  }

  public SchemaDetails getSchemaDetails(String environmentId, String topicName) {
    return environmentCatalogService.getSchemaDetails(environmentId, topicName);
  }

  public SchemaMutationResponse upsertSchema(String environmentId, SchemaUpdateRequest request) {
    return environmentCatalogService.upsertSchema(environmentId, request);
  }

  public SchemaMutationResponse deleteSchema(String environmentId, SchemaDeleteRequest request) {
    return environmentCatalogService.deleteSchema(environmentId, request);
  }
}
