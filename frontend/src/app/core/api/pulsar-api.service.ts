import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Observable, switchMap, tap } from 'rxjs';
import { DemoModeService } from '../demo-mode.service';
import {
  CatalogMutationResponse,
  CatalogSummary,
  ConsumeMessagesRequest,
  ConsumeMessagesResponse,
  CreateNamespaceRequest,
  CreateSubscriptionRequest,
  CreateTenantRequest,
  CreateTopicRequest,
  EnvironmentConnectionTestResult,
  EnvironmentDetails,
  EnvironmentHealth,
  EnvironmentSyncStatus,
  EnvironmentSummary,
  EnvironmentUpsertRequest,
  ExportMessagesRequest,
  ExportMessagesResponse,
  NamespaceDetails,
  NamespaceDeleteRequest,
  NamespaceMutationResponse,
  NamespacePoliciesResponse,
  NamespacePoliciesUpdateRequest,
  NamespaceYamlCurrentResponse,
  PeekMessagesResponse,
  PlatformArtifactDeleteRequest,
  PlatformArtifactDetails,
  PlatformArtifactMutationRequest,
  PlatformArtifactMutationResponse,
  PlatformSummary,
  PublishMessageRequest,
  PublishMessageResponse,
  ReplayCopyJobRequest,
  ReplayCopyJobStatusResponse,
  ResetCursorRequest,
  ResetCursorResponse,
  SchemaDeleteRequest,
  SchemaDetails,
  SchemaMutationResponse,
  SchemaUpdateRequest,
  SkipMessagesRequest,
  SkipMessagesResponse,
  SubscriptionMutationResponse,
  TenantDeleteRequest,
  TenantDetails,
  TenantMutationResponse,
  TenantUpdateRequest,
  TenantYamlApplyRequest,
  TenantYamlApplyResponse,
  TenantYamlPreviewRequest,
  TenantYamlPreviewResponse,
  TerminateTopicRequest,
  TerminateTopicResponse,
  TopicDeleteRequest,
  TopicDeleteResponse,
  TopicDetails,
  TopicPage,
  TopicPoliciesResponse,
  TopicPoliciesUpdateRequest,
  TopicPoliciesUpdateResponse,
  UnloadTopicRequest,
  UnloadTopicResponse
} from '../models/api.models';

export interface TopicQuery {
  tenant?: string;
  namespace?: string;
  search?: string;
  page?: number;
  pageSize?: number;
}

@Injectable({ providedIn: 'root' })
export class PulsarApiService {
  private readonly http = inject(HttpClient);
  private readonly demoMode = inject(DemoModeService);
  private readonly baseUrl = '/api/v1';
  private readonly environmentRefresh$ = new BehaviorSubject<void>(undefined);

  getEnvironments(): Observable<EnvironmentSummary[]> {
    return this.environmentRefresh$.pipe(
      switchMap(() => this.http.get<EnvironmentSummary[]>(`${this.baseUrl}/environments`, {
        params: this.demoMode.appendHttpParams(new HttpParams())
      }))
    );
  }

  getEnvironment(environmentId: string): Observable<EnvironmentDetails> {
    return this.http.get<EnvironmentDetails>(`${this.baseUrl}/environments/${environmentId}`, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
  }

  createEnvironment(request: EnvironmentUpsertRequest): Observable<EnvironmentDetails> {
    return this.http.post<EnvironmentDetails>(`${this.baseUrl}/environments`, request, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    }).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  updateEnvironment(environmentId: string, request: EnvironmentUpsertRequest): Observable<EnvironmentDetails> {
    return this.http.patch<EnvironmentDetails>(`${this.baseUrl}/environments/${environmentId}`, request, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    }).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  testEnvironmentConnection(environmentId: string): Observable<EnvironmentConnectionTestResult> {
    return this.http.post<EnvironmentConnectionTestResult>(
      `${this.baseUrl}/environments/${environmentId}/test-connection`,
      {},
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    ).pipe(tap(() => this.refreshEnvironments()));
  }

  syncEnvironment(environmentId: string): Observable<EnvironmentSyncStatus> {
    return this.http.post<EnvironmentSyncStatus>(
      `${this.baseUrl}/environments/${environmentId}/sync`,
      {},
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    ).pipe(tap(() => this.refreshEnvironments()));
  }

  getEnvironmentSyncStatus(environmentId: string): Observable<EnvironmentSyncStatus> {
    return this.http.get<EnvironmentSyncStatus>(`${this.baseUrl}/environments/${environmentId}/sync-status`, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
  }

  deleteEnvironment(environmentId: string): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/environments/${environmentId}`, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    }).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  getEnvironmentHealth(environmentId: string): Observable<EnvironmentHealth> {
    return this.http.get<EnvironmentHealth>(`${this.baseUrl}/environments/${environmentId}/health`, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
  }

  getCatalogSummary(environmentId: string): Observable<CatalogSummary> {
    return this.http.get<CatalogSummary>(`${this.baseUrl}/environments/${environmentId}/catalog`, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
  }

  createTenant(environmentId: string, request: CreateTenantRequest): Observable<CatalogMutationResponse> {
    return this.http.post<CatalogMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/tenants`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getTenantDetails(environmentId: string, tenant: string): Observable<TenantDetails> {
    return this.http.get<TenantDetails>(`${this.baseUrl}/environments/${environmentId}/tenants/detail`, {
      params: this.demoMode.appendHttpParams(new HttpParams().set('tenant', tenant))
    });
  }

  updateTenant(environmentId: string, request: TenantUpdateRequest): Observable<TenantMutationResponse> {
    return this.http.post<TenantMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/tenants/update`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  deleteTenant(environmentId: string, request: TenantDeleteRequest): Observable<TenantMutationResponse> {
    return this.http.post<TenantMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/tenants/delete`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  createNamespace(environmentId: string, request: CreateNamespaceRequest): Observable<CatalogMutationResponse> {
    return this.http.post<CatalogMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getTopics(environmentId: string, query: TopicQuery): Observable<TopicPage> {
    let params = new HttpParams()
      .set('page', String(query.page ?? 0))
      .set('pageSize', String(query.pageSize ?? 25));

    if (query.tenant) {
      params = params.set('tenant', query.tenant);
    }

    if (query.namespace) {
      params = params.set('namespace', query.namespace);
    }

    if (query.search) {
      params = params.set('search', query.search);
    }

    return this.http.get<TopicPage>(`${this.baseUrl}/environments/${environmentId}/topics`, {
      params: this.demoMode.appendHttpParams(params)
    });
  }

  getNamespaceDetails(environmentId: string, tenant: string, namespace: string): Observable<NamespaceDetails> {
    return this.http.get<NamespaceDetails>(`${this.baseUrl}/environments/${environmentId}/namespaces/detail`, {
      params: this.demoMode.appendHttpParams(new HttpParams().set('tenant', tenant).set('namespace', namespace))
    });
  }

  updateNamespacePolicies(
    environmentId: string,
    request: NamespacePoliciesUpdateRequest
  ): Observable<NamespacePoliciesResponse> {
    return this.http.post<NamespacePoliciesResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/policies`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  deleteNamespace(environmentId: string, request: NamespaceDeleteRequest): Observable<NamespaceMutationResponse> {
    return this.http.post<NamespaceMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/delete`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  createTopic(environmentId: string, request: CreateTopicRequest): Observable<TopicDetails> {
    return this.http.post<TopicDetails>(`${this.baseUrl}/environments/${environmentId}/topics`, request, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
  }

  deleteTopic(environmentId: string, request: TopicDeleteRequest): Observable<TopicDeleteResponse> {
    return this.http.post<TopicDeleteResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/delete`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  createSubscription(
    environmentId: string,
    request: CreateSubscriptionRequest
  ): Observable<SubscriptionMutationResponse> {
    return this.http.post<SubscriptionMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/subscriptions`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  deleteSubscription(
    environmentId: string,
    topicName: string,
    subscriptionName: string
  ): Observable<SubscriptionMutationResponse> {
    const params = new HttpParams()
      .set('topic', topicName)
      .set('subscription', subscriptionName);

    return this.http.delete<SubscriptionMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/subscriptions`,
      { params: this.demoMode.appendHttpParams(params) }
    );
  }

  getTopicDetails(environmentId: string, topicName: string): Observable<TopicDetails> {
    return this.http.get<TopicDetails>(`${this.baseUrl}/environments/${environmentId}/topics/detail`, {
      params: this.demoMode.appendHttpParams(new HttpParams().set('topic', topicName))
    });
  }

  peekMessages(environmentId: string, topicName: string, limit = 5): Observable<PeekMessagesResponse> {
    return this.http.get<PeekMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/peek`,
      { params: this.demoMode.appendHttpParams(new HttpParams().set('topic', topicName).set('limit', String(limit))) }
    );
  }

  terminateTopic(environmentId: string, request: TerminateTopicRequest): Observable<TerminateTopicResponse> {
    return this.http.post<TerminateTopicResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/terminate`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getTopicPolicies(environmentId: string, topicName: string): Observable<TopicPoliciesResponse> {
    return this.http.get<TopicPoliciesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/policies`,
      { params: this.demoMode.appendHttpParams(new HttpParams().set('topic', topicName)) }
    );
  }

  getSchemaDetails(environmentId: string, topicName: string): Observable<SchemaDetails> {
    return this.http.get<SchemaDetails>(
      `${this.baseUrl}/environments/${environmentId}/topics/schema`,
      { params: this.demoMode.appendHttpParams(new HttpParams().set('topic', topicName)) }
    );
  }

  upsertSchema(environmentId: string, request: SchemaUpdateRequest): Observable<SchemaMutationResponse> {
    return this.http.post<SchemaMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/schema`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  deleteSchema(environmentId: string, request: SchemaDeleteRequest): Observable<SchemaMutationResponse> {
    return this.http.post<SchemaMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/schema/delete`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  updateTopicPolicies(
    environmentId: string,
    request: TopicPoliciesUpdateRequest
  ): Observable<TopicPoliciesUpdateResponse> {
    return this.http.post<TopicPoliciesUpdateResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/policies`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  publishMessage(environmentId: string, request: PublishMessageRequest): Observable<PublishMessageResponse> {
    return this.http.post<PublishMessageResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/publish`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  consumeMessages(environmentId: string, request: ConsumeMessagesRequest): Observable<ConsumeMessagesResponse> {
    return this.http.post<ConsumeMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/consume`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  exportMessages(environmentId: string, request: ExportMessagesRequest): Observable<ExportMessagesResponse> {
    return this.http.post<ExportMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/export`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  resetCursor(environmentId: string, request: ResetCursorRequest): Observable<ResetCursorResponse> {
    return this.http.post<ResetCursorResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/reset-cursor`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  skipMessages(environmentId: string, request: SkipMessagesRequest): Observable<SkipMessagesResponse> {
    return this.http.post<SkipMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/skip-messages`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  unloadTopic(environmentId: string, request: UnloadTopicRequest): Observable<UnloadTopicResponse> {
    return this.http.post<UnloadTopicResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/unload`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getPlatformSummary(environmentId: string): Observable<PlatformSummary> {
    return this.http.get<PlatformSummary>(
      `${this.baseUrl}/environments/${environmentId}/platform`,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getPlatformArtifactDetails(
    environmentId: string,
    artifactType: string,
    name: string,
    tenant?: string | null,
    namespace?: string | null
  ): Observable<PlatformArtifactDetails> {
    let params = new HttpParams()
      .set('type', artifactType)
      .set('name', name);
    if (tenant) {
      params = params.set('tenant', tenant);
    }
    if (namespace) {
      params = params.set('namespace', namespace);
    }
    return this.http.get<PlatformArtifactDetails>(
      `${this.baseUrl}/environments/${environmentId}/platform/artifacts/detail`,
      { params: this.demoMode.appendHttpParams(params) }
    );
  }

  upsertPlatformArtifact(
    environmentId: string,
    request: PlatformArtifactMutationRequest
  ): Observable<PlatformArtifactMutationResponse> {
    return this.http.post<PlatformArtifactMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/platform/artifacts`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  deletePlatformArtifact(
    environmentId: string,
    request: PlatformArtifactDeleteRequest
  ): Observable<PlatformArtifactMutationResponse> {
    return this.http.post<PlatformArtifactMutationResponse>(
      `${this.baseUrl}/environments/${environmentId}/platform/artifacts/delete`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  createReplayCopyJob(
    environmentId: string,
    request: ReplayCopyJobRequest
  ): Observable<ReplayCopyJobStatusResponse> {
    return this.http.post<ReplayCopyJobStatusResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/replay-copy`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getReplayCopyJob(
    environmentId: string,
    jobId: string
  ): Observable<ReplayCopyJobStatusResponse> {
    return this.http.get<ReplayCopyJobStatusResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/jobs/${jobId}`,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  validateTenantYaml(
    environmentId: string,
    request: TenantYamlPreviewRequest
  ): Observable<TenantYamlPreviewResponse> {
    return this.http.post<TenantYamlPreviewResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/yaml/validate`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  getCurrentNamespaceYaml(
    environmentId: string,
    tenant: string,
    namespace: string
  ): Observable<NamespaceYamlCurrentResponse> {
    return this.http.get<NamespaceYamlCurrentResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/yaml/current`,
      {
        params: this.demoMode.appendHttpParams(
          new HttpParams().set('tenant', tenant).set('namespace', namespace)
        )
      }
    );
  }

  previewTenantYaml(
    environmentId: string,
    request: TenantYamlPreviewRequest
  ): Observable<TenantYamlPreviewResponse> {
    return this.http.post<TenantYamlPreviewResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/yaml/preview`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  applyTenantYaml(
    environmentId: string,
    request: TenantYamlApplyRequest
  ): Observable<TenantYamlApplyResponse> {
    return this.http.post<TenantYamlApplyResponse>(
      `${this.baseUrl}/environments/${environmentId}/namespaces/yaml/apply`,
      request,
      { params: this.demoMode.appendHttpParams(new HttpParams()) }
    );
  }

  refreshEnvironments() {
    this.environmentRefresh$.next();
  }
}
