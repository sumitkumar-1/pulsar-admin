import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Observable, switchMap, tap } from 'rxjs';
import { DemoModeService } from '../demo-mode.service';
import {
  CatalogMutationResponse,
  CatalogSummary,
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
  PeekMessagesResponse,
  ReplayCopyJobRequest,
  ReplayCopyJobStatusResponse,
  ResetCursorRequest,
  ResetCursorResponse,
  SkipMessagesRequest,
  SkipMessagesResponse,
  SubscriptionMutationResponse,
  TopicDetails,
  TopicPage,
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

  createTopic(environmentId: string, request: CreateTopicRequest): Observable<TopicDetails> {
    return this.http.post<TopicDetails>(`${this.baseUrl}/environments/${environmentId}/topics`, request, {
      params: this.demoMode.appendHttpParams(new HttpParams())
    });
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

  refreshEnvironments() {
    this.environmentRefresh$.next();
  }
}
