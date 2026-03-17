import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Observable, switchMap, tap } from 'rxjs';
import {
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
  TopicDetails,
  TopicPage
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
  private readonly baseUrl = '/api/v1';
  private readonly environmentRefresh$ = new BehaviorSubject<void>(undefined);

  getEnvironments(): Observable<EnvironmentSummary[]> {
    return this.environmentRefresh$.pipe(
      switchMap(() => this.http.get<EnvironmentSummary[]>(`${this.baseUrl}/environments`))
    );
  }

  getEnvironment(environmentId: string): Observable<EnvironmentDetails> {
    return this.http.get<EnvironmentDetails>(`${this.baseUrl}/environments/${environmentId}`);
  }

  createEnvironment(request: EnvironmentUpsertRequest): Observable<EnvironmentDetails> {
    return this.http.post<EnvironmentDetails>(`${this.baseUrl}/environments`, request).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  updateEnvironment(environmentId: string, request: EnvironmentUpsertRequest): Observable<EnvironmentDetails> {
    return this.http.patch<EnvironmentDetails>(`${this.baseUrl}/environments/${environmentId}`, request).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  testEnvironmentConnection(environmentId: string): Observable<EnvironmentConnectionTestResult> {
    return this.http.post<EnvironmentConnectionTestResult>(
      `${this.baseUrl}/environments/${environmentId}/test-connection`,
      {}
    ).pipe(tap(() => this.refreshEnvironments()));
  }

  syncEnvironment(environmentId: string): Observable<EnvironmentSyncStatus> {
    return this.http.post<EnvironmentSyncStatus>(
      `${this.baseUrl}/environments/${environmentId}/sync`,
      {}
    ).pipe(tap(() => this.refreshEnvironments()));
  }

  getEnvironmentSyncStatus(environmentId: string): Observable<EnvironmentSyncStatus> {
    return this.http.get<EnvironmentSyncStatus>(`${this.baseUrl}/environments/${environmentId}/sync-status`);
  }

  deleteEnvironment(environmentId: string): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/environments/${environmentId}`).pipe(
      tap(() => this.refreshEnvironments())
    );
  }

  getEnvironmentHealth(environmentId: string): Observable<EnvironmentHealth> {
    return this.http.get<EnvironmentHealth>(`${this.baseUrl}/environments/${environmentId}/health`);
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

    return this.http.get<TopicPage>(`${this.baseUrl}/environments/${environmentId}/topics`, { params });
  }

  getTopicDetails(environmentId: string, topicName: string): Observable<TopicDetails> {
    return this.http.get<TopicDetails>(`${this.baseUrl}/environments/${environmentId}/topics/detail`, {
      params: new HttpParams().set('topic', topicName)
    });
  }

  peekMessages(environmentId: string, topicName: string, limit = 5): Observable<PeekMessagesResponse> {
    return this.http.get<PeekMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/peek`,
      { params: new HttpParams().set('topic', topicName).set('limit', String(limit)) }
    );
  }

  resetCursor(environmentId: string, request: ResetCursorRequest): Observable<ResetCursorResponse> {
    return this.http.post<ResetCursorResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/reset-cursor`,
      request
    );
  }

  skipMessages(environmentId: string, request: SkipMessagesRequest): Observable<SkipMessagesResponse> {
    return this.http.post<SkipMessagesResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/skip-messages`,
      request
    );
  }

  createReplayCopyJob(
    environmentId: string,
    request: ReplayCopyJobRequest
  ): Observable<ReplayCopyJobStatusResponse> {
    return this.http.post<ReplayCopyJobStatusResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/replay-copy`,
      request
    );
  }

  getReplayCopyJob(
    environmentId: string,
    jobId: string
  ): Observable<ReplayCopyJobStatusResponse> {
    return this.http.get<ReplayCopyJobStatusResponse>(
      `${this.baseUrl}/environments/${environmentId}/topics/jobs/${jobId}`
    );
  }

  refreshEnvironments() {
    this.environmentRefresh$.next();
  }
}
