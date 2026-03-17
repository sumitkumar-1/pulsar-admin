import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import {
  EnvironmentHealth,
  EnvironmentSummary,
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

  getEnvironments(): Observable<EnvironmentSummary[]> {
    return this.http.get<EnvironmentSummary[]>(`${this.baseUrl}/environments`);
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
    return this.http.get<TopicDetails>(
      `${this.baseUrl}/environments/${environmentId}/topics/${encodeURIComponent(topicName)}`
    );
  }
}
