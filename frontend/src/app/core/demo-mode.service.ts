import { DOCUMENT } from '@angular/common';
import { Injectable, inject } from '@angular/core';
import { HttpParams } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class DemoModeService {
  private readonly document = inject(DOCUMENT);

  isMockMode(): boolean {
    return this.currentMode() === 'mock';
  }

  queryParams<T extends Record<string, string | null | undefined>>(params: T): T & { mode?: 'mock' } {
    if (!this.isMockMode()) {
      return params as T & { mode?: 'mock' };
    }

    return {
      ...params,
      mode: 'mock'
    };
  }

  appendHttpParams(params: HttpParams): HttpParams {
    return this.isMockMode() ? params.set('mode', 'mock') : params;
  }

  private currentMode(): string | null {
    const search = this.document?.location?.search ?? '';
    return new URLSearchParams(search).get('mode');
  }
}
