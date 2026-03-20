import { DOCUMENT } from '@angular/common';
import { Injectable, inject } from '@angular/core';
import { HttpParams } from '@angular/common/http';
import { Router } from '@angular/router';

@Injectable({ providedIn: 'root' })
export class DemoModeService {
  private readonly document = inject(DOCUMENT);
  private readonly router = inject(Router);

  currentMode(): 'live' | 'mock' {
    return this.readCurrentMode() === 'mock' ? 'mock' : 'live';
  }

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

  setMode(mode: 'live' | 'mock'): Promise<boolean> {
    return this.router.navigate(['/environments'], {
      queryParams: mode === 'mock' ? { mode: 'mock' } : {}
    });
  }

  private readCurrentMode(): string | null {
    const search = this.document?.location?.search ?? '';
    return new URLSearchParams(search).get('mode');
  }
}
