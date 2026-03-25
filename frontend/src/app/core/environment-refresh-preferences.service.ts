import { DOCUMENT } from '@angular/common';
import { Injectable, inject } from '@angular/core';

export interface EnvironmentRefreshPreference {
  enabled: boolean;
  intervalSeconds: number;
}

@Injectable({ providedIn: 'root' })
export class EnvironmentRefreshPreferencesService {
  private static readonly STORAGE_KEY = 'pulsar-admin.environment-refresh.v1';
  private readonly document = inject(DOCUMENT);

  get(environmentId: string): EnvironmentRefreshPreference {
    const all = this.readAll();
    const stored = all[environmentId];
    if (!stored) {
      return { enabled: false, intervalSeconds: 15 };
    }
    return {
      enabled: !!stored.enabled,
      intervalSeconds: this.normalizeInterval(stored.intervalSeconds)
    };
  }

  setEnabled(environmentId: string, enabled: boolean) {
    const all = this.readAll();
    const current = this.get(environmentId);
    all[environmentId] = {
      enabled,
      intervalSeconds: current.intervalSeconds
    };
    this.writeAll(all);
  }

  setIntervalSeconds(environmentId: string, intervalSeconds: number) {
    const all = this.readAll();
    const current = this.get(environmentId);
    all[environmentId] = {
      enabled: current.enabled,
      intervalSeconds: this.normalizeInterval(intervalSeconds)
    };
    this.writeAll(all);
  }

  private normalizeInterval(intervalSeconds: number): number {
    if (!Number.isFinite(intervalSeconds)) {
      return 15;
    }
    return Math.max(5, Math.min(300, Math.round(intervalSeconds)));
  }

  private readAll(): Record<string, EnvironmentRefreshPreference> {
    try {
      const raw = this.document?.defaultView?.localStorage?.getItem(EnvironmentRefreshPreferencesService.STORAGE_KEY);
      if (!raw) {
        return {};
      }
      const parsed = JSON.parse(raw) as Record<string, EnvironmentRefreshPreference>;
      return parsed ?? {};
    } catch {
      return {};
    }
  }

  private writeAll(value: Record<string, EnvironmentRefreshPreference>) {
    try {
      this.document?.defaultView?.localStorage?.setItem(
        EnvironmentRefreshPreferencesService.STORAGE_KEY,
        JSON.stringify(value)
      );
    } catch {
      // Best-effort persistence only.
    }
  }
}
