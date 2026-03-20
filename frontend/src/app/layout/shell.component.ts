import { AsyncPipe, NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NavigationEnd, Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { filter, shareReplay, startWith } from 'rxjs';
import { PulsarApiService } from '../core/api/pulsar-api.service';
import { DemoModeService } from '../core/demo-mode.service';
import { EnvironmentSummary } from '../core/models/api.models';

@Component({
  selector: 'app-shell',
  standalone: true,
  imports: [AsyncPipe, NgClass, RouterLink, RouterLinkActive, RouterOutlet],
  templateUrl: './shell.component.html',
  styleUrl: './shell.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class ShellComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);

  readonly environments$ = this.api.getEnvironments().pipe(shareReplay(1));
  readonly currentEnvironmentId = signal<string | null>(null);

  constructor() {
    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        startWith(null),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(() => {
        const match = this.router.url.match(/\/environments\/([^/?]+)/);
        this.currentEnvironmentId.set(match?.[1] ?? null);
      });
  }

  onEnvironmentSelected(environmentId: string) {
    if (!environmentId) {
      return;
    }

    void this.router.navigate(['/environments', environmentId, 'topics'], {
      queryParams: this.modeQueryParams()
    });
  }

  resolveCurrentEnvironment(environments: EnvironmentSummary[]): EnvironmentSummary | undefined {
    return environments.find((environment) => environment.id === this.currentEnvironmentId());
  }

  statusClass(status: string): string {
    return status.toLowerCase();
  }

  modeQueryParams() {
    return this.demoMode.queryParams({});
  }
}
