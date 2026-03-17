import { DecimalPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';
import { combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import {
  EnvironmentHealth,
  TopicListItem,
  TopicPage
} from '../../core/models/api.models';

interface TopicGroup {
  namespace: string;
  items: TopicListItem[];
}

@Component({
  selector: 'app-topic-explorer',
  standalone: true,
  imports: [DecimalPipe, NgClass, ReactiveFormsModule, UpperCasePipe],
  templateUrl: './topic-explorer.component.html',
  styleUrl: './topic-explorer.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicExplorerComponent {
  private readonly api = inject(PulsarApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);

  readonly searchControl = new FormControl('', { nonNullable: true });
  readonly environmentId = signal('');
  readonly environmentHealth = signal<EnvironmentHealth | null>(null);
  readonly topicPage = signal<TopicPage | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);

  readonly topicGroups = computed<TopicGroup[]>(() => {
    const page = this.topicPage();

    if (!page) {
      return [];
    }

    const grouped = new Map<string, TopicListItem[]>();

    for (const item of page.items) {
      const key = `${item.tenant}/${item.namespace}`;
      grouped.set(key, [...(grouped.get(key) ?? []), item]);
    }

    return [...grouped.entries()].map(([namespace, items]) => ({ namespace, items }));
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(
        switchMap(([params, queryParams]) => this.loadData(params, queryParams)),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ([health, pageResult]) => {
          this.environmentHealth.set(health);
          this.topicPage.set(pageResult);
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.loadError.set(error.error?.message ?? 'Unable to load topics right now.');
          this.loading.set(false);
        }
      });
  }

  applySearch() {
    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        search: this.searchControl.value || null,
        page: 0
      },
      queryParamsHandling: 'merge'
    });
  }

  clearSearch() {
    this.searchControl.setValue('');
    this.applySearch();
  }

  openTopic(topic: TopicListItem) {
    void this.router.navigate(['/environments', this.environmentId(), 'topic-details'], {
      queryParams: { topic: topic.fullName }
    });
  }

  healthClass(status: string): string {
    return status.toLowerCase();
  }

  private loadData(params: ParamMap, queryParams: ParamMap) {
    const envId = params.get('envId') ?? '';
    const search = queryParams.get('search') ?? '';
    const tenant = queryParams.get('tenant') ?? undefined;
    const namespace = queryParams.get('namespace') ?? undefined;
    const page = Number(queryParams.get('page') ?? '0');
    const pageSize = Number(queryParams.get('pageSize') ?? '25');

    this.environmentId.set(envId);
    this.searchControl.setValue(search, { emitEvent: false });
    this.loading.set(true);
    this.loadError.set(null);

    return combineLatest([
      this.api.getEnvironmentHealth(envId),
      this.api.getTopics(envId, { search, tenant, namespace, page, pageSize })
    ]);
  }
}
