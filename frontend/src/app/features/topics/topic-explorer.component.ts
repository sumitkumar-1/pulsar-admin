import { DecimalPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';
import { BehaviorSubject, combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import {
  CreateTopicRequest,
  EnvironmentHealth,
  TopicListItem,
  TopicPage
} from '../../core/models/api.models';
import { CreateTopicDialogComponent } from './create-topic-dialog.component';

interface TopicGroup {
  namespace: string;
  items: TopicListItem[];
}

@Component({
  selector: 'app-topic-explorer',
  standalone: true,
  imports: [CreateTopicDialogComponent, DecimalPipe, NgClass, ReactiveFormsModule, UpperCasePipe],
  templateUrl: './topic-explorer.component.html',
  styleUrl: './topic-explorer.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicExplorerComponent {
  private readonly api = inject(PulsarApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refresh$ = new BehaviorSubject<void>(undefined);

  readonly searchControl = new FormControl('', { nonNullable: true });
  readonly createTopicForm = new FormGroup({
    domain: new FormControl<'persistent' | 'non-persistent'>('persistent', { nonNullable: true, validators: [Validators.required] }),
    tenant: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    namespace: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    topic: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    partitions: new FormControl(0, { nonNullable: true, validators: [Validators.min(0), Validators.max(128)] }),
    notes: new FormControl('', { nonNullable: true })
  });
  readonly environmentId = signal('');
  readonly environmentHealth = signal<EnvironmentHealth | null>(null);
  readonly topicPage = signal<TopicPage | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly dialogOpen = signal(false);
  readonly savingTopic = signal(false);
  readonly actionFeedback = signal<{ kind: 'success' | 'error'; message: string } | null>(null);

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

  readonly namespaceOptions = computed(() => {
    const seen = new Set<string>();
    const options: Array<{ tenant: string; namespace: string }> = [];

    for (const group of this.topicGroups()) {
      const [tenant, namespace] = group.namespace.split('/');
      const key = `${tenant}/${namespace}`;
      if (!seen.has(key)) {
        seen.add(key);
        options.push({ tenant, namespace });
      }
    }

    return options.sort((left, right) =>
      `${left.tenant}/${left.namespace}`.localeCompare(`${right.tenant}/${right.namespace}`)
    );
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap, this.refresh$])
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

  openCreateTopicDialog() {
    this.createTopicForm.reset({
      domain: 'persistent',
      tenant: '',
      namespace: '',
      topic: '',
      partitions: 0,
      notes: ''
    });
    this.actionFeedback.set(null);
    this.dialogOpen.set(true);
  }

  closeCreateTopicDialog() {
    if (!this.savingTopic()) {
      this.dialogOpen.set(false);
    }
  }

  applyNamespaceSelection(value: string) {
    if (!value) {
      return;
    }

    const [tenant, namespace] = value.split('/');
    this.createTopicForm.patchValue({ tenant, namespace });
  }

  submitCreateTopic() {
    if (this.createTopicForm.invalid) {
      this.createTopicForm.markAllAsTouched();
      return;
    }

    this.savingTopic.set(true);
    this.actionFeedback.set(null);

    const request: CreateTopicRequest = {
      domain: this.createTopicForm.controls.domain.value,
      tenant: this.createTopicForm.controls.tenant.value.trim(),
      namespace: this.createTopicForm.controls.namespace.value.trim(),
      topic: this.createTopicForm.controls.topic.value.trim(),
      partitions: Number(this.createTopicForm.controls.partitions.value),
      notes: this.createTopicForm.controls.notes.value.trim() || null
    };

    this.api.createTopic(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (topic) => {
          this.dialogOpen.set(false);
          this.savingTopic.set(false);
          this.actionFeedback.set({
            kind: 'success',
            message: `Created ${topic.fullName} and refreshed the topic inventory.`
          });
          this.refresh$.next(undefined);
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingTopic.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to create the topic right now.'
          });
        }
      });
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
