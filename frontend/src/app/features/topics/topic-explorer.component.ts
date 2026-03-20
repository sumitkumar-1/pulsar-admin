import { DecimalPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';
import { BehaviorSubject, combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import {
  CatalogSummary,
  CreateNamespaceRequest,
  CreateTenantRequest,
  CreateTopicRequest,
  EnvironmentHealth,
  NamespaceSummary,
  TopicListItem,
  TopicPage
} from '../../core/models/api.models';
import { CreateNamespaceDialogComponent } from './create-namespace-dialog.component';
import { CreateTenantDialogComponent } from './create-tenant-dialog.component';
import { CreateTopicDialogComponent } from './create-topic-dialog.component';

interface NamespaceTopicGroup {
  key: string;
  tenant: string;
  namespace: string;
  items: TopicListItem[];
}

interface TenantTopicGroup {
  tenant: string;
  namespaceCount: number;
  topicCount: number;
  namespaces: NamespaceTopicGroup[];
}

@Component({
  selector: 'app-topic-explorer',
  standalone: true,
  imports: [
    CreateNamespaceDialogComponent,
    CreateTenantDialogComponent,
    CreateTopicDialogComponent,
    DecimalPipe,
    NgClass,
    ReactiveFormsModule,
    UpperCasePipe
  ],
  templateUrl: './topic-explorer.component.html',
  styleUrl: './topic-explorer.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicExplorerComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
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
  readonly createTenantForm = new FormGroup({
    tenant: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    adminRoles: new FormControl('', { nonNullable: true }),
    allowedClusters: new FormControl('', { nonNullable: true })
  });
  readonly createNamespaceForm = new FormGroup({
    tenant: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    namespace: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] })
  });
  readonly environmentId = signal('');
  readonly environmentHealth = signal<EnvironmentHealth | null>(null);
  readonly catalogSummary = signal<CatalogSummary | null>(null);
  readonly topicPage = signal<TopicPage | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly tenantDialogOpen = signal(false);
  readonly namespaceDialogOpen = signal(false);
  readonly dialogOpen = signal(false);
  readonly savingTenant = signal(false);
  readonly savingNamespace = signal(false);
  readonly savingTopic = signal(false);
  readonly actionFeedback = signal<{ kind: 'success' | 'error'; message: string } | null>(null);

  readonly topicGroups = computed<TenantTopicGroup[]>(() => {
    const page = this.topicPage();
    const catalog = this.catalogSummary();
    const hasSearch = !!this.searchControl.value.trim();

    if (!page || !catalog) {
      return [];
    }

    const topicMap = new Map<string, TopicListItem[]>();

    for (const item of page.items) {
      const key = `${item.tenant}/${item.namespace}`;
      topicMap.set(key, [...(topicMap.get(key) ?? []), item]);
    }

    const namespaceSource: NamespaceSummary[] = hasSearch
      ? [...topicMap.keys()].map((key) => {
          const [tenant, namespace] = key.split('/');
          return { tenant, namespace, topicCount: topicMap.get(key)?.length ?? 0 };
        })
      : catalog.namespaces;

    const tenantSource = hasSearch
      ? Array.from(new Set(namespaceSource.map((namespace) => namespace.tenant)))
      : catalog.tenants.map((tenant) => tenant.name);

    return tenantSource
      .map((tenant) => {
        const namespaces = namespaceSource
          .filter((namespace) => namespace.tenant === tenant)
          .map((namespace) => ({
            key: `${tenant}/${namespace.namespace}`,
            tenant,
            namespace: namespace.namespace,
            items: [...(topicMap.get(`${tenant}/${namespace.namespace}`) ?? [])].sort((left, right) =>
              left.topic.localeCompare(right.topic))
          }))
          .filter((namespace) => !hasSearch || namespace.items.length > 0)
          .sort((left, right) => left.namespace.localeCompare(right.namespace));

        const topicCount = namespaces.reduce((sum, namespace) => sum + namespace.items.length, 0);

        return {
          tenant,
          namespaceCount: namespaces.length,
          topicCount,
          namespaces
        };
      })
      .filter((tenant) => tenant.namespaces.length > 0 || !hasSearch)
      .sort((left, right) => left.tenant.localeCompare(right.tenant));
  });

  readonly namespaceOptions = computed(() => {
    const catalog = this.catalogSummary();

    if (!catalog) {
      return [];
    }

    return catalog.namespaces.map((namespace) => ({ tenant: namespace.tenant, namespace: namespace.namespace }))
      .sort((left, right) =>
      `${left.tenant}/${left.namespace}`.localeCompare(`${right.tenant}/${right.namespace}`)
    );
  });

  readonly tenantOptions = computed(() => {
    const catalog = this.catalogSummary();
    return catalog ? catalog.tenants.map((tenant) => tenant.name).sort((left, right) => left.localeCompare(right)) : [];
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap, this.refresh$])
      .pipe(
        switchMap(([params, queryParams]) => this.loadData(params, queryParams)),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ([health, catalog, pageResult]) => {
          this.environmentHealth.set(health);
          this.catalogSummary.set(catalog);
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
      queryParams: this.demoMode.queryParams({ topic: topic.fullName })
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

  openCreateTenantDialog() {
    this.createTenantForm.reset({
      tenant: '',
      adminRoles: '',
      allowedClusters: ''
    });
    this.actionFeedback.set(null);
    this.tenantDialogOpen.set(true);
  }

  openCreateNamespaceDialog() {
    this.createNamespaceForm.reset({
      tenant: '',
      namespace: ''
    });
    this.actionFeedback.set(null);
    this.namespaceDialogOpen.set(true);
  }

  closeCreateTopicDialog() {
    if (!this.savingTopic()) {
      this.dialogOpen.set(false);
    }
  }

  closeCreateTenantDialog() {
    if (!this.savingTenant()) {
      this.tenantDialogOpen.set(false);
    }
  }

  closeCreateNamespaceDialog() {
    if (!this.savingNamespace()) {
      this.namespaceDialogOpen.set(false);
    }
  }

  applyNamespaceSelection(value: string) {
    if (!value) {
      return;
    }

    const [tenant, namespace] = value.split('/');
    this.createTopicForm.patchValue({ tenant, namespace });
  }

  applyTenantSelection(value: string) {
    if (!value) {
      return;
    }

    this.createNamespaceForm.patchValue({ tenant: value });
  }

  submitCreateTenant() {
    if (this.createTenantForm.invalid) {
      this.createTenantForm.markAllAsTouched();
      return;
    }

    this.savingTenant.set(true);
    this.actionFeedback.set(null);

    const request: CreateTenantRequest = {
      tenant: this.createTenantForm.controls.tenant.value.trim(),
      adminRoles: this.toCsvList(this.createTenantForm.controls.adminRoles.value),
      allowedClusters: this.toCsvList(this.createTenantForm.controls.allowedClusters.value)
    };

    this.api.createTenant(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.tenantDialogOpen.set(false);
          this.savingTenant.set(false);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          this.refresh$.next(undefined);
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingTenant.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to create the tenant right now.'
          });
        }
      });
  }

  submitCreateNamespace() {
    if (this.createNamespaceForm.invalid) {
      this.createNamespaceForm.markAllAsTouched();
      return;
    }

    this.savingNamespace.set(true);
    this.actionFeedback.set(null);

    const request: CreateNamespaceRequest = {
      tenant: this.createNamespaceForm.controls.tenant.value.trim(),
      namespace: this.createNamespaceForm.controls.namespace.value.trim()
    };

    this.api.createNamespace(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.namespaceDialogOpen.set(false);
          this.savingNamespace.set(false);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          this.refresh$.next(undefined);
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingNamespace.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to create the namespace right now.'
          });
        }
      });
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
      this.api.getCatalogSummary(envId),
      this.api.getTopics(envId, { search, tenant, namespace, page, pageSize })
    ]);
  }

  private toCsvList(value: string): string[] {
    return value.split(',')
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }
}
