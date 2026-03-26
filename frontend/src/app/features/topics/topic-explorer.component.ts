import { DecimalPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, computed, effect, inject, signal } from '@angular/core';
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
  EnvironmentSyncStatus,
  NamespaceDeleteRequest,
  NamespaceSummary,
  PlatformArtifactDeleteRequest,
  PlatformArtifactDetails,
  PlatformArtifactMutationRequest,
  PlatformArtifactMutationResponse,
  PlatformSummary,
  TenantDeleteRequest,
  TenantDetails,
  TenantUpdateRequest,
  TenantSummary,
  TopicListItem,
  TopicPage
} from '../../core/models/api.models';
import { CreateNamespaceDialogComponent } from './create-namespace-dialog.component';
import { CreateTenantDialogComponent } from './create-tenant-dialog.component';
import { CreateTopicDialogComponent } from './create-topic-dialog.component';

interface NamespaceWorkspaceItem {
  key: string;
  tenant: string;
  namespace: string;
  topicCount: number;
}

type PlatformArtifactType = 'FUNCTION' | 'SOURCE' | 'SINK' | 'CONNECTOR';

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
  private static readonly INITIAL_SYNC_POLL_MS = 5000;
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly refresh$ = new BehaviorSubject<void>(undefined);
  private syncRetryHandle: number | null = null;

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
  readonly editTenantForm = new FormGroup({
    tenant: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    adminRoles: new FormControl('', { nonNullable: true }),
    allowedClusters: new FormControl('', { nonNullable: true }),
    reason: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.maxLength(240)] })
  });
  readonly deleteTenantForm = new FormGroup({
    reason: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.maxLength(240)] })
  });
  readonly platformForm = new FormGroup({
    artifactType: new FormControl<PlatformArtifactType>('FUNCTION', { nonNullable: true, validators: [Validators.required] }),
    name: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    tenant: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    namespace: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)] }),
    className: new FormControl('', { nonNullable: true }),
    archive: new FormControl('', { nonNullable: true }),
    inputTopic: new FormControl('', { nonNullable: true }),
    outputTopic: new FormControl('', { nonNullable: true }),
    parallelism: new FormControl(1, { nonNullable: true, validators: [Validators.min(1), Validators.max(128)] }),
    configs: new FormControl('{\n}', { nonNullable: true, validators: [Validators.required] }),
    reason: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.maxLength(240)] })
  });
  readonly platformDeleteForm = new FormGroup({
    reason: new FormControl('', { nonNullable: true, validators: [Validators.required, Validators.maxLength(240)] })
  });
  readonly environmentId = signal('');
  readonly environmentHealth = signal<EnvironmentHealth | null>(null);
  readonly environmentSyncStatus = signal<EnvironmentSyncStatus | null>(null);
  readonly catalogSummary = signal<CatalogSummary | null>(null);
  readonly platformSummary = signal<PlatformSummary | null>(null);
  readonly topicPage = signal<TopicPage | null>(null);
  readonly selectedTenant = signal('');
  readonly selectedNamespace = signal('');
  readonly activeTab = signal<'topics' | 'namespace' | 'platform'>('topics');
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly tenantDialogOpen = signal(false);
  readonly namespaceDialogOpen = signal(false);
  readonly dialogOpen = signal(false);
  readonly editTenantDialogOpen = signal(false);
  readonly deleteTenantDialogOpen = signal(false);
  readonly platformDialogOpen = signal(false);
  readonly platformDeleteDialogOpen = signal(false);
  readonly savingTenant = signal(false);
  readonly savingNamespace = signal(false);
  readonly savingTopic = signal(false);
  readonly savingPlatform = signal(false);
  readonly platformLoading = signal(false);
  readonly actionFeedback = signal<{ kind: 'success' | 'error'; message: string } | null>(null);
  readonly platformArtifact = signal<PlatformArtifactDetails | null>(null);
  readonly platformMode = signal<'create' | 'edit'>('create');

  readonly namespaceItems = computed<NamespaceWorkspaceItem[]>(() => {
    const catalog = this.catalogSummary();

    if (!catalog) {
      return [];
    }

    return catalog.namespaces
      .map((namespace) => ({
        key: `${namespace.tenant}/${namespace.namespace}`,
        tenant: namespace.tenant,
        namespace: namespace.namespace,
        topicCount: namespace.topicCount
      }))
      .sort((left, right) => left.key.localeCompare(right.key));
  });

  readonly selectedNamespaceSummary = computed<NamespaceSummary | null>(() => {
    const catalog = this.catalogSummary();
    const tenant = this.selectedTenant();
    const namespace = this.selectedNamespace();

    if (!catalog || !tenant || !namespace) {
      return null;
    }

    return catalog.namespaces.find((item) => item.tenant === tenant && item.namespace === namespace) ?? null;
  });

  readonly selectedTenantSummary = computed<TenantSummary | null>(() => {
    const catalog = this.catalogSummary();
    const tenant = this.selectedTenant();
    if (!catalog || !tenant) {
      return null;
    }
    return catalog.tenants.find((item) => item.name === tenant) ?? null;
  });

  readonly selectedTopics = computed(() =>
    [...(this.topicPage()?.items ?? [])].sort((left, right) => left.topic.localeCompare(right.topic))
  );
  readonly activeSearchTerm = computed(() => this.searchControl.value.trim().toLowerCase());
  readonly visibleSelectedTopics = computed(() => {
    const search = this.activeSearchTerm();
    const topics = this.selectedTopics();

    if (!search) {
      return topics;
    }

    return topics.filter((topic) =>
      topic.fullName.toLowerCase().includes(search)
      || topic.topic.toLowerCase().includes(search)
      || topic.namespace.toLowerCase().includes(search)
      || topic.summary.toLowerCase().includes(search)
    );
  });
  readonly selectedTopicPage = computed(() => this.topicPage()?.page ?? 0);
  readonly selectedTopicPageSize = computed(() => this.topicPage()?.pageSize ?? 25);
  readonly selectedTopicTotal = computed(() => this.topicPage()?.total ?? 0);
  readonly selectedTopicRangeStart = computed(() => {
    const total = this.selectedTopicTotal();
    if (total === 0) {
      return 0;
    }
    return this.selectedTopicPage() * this.selectedTopicPageSize() + 1;
  });
  readonly selectedTopicRangeEnd = computed(() => {
    const total = this.selectedTopicTotal();
    if (total === 0) {
      return 0;
    }
    return Math.min(total, this.selectedTopicRangeStart() + this.selectedTopics().length - 1);
  });
  readonly hasPreviousTopicPage = computed(() => this.selectedTopicPage() > 0);
  readonly hasNextTopicPage = computed(() =>
    this.selectedTopicRangeEnd() < this.selectedTopicTotal()
  );
  readonly visibleSelectedTopicCount = computed(() => this.visibleSelectedTopics().length);

  readonly totalTopicCount = computed(() =>
    this.catalogSummary()?.namespaces.reduce((sum, namespace) => sum + namespace.topicCount, 0) ?? 0
  );

  readonly hasNamespaces = computed(() => (this.catalogSummary()?.namespaces.length ?? 0) > 0);
  readonly selectedNamespaceLabel = computed(() => {
    const tenant = this.selectedTenant();
    const namespace = this.selectedNamespace();
    return tenant && namespace ? `${tenant}/${namespace}` : 'No namespace selected';
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
  readonly visibleFunctions = computed(() => this.filterPlatformArtifacts(this.platformSummary()?.functions ?? []));
  readonly visibleSources = computed(() => this.filterPlatformArtifacts(this.platformSummary()?.sources ?? []));
  readonly visibleSinks = computed(() => this.filterPlatformArtifacts(this.platformSummary()?.sinks ?? []));
  readonly visibleConnectors = computed(() => this.platformSummary()?.connectors ?? []);
  readonly awaitingInitialSync = computed(() => this.environmentSyncStatus()?.syncStatus === 'SYNCING');

  constructor() {
    this.destroyRef.onDestroy(() => this.clearSyncRetry());

    combineLatest([this.route.paramMap, this.route.queryParamMap, this.refresh$])
      .pipe(
        switchMap(([params, queryParams]) => this.loadData(params, queryParams)),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ([health, catalog, pageResult]) => {
          this.clearSyncRetry();
          this.environmentHealth.set(health);
          this.environmentSyncStatus.set(null);
          this.catalogSummary.set(catalog);
          this.topicPage.set(pageResult);
          this.loading.set(false);
          if (typeof this.api.getPlatformSummary === 'function') {
            this.api.getPlatformSummary(this.environmentId())
              .pipe(takeUntilDestroyed(this.destroyRef))
              .subscribe({
                next: (summary) => this.platformSummary.set(summary),
                error: () => this.platformSummary.set(null)
              });
          } else {
            this.platformSummary.set(null);
          }
        },
        error: (error: { error?: { message?: string } }) => {
          this.handleLoadError(error);
        }
      });

    effect(() => {
      const namespaces = this.namespaceItems();
      const tenant = this.selectedTenant();
      const namespace = this.selectedNamespace();

      if (!tenant && !namespace) {
        return;
      }

      const selectedExists = namespaces.some((item) => item.tenant === tenant && item.namespace === namespace);
      if (selectedExists) {
        return;
      }

      const nextNamespace = namespaces[0];
      this.selectedTenant.set(nextNamespace?.tenant ?? '');
      this.selectedNamespace.set(nextNamespace?.namespace ?? '');
    });
  }

  applySearch() {
    if (!this.selectedTenant() || !this.selectedNamespace()) {
      this.actionFeedback.set({
        kind: 'error',
        message: 'Select a tenant and namespace first, then search within that namespace.'
      });
      return;
    }

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

  changeTopicPageSize(rawValue: string) {
    const pageSize = Number(rawValue);
    if (![10, 25, 50, 100].includes(pageSize)) {
      return;
    }

    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        pageSize,
        page: 0
      },
      queryParamsHandling: 'merge'
    });
  }

  goToPreviousTopicPage() {
    if (!this.hasPreviousTopicPage()) {
      return;
    }
    this.goToTopicPage(this.selectedTopicPage() - 1);
  }

  goToNextTopicPage() {
    if (!this.hasNextTopicPage()) {
      return;
    }
    this.goToTopicPage(this.selectedTopicPage() + 1);
  }

  openTopic(topic: TopicListItem) {
    void this.router.navigate(['/environments', this.environmentId(), 'topic-details'], {
      queryParams: this.demoMode.queryParams({ topic: topic.fullName })
    });
  }

  openNamespace(tenant: string, namespace: string) {
    void this.router.navigate(['/environments', this.environmentId(), 'namespace-details'], {
      queryParams: this.demoMode.queryParams({ tenant, namespace })
    });
  }

  openNamespaceYaml() {
    if (!this.selectedTenant() || !this.selectedNamespace()) {
      return;
    }

    void this.router.navigate(['/environments', this.environmentId(), 'namespace-yaml'], {
      queryParams: this.demoMode.queryParams({
        tenant: this.selectedTenant(),
        namespace: this.selectedNamespace()
      })
    });
  }

  healthClass(status: string): string {
    return status.toLowerCase();
  }

  setActiveTab(tab: 'topics' | 'namespace' | 'platform') {
    this.activeTab.set(tab);
  }

  openCreateTopicDialog() {
    this.createTopicForm.reset({
      domain: 'persistent',
      tenant: this.selectedTenant(),
      namespace: this.selectedNamespace(),
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

  openEditTenantDialog() {
    const tenant = this.selectedTenant();
    if (!tenant) {
      return;
    }

    this.api.getTenantDetails(this.environmentId(), tenant)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (details: TenantDetails) => {
          this.editTenantForm.reset({
            tenant: details.tenant,
            adminRoles: details.adminRoles.join(','),
            allowedClusters: details.allowedClusters.join(','),
            reason: ''
          });
          this.actionFeedback.set(null);
          this.editTenantDialogOpen.set(true);
        },
        error: (error: { error?: { message?: string } }) => {
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to load tenant details right now.'
          });
        }
      });
  }

  openDeleteTenantDialog() {
    if (!this.selectedTenant()) {
      return;
    }
    this.deleteTenantForm.reset({ reason: '' });
    this.actionFeedback.set(null);
    this.deleteTenantDialogOpen.set(true);
  }

  openCreateNamespaceDialog() {
    this.createNamespaceForm.reset({
      tenant: this.selectedTenant(),
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

  closeEditTenantDialog() {
    if (!this.savingTenant()) {
      this.editTenantDialogOpen.set(false);
    }
  }

  closeDeleteTenantDialog() {
    if (!this.savingTenant()) {
      this.deleteTenantDialogOpen.set(false);
    }
  }

  closePlatformDialog() {
    if (!this.savingPlatform()) {
      this.platformDialogOpen.set(false);
    }
  }

  closePlatformDeleteDialog() {
    if (!this.savingPlatform()) {
      this.platformDeleteDialogOpen.set(false);
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
          void this.selectNamespace(request.tenant, request.namespace);
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

  submitUpdateTenant() {
    if (this.editTenantForm.invalid) {
      this.editTenantForm.markAllAsTouched();
      return;
    }

    this.savingTenant.set(true);
    const request: TenantUpdateRequest = {
      tenant: this.editTenantForm.controls.tenant.value.trim(),
      adminRoles: this.toCsvList(this.editTenantForm.controls.adminRoles.value),
      allowedClusters: this.toCsvList(this.editTenantForm.controls.allowedClusters.value),
      reason: this.editTenantForm.controls.reason.value.trim()
    };

    this.api.updateTenant(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.editTenantDialogOpen.set(false);
          this.savingTenant.set(false);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          this.refresh$.next(undefined);
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingTenant.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to update the tenant right now.'
          });
        }
      });
  }

  submitDeleteTenant() {
    if (this.deleteTenantForm.invalid || !this.selectedTenant()) {
      this.deleteTenantForm.markAllAsTouched();
      return;
    }

    this.savingTenant.set(true);
    const request: TenantDeleteRequest = {
      tenant: this.selectedTenant(),
      reason: this.deleteTenantForm.controls.reason.value.trim()
    };

    this.api.deleteTenant(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.deleteTenantDialogOpen.set(false);
          this.savingTenant.set(false);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          const nextNamespace = response.catalogSummary.namespaces[0];
          this.selectedTenant.set(nextNamespace?.tenant ?? '');
          this.selectedNamespace.set(nextNamespace?.namespace ?? '');
          const queryParams = nextNamespace
            ? this.demoMode.queryParams({ tenant: nextNamespace.tenant, namespace: nextNamespace.namespace, page: '0' })
            : this.demoMode.queryParams({});
          void this.router.navigate([], {
            relativeTo: this.route,
            queryParams,
            queryParamsHandling: nextNamespace ? 'merge' : undefined
          });
          this.refresh$.next(undefined);
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingTenant.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to delete the tenant right now.'
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
          void this.selectNamespace(topic.tenant, topic.namespace);
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

  openCreatePlatformArtifact(type: PlatformArtifactType) {
    this.platformMode.set('create');
    this.platformArtifact.set(null);
    this.platformForm.reset({
      artifactType: type,
      name: '',
      tenant: this.selectedTenant(),
      namespace: this.selectedNamespace(),
      className: '',
      archive: '',
      inputTopic: '',
      outputTopic: '',
      parallelism: 1,
      configs: '{\n}',
      reason: ''
    });
    this.platformDialogOpen.set(true);
  }

  openEditPlatformArtifact(type: PlatformArtifactType, name: string, tenant?: string | null, namespace?: string | null) {
    this.platformMode.set('edit');
    this.platformLoading.set(true);
    this.platformArtifact.set(null);
    this.api.getPlatformArtifactDetails(this.environmentId(), type, name, tenant, namespace)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (details) => {
          this.platformArtifact.set(details);
          this.platformForm.reset({
            artifactType: details.artifactType as PlatformArtifactType,
            name: details.name,
            tenant: details.tenant ?? this.selectedTenant(),
            namespace: details.namespace ?? this.selectedNamespace(),
            className: details.className ?? '',
            archive: details.archive ?? '',
            inputTopic: details.inputTopic ?? '',
            outputTopic: details.outputTopic ?? '',
            parallelism: details.parallelism ?? 1,
            configs: details.configs || '{\n}',
            reason: ''
          });
          this.platformLoading.set(false);
          this.platformDialogOpen.set(true);
        },
        error: (error: { error?: { message?: string } }) => {
          this.platformLoading.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to load the platform artifact details.'
          });
        }
      });
  }

  openDeletePlatformArtifact(type: PlatformArtifactType, name: string, tenant?: string | null, namespace?: string | null) {
    this.platformArtifact.set({
      environmentId: this.environmentId(),
      artifactType: type,
      name,
      tenant: tenant ?? null,
      namespace: namespace ?? null,
      status: '',
      details: '',
      archive: null,
      className: null,
      inputTopic: null,
      outputTopic: null,
      parallelism: null,
      configs: '',
      editable: type !== 'CONNECTOR'
    });
    this.platformDeleteForm.reset({ reason: '' });
    this.platformDeleteDialogOpen.set(true);
  }

  submitPlatformArtifact() {
    if (this.platformForm.invalid) {
      this.platformForm.markAllAsTouched();
      return;
    }
    const form = this.platformForm.getRawValue();
    const request: PlatformArtifactMutationRequest = {
      artifactType: form.artifactType,
      name: form.name.trim(),
      tenant: form.tenant.trim() || null,
      namespace: form.namespace.trim() || null,
      className: form.className.trim() || null,
      archive: form.archive.trim() || null,
      inputTopic: form.inputTopic.trim() || null,
      outputTopic: form.outputTopic.trim() || null,
      parallelism: Number(form.parallelism),
      configs: form.configs.trim() || '{}',
      reason: form.reason.trim()
    };
    this.savingPlatform.set(true);
    this.api.upsertPlatformArtifact(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response: PlatformArtifactMutationResponse) => {
          this.savingPlatform.set(false);
          this.platformDialogOpen.set(false);
          this.platformSummary.set(response.platformSummary);
          this.actionFeedback.set({ kind: 'success', message: response.message });
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingPlatform.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to save the platform artifact.'
          });
        }
      });
  }

  submitDeletePlatformArtifact() {
    const artifact = this.platformArtifact();
    if (!artifact) {
      return;
    }
    if (this.platformDeleteForm.invalid) {
      this.platformDeleteForm.markAllAsTouched();
      return;
    }
    const request: PlatformArtifactDeleteRequest = {
      artifactType: artifact.artifactType,
      name: artifact.name,
      tenant: artifact.tenant,
      namespace: artifact.namespace,
      reason: this.platformDeleteForm.controls.reason.value.trim()
    };
    this.savingPlatform.set(true);
    this.api.deletePlatformArtifact(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.savingPlatform.set(false);
          this.platformDeleteDialogOpen.set(false);
          this.platformSummary.set(response.platformSummary);
          this.actionFeedback.set({ kind: 'success', message: response.message });
        },
        error: (error: { error?: { message?: string } }) => {
          this.savingPlatform.set(false);
          this.actionFeedback.set({
            kind: 'error',
            message: error.error?.message ?? 'Unable to delete the platform artifact.'
          });
        }
      });
  }

  private loadData(params: ParamMap, queryParams: ParamMap) {
    const envId = params.get('envId') ?? '';
    const tenant = queryParams.get('tenant') ?? undefined;
    const namespace = queryParams.get('namespace') ?? undefined;
    const search = tenant && namespace ? (queryParams.get('search') ?? '') : '';
    const page = Number(queryParams.get('page') ?? '0');
    const pageSize = Number(queryParams.get('pageSize') ?? '25');
    const environmentChanged = this.environmentId() !== envId;
    const needsInitialLoad = environmentChanged || this.catalogSummary() === null || this.environmentHealth() === null;

    this.environmentId.set(envId);
    this.selectedTenant.set(tenant ?? '');
    this.selectedNamespace.set(namespace ?? '');
    this.searchControl.setValue(search, { emitEvent: false });
    this.loading.set(needsInitialLoad);
    this.loadError.set(null);

    return combineLatest([
      this.api.getEnvironmentHealth(envId),
      this.api.getCatalogSummary(envId),
      this.api.getTopics(envId, { search: search || undefined, tenant, namespace, page, pageSize })
    ]);
  }

  private handleLoadError(error: { error?: { message?: string } }) {
    const envId = this.environmentId();
    this.api.getEnvironmentSyncStatus(envId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (status) => {
          this.environmentSyncStatus.set(status);
          this.loading.set(false);

          if (status.syncStatus === 'SYNCING') {
            this.loadError.set(null);
            this.scheduleSyncRetry();
            return;
          }

          this.clearSyncRetry();
          this.loadError.set(error.error?.message ?? status.syncMessage ?? 'Unable to load topics right now.');
        },
        error: () => {
          this.clearSyncRetry();
          this.loadError.set(error.error?.message ?? 'Unable to load topics right now.');
          this.loading.set(false);
        }
      });
  }

  private scheduleSyncRetry() {
    if (this.syncRetryHandle !== null) {
      return;
    }

    this.syncRetryHandle = window.setTimeout(() => {
      this.syncRetryHandle = null;
      this.refresh$.next();
    }, TopicExplorerComponent.INITIAL_SYNC_POLL_MS);
  }

  private clearSyncRetry() {
    if (this.syncRetryHandle !== null) {
      window.clearTimeout(this.syncRetryHandle);
      this.syncRetryHandle = null;
    }
  }

  async selectNamespace(tenant: string, namespace: string) {
    this.activeTab.set('topics');
    await this.router.navigate([], {
      relativeTo: this.route,
      queryParams: this.demoMode.queryParams({
        tenant,
        namespace,
        page: '0'
      }),
      queryParamsHandling: 'merge'
    });
  }

  private goToTopicPage(page: number) {
    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        page: Math.max(0, page)
      },
      queryParamsHandling: 'merge'
    });
  }

  private toCsvList(value: string): string[] {
    return value.split(',')
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }

  functionsCount(): number {
    return this.platformSummary()?.functions.length ?? 0;
  }

  sourcesCount(): number {
    return this.platformSummary()?.sources.length ?? 0;
  }

  sinksCount(): number {
    return this.platformSummary()?.sinks.length ?? 0;
  }

  connectorsCount(): number {
    return this.platformSummary()?.connectors.length ?? 0;
  }

  private filterPlatformArtifacts<T extends { tenant: string | null; namespace: string | null }>(artifacts: T[]): T[] {
    const tenant = this.selectedTenant();
    const namespace = this.selectedNamespace();
    if (!tenant || !namespace) {
      return artifacts;
    }
    return artifacts.filter((item) => item.tenant === tenant && item.namespace === namespace);
  }
}
