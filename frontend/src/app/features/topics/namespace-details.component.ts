import { DatePipe, DecimalPipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import { NamespaceDeleteRequest, NamespaceDetails, NamespacePoliciesUpdateRequest, TopicListItem } from '../../core/models/api.models';

@Component({
  selector: 'app-namespace-details',
  standalone: true,
  imports: [DatePipe, DecimalPipe, ReactiveFormsModule, RouterLink],
  templateUrl: './namespace-details.component.html',
  styleUrl: './namespace-details.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NamespaceDetailsComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);

  readonly environmentId = signal('');
  readonly namespaceDetails = signal<NamespaceDetails | null>(null);
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly saveError = signal<string | null>(null);
  readonly saveSuccess = signal<string | null>(null);
  readonly saving = signal(false);
  readonly deleteDialogOpen = signal(false);
  readonly deleteSaving = signal(false);

  readonly policyForm = this.formBuilder.nonNullable.group({
    retentionTimeInMinutes: [0, [Validators.min(0)]],
    retentionSizeInMb: [0, [Validators.min(0)]],
    messageTtlInSeconds: [0, [Validators.min(0)]],
    deduplicationEnabled: [false],
    backlogQuotaLimitInBytes: [0, [Validators.min(0)]],
    backlogQuotaLimitTimeInSeconds: [0, [Validators.min(0)]],
    dispatchRatePerTopicInMsg: [0, [Validators.min(0)]],
    dispatchRatePerTopicInByte: [0, [Validators.min(0)]],
    publishRateInMsg: [0, [Validators.min(0)]],
    publishRateInByte: [0, [Validators.min(0)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });
  readonly deleteForm = this.formBuilder.nonNullable.group({
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly summaryText = computed(() => {
    const details = this.namespaceDetails();
    if (!details) {
      return 'Loading namespace details.';
    }
    return `${details.topicCount} topics are currently tracked inside ${details.tenant}/${details.namespace}.`;
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(
        switchMap(([params, queryParams]) => {
          const envId = params.get('envId') ?? '';
          const tenant = queryParams.get('tenant') ?? '';
          const namespace = queryParams.get('namespace') ?? '';
          this.environmentId.set(envId);
          this.loading.set(true);
          this.loadError.set(null);
          return this.api.getNamespaceDetails(envId, tenant, namespace);
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: (details) => {
          this.namespaceDetails.set(details);
          this.policyForm.patchValue({
            retentionTimeInMinutes: details.policies.retentionTimeInMinutes ?? 0,
            retentionSizeInMb: details.policies.retentionSizeInMb ?? 0,
            messageTtlInSeconds: details.policies.messageTtlInSeconds ?? 0,
            deduplicationEnabled: details.policies.deduplicationEnabled ?? false,
            backlogQuotaLimitInBytes: details.policies.backlogQuotaLimitInBytes ?? 0,
            backlogQuotaLimitTimeInSeconds: details.policies.backlogQuotaLimitTimeInSeconds ?? 0,
            dispatchRatePerTopicInMsg: details.policies.dispatchRatePerTopicInMsg ?? 0,
            dispatchRatePerTopicInByte: details.policies.dispatchRatePerTopicInByte ?? 0,
            publishRateInMsg: details.policies.publishRateInMsg ?? 0,
            publishRateInByte: details.policies.publishRateInByte ?? 0,
            reason: ''
          });
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.loadError.set(error.error?.message ?? 'Unable to load namespace details.');
          this.loading.set(false);
        }
      });
  }

  openTopic(topic: TopicListItem) {
    void this.router.navigate(['/environments', this.environmentId(), 'topic-details'], {
      queryParams: this.demoMode.queryParams({ topic: topic.fullName })
    });
  }

  openNamespaceYaml() {
    const details = this.namespaceDetails();
    if (!details) {
      return;
    }
    void this.router.navigate(['/environments', this.environmentId(), 'namespace-yaml'], {
      queryParams: this.demoMode.queryParams({ tenant: details.tenant, namespace: details.namespace })
    });
  }

  submitPolicies() {
    const details = this.namespaceDetails();
    if (!details) {
      return;
    }

    if (this.policyForm.invalid) {
      this.policyForm.markAllAsTouched();
      return;
    }

    const form = this.policyForm.getRawValue();
    const request: NamespacePoliciesUpdateRequest = {
      tenant: details.tenant,
      namespace: details.namespace,
      policies: {
        retentionTimeInMinutes: form.retentionTimeInMinutes,
        retentionSizeInMb: form.retentionSizeInMb,
        messageTtlInSeconds: form.messageTtlInSeconds,
        deduplicationEnabled: form.deduplicationEnabled,
        backlogQuotaLimitInBytes: form.backlogQuotaLimitInBytes,
        backlogQuotaLimitTimeInSeconds: form.backlogQuotaLimitTimeInSeconds,
        dispatchRatePerTopicInMsg: form.dispatchRatePerTopicInMsg,
        dispatchRatePerTopicInByte: form.dispatchRatePerTopicInByte,
        publishRateInMsg: form.publishRateInMsg,
        publishRateInByte: form.publishRateInByte
      },
      reason: form.reason
    };

    this.saving.set(true);
    this.saveError.set(null);
    this.saveSuccess.set(null);

    this.api.updateNamespacePolicies(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.namespaceDetails.set(response.namespaceDetails);
          this.saveSuccess.set(response.message);
          this.saving.set(false);
          this.policyForm.patchValue({ reason: '' });
        },
        error: (error: { error?: { message?: string } }) => {
          this.saveError.set(error.error?.message ?? 'Unable to update namespace policies.');
          this.saving.set(false);
        }
      });
  }

  openDeleteDialog() {
    this.deleteForm.reset({ reason: '' });
    this.deleteDialogOpen.set(true);
    this.saveError.set(null);
    this.saveSuccess.set(null);
  }

  closeDeleteDialog() {
    if (!this.deleteSaving()) {
      this.deleteDialogOpen.set(false);
    }
  }

  submitDeleteNamespace() {
    const details = this.namespaceDetails();
    if (!details) {
      return;
    }

    if (this.deleteForm.invalid) {
      this.deleteForm.markAllAsTouched();
      return;
    }

    this.deleteSaving.set(true);
    this.saveError.set(null);
    const request: NamespaceDeleteRequest = {
      tenant: details.tenant,
      namespace: details.namespace,
      reason: this.deleteForm.controls.reason.value.trim()
    };

    this.api.deleteNamespace(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.deleteSaving.set(false);
          this.deleteDialogOpen.set(false);
          this.saveSuccess.set(response.message);
          void this.router.navigate(['/environments', this.environmentId(), 'topics'], {
            queryParams: this.demoMode.queryParams({})
          });
        },
        error: (error: { error?: { message?: string } }) => {
          this.deleteSaving.set(false);
          this.saveError.set(error.error?.message ?? 'Unable to delete namespace.');
        }
      });
  }

  modeQueryParams(extra: Record<string, string> = {}) {
    return this.demoMode.queryParams(extra);
  }
}
