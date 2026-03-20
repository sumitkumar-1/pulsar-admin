import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import { TenantYamlApplyResponse, TenantYamlPreviewResponse } from '../../core/models/api.models';

@Component({
  selector: 'app-tenant-yaml-sync',
  standalone: true,
  imports: [ReactiveFormsModule, RouterLink],
  templateUrl: './tenant-yaml-sync.component.html',
  styleUrl: './tenant-yaml-sync.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TenantYamlSyncComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);

  readonly environmentId = signal('');
  readonly tenant = signal('');
  readonly loading = signal(true);
  readonly validationState = signal<TenantYamlPreviewResponse | null>(null);
  readonly previewState = signal<TenantYamlPreviewResponse | null>(null);
  readonly applyState = signal<TenantYamlApplyResponse | null>(null);
  readonly error = signal<string | null>(null);
  readonly validating = signal(false);
  readonly previewing = signal(false);
  readonly applying = signal(false);

  readonly yamlForm = this.formBuilder.nonNullable.group({
    tenant: ['', [Validators.required]],
    yaml: ['', [Validators.required, Validators.minLength(20)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(([params, queryParams]) => {
        const envId = params.get('envId') ?? '';
        const tenant = queryParams.get('tenant') ?? '';
        this.environmentId.set(envId);
        this.tenant.set(tenant);
        this.yamlForm.patchValue({
          tenant,
          yaml: this.sampleYaml(tenant),
          reason: ''
        });
        this.loading.set(false);
      });
  }

  validateYaml() {
    if (this.yamlForm.invalid) {
      this.yamlForm.markAllAsTouched();
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.validating.set(true);
    this.error.set(null);
    this.validationState.set(null);

    this.api.validateTenantYaml(this.environmentId(), {
      tenant: form.tenant,
      yaml: form.yaml
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.validationState.set(response);
        this.validating.set(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to validate YAML.');
        this.validating.set(false);
      }
    });
  }

  previewYaml() {
    if (this.yamlForm.invalid) {
      this.yamlForm.markAllAsTouched();
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.previewing.set(true);
    this.error.set(null);
    this.previewState.set(null);
    this.applyState.set(null);

    this.api.previewTenantYaml(this.environmentId(), {
      tenant: form.tenant,
      yaml: form.yaml
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.previewState.set(response);
        this.previewing.set(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to preview YAML changes.');
        this.previewing.set(false);
      }
    });
  }

  applyYaml() {
    const preview = this.previewState();
    if (!preview?.previewId) {
      this.error.set('Generate a preview before applying changes.');
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.applying.set(true);
    this.error.set(null);
    this.applyState.set(null);

    this.api.applyTenantYaml(this.environmentId(), {
      previewId: preview.previewId,
      reason: form.reason
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.applyState.set(response);
        this.applying.set(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to apply YAML changes.');
        this.applying.set(false);
      }
    });
  }

  modeQueryParams() {
    return this.demoMode.queryParams({});
  }

  private sampleYaml(tenant: string): string {
    return `tenant: ${tenant || 'acme'}
namespaces:
  - name: orders
    policies:
      retentionTimeInMinutes: 10080
      retentionSizeInMb: 4096
      messageTtlInSeconds: 86400
      deduplicationEnabled: true
    topics:
      - name: order-events
        domain: persistent
        partitions: 0
        notes: Order lifecycle event stream
        policies:
          retentionTimeInMinutes: 10080
          retentionSizeInMb: 2048
          ttlInSeconds: 86400
          compactionThresholdInBytes: 104857600
          maxProducers: 16
          maxConsumers: 48
          maxSubscriptions: 24
`;
  }
}
