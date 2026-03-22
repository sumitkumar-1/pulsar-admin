import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import {
  NamespaceYamlCurrentResponse,
  TenantYamlApplyResponse,
  TenantYamlPreviewResponse
} from '../../core/models/api.models';

@Component({
  selector: 'app-tenant-yaml-sync',
  standalone: true,
  imports: [DatePipe, ReactiveFormsModule, RouterLink],
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
  readonly namespace = signal('');
  readonly loading = signal(true);
  readonly loadingCurrentYaml = signal(false);
  readonly validationState = signal<TenantYamlPreviewResponse | null>(null);
  readonly previewState = signal<TenantYamlPreviewResponse | null>(null);
  readonly applyState = signal<TenantYamlApplyResponse | null>(null);
  readonly currentYamlState = signal<NamespaceYamlCurrentResponse | null>(null);
  readonly error = signal<string | null>(null);
  readonly validating = signal(false);
  readonly previewing = signal(false);
  readonly applying = signal(false);

  readonly yamlForm = this.formBuilder.nonNullable.group({
    tenant: ['', [Validators.required]],
    namespace: ['', [Validators.required]],
    yaml: ['', [Validators.required, Validators.minLength(20)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(([params, queryParams]) => {
        const envId = params.get('envId') ?? '';
        const tenant = queryParams.get('tenant') ?? '';
        const namespace = queryParams.get('namespace') ?? '';
        this.environmentId.set(envId);
        this.tenant.set(tenant);
        this.namespace.set(namespace);
        this.yamlForm.patchValue({
          tenant,
          namespace,
          yaml: '',
          reason: ''
        });
        this.validationState.set(null);
        this.previewState.set(null);
        this.applyState.set(null);
        this.loadCurrentYaml(true);
      });
  }

  loadCurrentYaml(forceEditorReset = false) {
    const environmentId = this.environmentId();
    const tenant = this.tenant();
    const namespace = this.namespace();

    if (!environmentId || !tenant || !namespace) {
      this.currentYamlState.set(null);
      this.loading.set(false);
      return;
    }

    this.loadingCurrentYaml.set(true);
    this.error.set(null);

    this.api.getCurrentNamespaceYaml(environmentId, tenant, namespace)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.currentYamlState.set(response);
          if (forceEditorReset || !this.yamlForm.controls.yaml.value.trim()) {
            this.yamlForm.patchValue({
              tenant: response.tenant,
              namespace: response.namespace,
              yaml: response.yaml,
              reason: this.yamlForm.controls.reason.value
            });
          }
          this.loadingCurrentYaml.set(false);
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.error.set(error.error?.message ?? 'Unable to load the current namespace YAML.');
          this.loadingCurrentYaml.set(false);
          this.loading.set(false);
        }
      });
  }

  resetToCurrentYaml() {
    const current = this.currentYamlState();
    if (!current) {
      this.loadCurrentYaml(true);
      return;
    }

    this.yamlForm.patchValue({
      tenant: current.tenant,
      namespace: current.namespace,
      yaml: current.yaml
    });
    this.validationState.set(null);
    this.previewState.set(null);
    this.applyState.set(null);
    this.error.set(null);
  }

  onYamlFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];

    if (!file) {
      return;
    }

    const fileName = file.name.toLowerCase();
    if (!fileName.endsWith('.yaml') && !fileName.endsWith('.yml')) {
      this.error.set('Upload a .yaml or .yml file.');
      input.value = '';
      return;
    }

    const reader = new FileReader();
    reader.onload = () => {
      this.yamlForm.patchValue({ yaml: String(reader.result ?? '') });
      this.validationState.set(null);
      this.previewState.set(null);
      this.applyState.set(null);
      this.error.set(null);
      input.value = '';
    };
    reader.onerror = () => {
      this.error.set('Unable to read the selected YAML file.');
      input.value = '';
    };
    reader.readAsText(file);
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
      namespace: form.namespace,
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
      namespace: form.namespace,
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

  backQueryParams() {
    return this.demoMode.queryParams({
      tenant: this.tenant(),
      namespace: this.namespace()
    });
  }
}
