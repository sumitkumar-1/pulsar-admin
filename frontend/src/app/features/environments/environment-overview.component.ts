import { AsyncPipe, DatePipe, NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { EnvironmentDetails, EnvironmentSummary, EnvironmentUpsertRequest } from '../../core/models/api.models';
import { EnvironmentDialogComponent } from './environment-dialog.component';

@Component({
  selector: 'app-environment-overview',
  standalone: true,
  imports: [AsyncPipe, DatePipe, NgClass, ReactiveFormsModule, RouterLink, EnvironmentDialogComponent],
  templateUrl: './environment-overview.component.html',
  styleUrl: './environment-overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class EnvironmentOverviewComponent {
  private readonly api = inject(PulsarApiService);
  private readonly formBuilder = inject(FormBuilder);
  private readonly destroyRef = inject(DestroyRef);

  readonly environments$ = this.api.getEnvironments();
  readonly selectedEnvironmentId = signal<string | null>(null);
  readonly saving = signal(false);
  readonly actionState = signal<string | null>(null);
  readonly actionError = signal<string | null>(null);
  readonly editing = signal(false);
  readonly dialogOpen = signal(false);

  readonly environmentForm = this.formBuilder.nonNullable.group({
    id: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]{2,32}$/)]],
    name: ['', [Validators.required, Validators.maxLength(64)]],
    kind: ['dev', [Validators.required]],
    region: ['', [Validators.required, Validators.maxLength(64)]],
    clusterLabel: ['', [Validators.required, Validators.maxLength(64)]],
    summary: ['', [Validators.required, Validators.maxLength(255)]],
    brokerUrl: ['', [Validators.required, Validators.maxLength(255)]],
    adminUrl: ['', [Validators.required, Validators.maxLength(255)]],
    authMode: ['none', [Validators.required]],
    credentialReference: [''],
    tlsEnabled: [false]
  });

  constructor() {
    this.environmentForm.controls.authMode.valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.updateCredentialValidators());

    this.updateCredentialValidators();
  }

  statusClass(status: string): string {
    return status.toLowerCase();
  }

  openCreateDialog() {
    this.editing.set(false);
    this.dialogOpen.set(true);
    this.selectedEnvironmentId.set(null);
    this.actionState.set(null);
    this.actionError.set(null);
    this.environmentForm.reset({
      id: '',
      name: '',
      kind: 'dev',
      region: '',
      clusterLabel: '',
      summary: '',
      brokerUrl: '',
      adminUrl: '',
      authMode: 'none',
      credentialReference: '',
      tlsEnabled: false
    });
    this.environmentForm.controls.id.enable();
    this.updateCredentialValidators();
  }

  closeDialog() {
    this.dialogOpen.set(false);
  }

  editEnvironment(environmentId: string, event?: Event) {
    event?.stopPropagation();
    event?.preventDefault();
    this.actionState.set(null);
    this.actionError.set(null);

    this.api.getEnvironment(environmentId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (environment) => {
          this.populateForm(environment);
          this.dialogOpen.set(true);
        },
        error: (error: { error?: { message?: string } }) => {
          this.actionError.set(error.error?.message ?? 'Unable to load environment details.');
        }
      });
  }

  saveEnvironment() {
    if (this.environmentForm.invalid) {
      this.environmentForm.markAllAsTouched();
      return;
    }

    const wasEditing = this.editing();
    this.saving.set(true);
    this.actionState.set(null);
    this.actionError.set(null);

    const request = this.environmentForm.getRawValue() as EnvironmentUpsertRequest;
    const operation = this.editing()
      ? this.api.updateEnvironment(request.id, request)
      : this.api.createEnvironment(request);

    operation.pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (environment) => {
        this.saving.set(false);
        this.populateForm(environment);
        this.dialogOpen.set(false);
        this.actionState.set(wasEditing ? 'Environment updated.' : 'Environment created. Test the connection to trigger sync.');
      },
      error: (error: { error?: { message?: string } }) => {
        this.saving.set(false);
        this.actionError.set(error.error?.message ?? 'Unable to save this environment.');
      }
    });
  }

  testConnection(environmentId: string, event?: Event) {
    event?.stopPropagation();
    event?.preventDefault();
    this.actionState.set(null);
    this.actionError.set(null);

    this.api.testEnvironmentConnection(environmentId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (result) => {
          this.actionState.set(result.message);
        },
        error: (error: { error?: { message?: string } }) => {
          this.actionError.set(error.error?.message ?? 'Connection test failed.');
        }
      });
  }

  syncEnvironment(environmentId: string, event?: Event) {
    event?.stopPropagation();
    event?.preventDefault();
    this.actionState.set(null);
    this.actionError.set(null);

    this.api.syncEnvironment(environmentId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (status) => {
          this.actionState.set(status.syncMessage);
        },
        error: (error: { error?: { message?: string } }) => {
          this.actionError.set(error.error?.message ?? 'Unable to sync environment metadata.');
        }
      });
  }

  deleteEnvironment(environmentId: string, event?: Event) {
    event?.stopPropagation();
    event?.preventDefault();

    const confirmed = window.confirm(`Soft delete environment "${environmentId}"?`);

    if (!confirmed) {
      return;
    }

    this.actionState.set(null);
    this.actionError.set(null);

    this.api.deleteEnvironment(environmentId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: () => {
          if (this.selectedEnvironmentId() === environmentId) {
            this.dialogOpen.set(false);
            this.editing.set(false);
            this.selectedEnvironmentId.set(null);
          }
          this.actionState.set('Environment deleted from active navigation.');
        },
        error: (error: { error?: { message?: string } }) => {
          this.actionError.set(error.error?.message ?? 'Unable to delete this environment.');
        }
      });
  }

  private populateForm(environment: EnvironmentDetails) {
    this.editing.set(true);
    this.selectedEnvironmentId.set(environment.id);
    this.environmentForm.setValue({
      id: environment.id,
      name: environment.name,
      kind: environment.kind,
      region: environment.region,
      clusterLabel: environment.clusterLabel,
      summary: environment.summary,
      brokerUrl: environment.brokerUrl,
      adminUrl: environment.adminUrl,
      authMode: environment.authMode,
      credentialReference: environment.credentialReference ?? '',
      tlsEnabled: environment.tlsEnabled
    });
    this.environmentForm.controls.id.disable();
    this.updateCredentialValidators();
  }

  private updateCredentialValidators() {
    const authMode = this.environmentForm.controls.authMode.value;
    const credentialControl = this.environmentForm.controls.credentialReference;

    if (authMode === 'none') {
      credentialControl.clearValidators();
    } else {
      credentialControl.setValidators([Validators.required, Validators.maxLength(255)]);
    }

    credentialControl.updateValueAndValidity({ emitEvent: false });
  }
}
