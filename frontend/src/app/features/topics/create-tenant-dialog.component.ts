import { NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';

@Component({
  selector: 'app-create-tenant-dialog',
  standalone: true,
  imports: [NgClass, ReactiveFormsModule],
  templateUrl: './create-tenant-dialog.component.html',
  styleUrl: './create-tenant-dialog.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CreateTenantDialogComponent {
  @Input({ required: true }) open = false;
  @Input({ required: true }) saving = false;
  @Input({ required: true }) form!: FormGroup;
  @Input() defaultClusterLabel = '';
  @Input() mode: 'create' | 'edit' = 'create';
  @Input() tenantLocked = false;

  @Output() cancel = new EventEmitter<void>();
  @Output() save = new EventEmitter<void>();

  isInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || control.touched);
  }

  title(): string {
    return this.mode === 'create' ? 'Add a tenant to this environment' : 'Update tenant access and cluster settings';
  }

  eyebrow(): string {
    return this.mode === 'create' ? 'Create Tenant' : 'Edit Tenant';
  }

  submitLabel(): string {
    if (this.saving) {
      return this.mode === 'create' ? 'Creating...' : 'Saving...';
    }
    return this.mode === 'create' ? 'Create Tenant' : 'Save Tenant';
  }
}
