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

  @Output() cancel = new EventEmitter<void>();
  @Output() save = new EventEmitter<void>();

  isInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || control.touched);
  }
}
