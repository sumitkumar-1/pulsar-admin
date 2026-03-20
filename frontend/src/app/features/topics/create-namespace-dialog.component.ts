import { NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';

@Component({
  selector: 'app-create-namespace-dialog',
  standalone: true,
  imports: [NgClass, ReactiveFormsModule],
  templateUrl: './create-namespace-dialog.component.html',
  styleUrl: './create-namespace-dialog.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CreateNamespaceDialogComponent {
  @Input({ required: true }) open = false;
  @Input({ required: true }) saving = false;
  @Input({ required: true }) form!: FormGroup;
  @Input() tenantOptions: string[] = [];

  @Output() cancel = new EventEmitter<void>();
  @Output() save = new EventEmitter<void>();
  @Output() tenantSelected = new EventEmitter<string>();

  isInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || control.touched);
  }
}
