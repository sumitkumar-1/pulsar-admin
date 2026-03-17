import { NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';

@Component({
  selector: 'app-create-topic-dialog',
  standalone: true,
  imports: [NgClass, ReactiveFormsModule],
  templateUrl: './create-topic-dialog.component.html',
  styleUrl: './create-topic-dialog.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CreateTopicDialogComponent {
  @Input({ required: true }) open = false;
  @Input({ required: true }) saving = false;
  @Input({ required: true }) form!: FormGroup;
  @Input() namespaceOptions: Array<{ tenant: string; namespace: string }> = [];

  @Output() cancel = new EventEmitter<void>();
  @Output() save = new EventEmitter<void>();
  @Output() namespaceSelected = new EventEmitter<string>();

  isInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || control.touched);
  }

  isPartitioned(): boolean {
    return Number(this.form.get('partitions')?.value ?? 0) > 0;
  }

  onNamespaceSelection(value: string) {
    this.namespaceSelected.emit(value);
  }
}
