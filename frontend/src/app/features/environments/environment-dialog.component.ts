import { NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
import { ReactiveFormsModule, FormGroup } from '@angular/forms';

@Component({
  selector: 'app-environment-dialog',
  standalone: true,
  imports: [NgClass, ReactiveFormsModule],
  templateUrl: './environment-dialog.component.html',
  styleUrl: './environment-dialog.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class EnvironmentDialogComponent {
  @Input({ required: true }) open = false;
  @Input({ required: true }) editing = false;
  @Input({ required: true }) saving = false;
  @Input({ required: true }) form!: FormGroup;

  @Output() cancel = new EventEmitter<void>();
  @Output() save = new EventEmitter<void>();

  isInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || control.touched);
  }

  authMode(): string {
    return String(this.form.get('authMode')?.value ?? 'none');
  }

  credentialHint(): string {
    const authMode = this.authMode();
    switch (authMode) {
      case 'token':
        return 'Use a raw token, `token:...`, or `env://PULSAR_TOKEN`.';
      case 'basic':
        return 'Use `username:password` or `env://PULSAR_BASIC_AUTH`.';
      case 'mtls':
        return 'mTLS is planned later. Use mock mode for now.';
      default:
        return 'No credential is needed for auth mode `none`.';
    }
  }
}
