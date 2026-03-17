import { DatePipe, DecimalPipe, NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import {
  PeekMessagesResponse,
  ResetCursorRequest,
  ResetCursorResponse,
  SkipMessagesRequest,
  SkipMessagesResponse,
  TopicDetails
} from '../../core/models/api.models';

@Component({
  selector: 'app-topic-details',
  standalone: true,
  imports: [DatePipe, DecimalPipe, NgClass, ReactiveFormsModule, RouterLink],
  templateUrl: './topic-details.component.html',
  styleUrl: './topic-details.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicDetailsComponent {
  private readonly api = inject(PulsarApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);

  readonly details = signal<TopicDetails | null>(null);
  readonly environmentId = signal('');
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly activeWorkflow = signal<'peek' | 'reset' | 'skip' | 'replay' | null>(null);
  readonly peekState = signal<PeekMessagesResponse | null>(null);
  readonly peekLoading = signal(false);
  readonly peekError = signal<string | null>(null);
  readonly resetSaving = signal(false);
  readonly resetResult = signal<ResetCursorResponse | null>(null);
  readonly resetError = signal<string | null>(null);
  readonly skipSaving = signal(false);
  readonly skipResult = signal<SkipMessagesResponse | null>(null);
  readonly skipError = signal<string | null>(null);

  readonly resetForm = this.formBuilder.nonNullable.group({
    subscriptionName: ['', [Validators.required]],
    target: ['LATEST', [Validators.required]],
    timestamp: [''],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly skipForm = this.formBuilder.nonNullable.group({
    subscriptionName: ['', [Validators.required]],
    messageCount: [1, [Validators.required, Validators.min(1), Validators.max(5000)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(
        switchMap(([params, queryParams]) => {
        const envId = params.get('envId') ?? '';
        const topic = queryParams.get('topic') ?? '';

        this.environmentId.set(envId);
        this.loading.set(true);
        this.loadError.set(null);

        return this.api.getTopicDetails(envId, topic);
      }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: (details) => {
          this.details.set(details);
          const firstSubscription = details.subscriptions[0] ?? '';
          this.resetForm.patchValue({
            subscriptionName: firstSubscription,
            target: 'LATEST',
            timestamp: '',
            reason: ''
          });
          this.skipForm.patchValue({
            subscriptionName: firstSubscription,
            messageCount: 1,
            reason: ''
          });
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.loadError.set(error.error?.message ?? 'Unable to load topic details.');
          this.loading.set(false);
        }
      });
  }

  healthClass(status: string): string {
    return status.toLowerCase();
  }

  openWorkflow(workflow: 'peek' | 'reset' | 'skip' | 'replay') {
    this.activeWorkflow.set(workflow);

    if (workflow === 'peek') {
      const topic = this.details();

      if (!topic) {
        this.peekError.set('Topic details are still loading.');
        return;
      }

      this.peekLoading.set(true);
      this.peekError.set(null);
      this.peekState.set(null);

      this.api.peekMessages(this.environmentId(), topic.fullName, 5)
        .pipe(takeUntilDestroyed(this.destroyRef))
        .subscribe({
          next: (response) => {
            this.peekState.set(response);
            this.peekLoading.set(false);
          },
          error: (error: { error?: { message?: string } }) => {
            this.peekError.set(error.error?.message ?? 'Unable to peek messages right now.');
            this.peekLoading.set(false);
          }
        });
      return;
    }

    if (workflow === 'reset') {
      const topic = this.details();
      this.resetError.set(null);
      this.resetResult.set(null);

      if (topic && topic.subscriptions.length > 0) {
        const currentSubscription = this.resetForm.controls.subscriptionName.value;
        this.resetForm.patchValue({
          subscriptionName: currentSubscription || topic.subscriptions[0] || '',
          target: this.resetForm.controls.target.value || 'LATEST',
          timestamp: this.resetForm.controls.timestamp.value,
          reason: this.resetForm.controls.reason.value
        });
      }
    }

    if (workflow === 'skip') {
      const topic = this.details();
      this.skipError.set(null);
      this.skipResult.set(null);

      if (topic && topic.subscriptions.length > 0) {
        const currentSubscription = this.skipForm.controls.subscriptionName.value;
        this.skipForm.patchValue({
          subscriptionName: currentSubscription || topic.subscriptions[0] || '',
          messageCount: this.skipForm.controls.messageCount.value || 1,
          reason: this.skipForm.controls.reason.value
        });
      }
    }

    this.peekLoading.set(false);
    this.peekError.set(null);
  }

  closeWorkflow() {
    this.activeWorkflow.set(null);
  }

  submitResetCursor() {
    const topic = this.details();

    if (!topic) {
      this.resetError.set('Topic details are still loading.');
      return;
    }

    if (this.resetForm.invalid) {
      this.resetForm.markAllAsTouched();
      return;
    }

    const formValue = this.resetForm.getRawValue();
    const request: ResetCursorRequest = {
      topicName: topic.fullName,
      subscriptionName: formValue.subscriptionName,
      target: formValue.target,
      timestamp: formValue.target === 'TIMESTAMP' ? this.toIsoTimestamp(formValue.timestamp) : null,
      reason: formValue.reason
    };

    this.resetSaving.set(true);
    this.resetError.set(null);
    this.resetResult.set(null);

    this.api.resetCursor(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.resetResult.set(response);
          this.resetSaving.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.resetError.set(error.error?.message ?? 'Unable to reset the subscription cursor.');
          this.resetSaving.set(false);
        }
      });
  }

  resetPreview(): string {
    const topic = this.details();
    const { subscriptionName, target, timestamp } = this.resetForm.getRawValue();

    if (!topic || !subscriptionName) {
      return 'Choose a subscription to preview the reset action.';
    }

    if (target === 'EARLIEST') {
      return `This will move subscription ${subscriptionName} on ${topic.topic} to the earliest available message.`;
    }

    if (target === 'TIMESTAMP') {
      return `This will move subscription ${subscriptionName} on ${topic.topic} to messages published after ${timestamp || 'the selected timestamp'}.`;
    }

    return `This will move subscription ${subscriptionName} on ${topic.topic} to the latest position and clear current backlog processing state.`;
  }

  submitSkipMessages() {
    const topic = this.details();

    if (!topic) {
      this.skipError.set('Topic details are still loading.');
      return;
    }

    if (this.skipForm.invalid) {
      this.skipForm.markAllAsTouched();
      return;
    }

    const formValue = this.skipForm.getRawValue();
    const request: SkipMessagesRequest = {
      topicName: topic.fullName,
      subscriptionName: formValue.subscriptionName,
      messageCount: Number(formValue.messageCount),
      reason: formValue.reason
    };

    this.skipSaving.set(true);
    this.skipError.set(null);
    this.skipResult.set(null);

    this.api.skipMessages(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.skipResult.set(response);
          this.skipSaving.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.skipError.set(error.error?.message ?? 'Unable to skip messages for this subscription.');
          this.skipSaving.set(false);
        }
      });
  }

  skipPreview(): string {
    const topic = this.details();
    const { subscriptionName, messageCount } = this.skipForm.getRawValue();

    if (!topic || !subscriptionName) {
      return 'Choose a subscription and message count to preview the skip action.';
    }

    return `This will advance subscription ${subscriptionName} on ${topic.topic} by ${messageCount} messages. Use this for bounded poison-message cleanup, not broad backlog removal.`;
  }

  private toIsoTimestamp(value: string): string {
    if (!value) {
      return '';
    }

    return new Date(value).toISOString();
  }
}
