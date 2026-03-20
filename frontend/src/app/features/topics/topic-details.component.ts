import { DatePipe, DecimalPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest, Subscription, switchMap, timer } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import {
  ConsumeMessagesRequest,
  ConsumeMessagesResponse,
  CreateSubscriptionRequest,
  PublishMessageRequest,
  PublishMessageResponse,
  PeekMessagesResponse,
  ReplayCopyJobRequest,
  ReplayCopyJobStatusResponse,
  ResetCursorRequest,
  ResetCursorResponse,
  SkipMessagesRequest,
  SkipMessagesResponse,
  SubscriptionMutationResponse,
  TerminateTopicRequest,
  TerminateTopicResponse,
  TopicDetails,
  TopicPoliciesResponse,
  TopicPoliciesUpdateRequest,
  TopicPoliciesUpdateResponse,
  UnloadTopicRequest,
  UnloadTopicResponse
} from '../../core/models/api.models';

@Component({
  selector: 'app-topic-details',
  standalone: true,
  imports: [DatePipe, DecimalPipe, NgClass, ReactiveFormsModule, RouterLink, UpperCasePipe],
  templateUrl: './topic-details.component.html',
  styleUrl: './topic-details.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicDetailsComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);
  private replayJobPolling: Subscription | null = null;

  readonly details = signal<TopicDetails | null>(null);
  readonly environmentId = signal('');
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly activeWorkflow = signal<'peek' | 'reset' | 'skip' | 'unload' | 'terminate' | 'policies' | 'test-messages' | 'replay' | 'create-subscription' | 'delete-subscription' | null>(null);
  readonly peekState = signal<PeekMessagesResponse | null>(null);
  readonly peekLoading = signal(false);
  readonly peekError = signal<string | null>(null);
  readonly resetSaving = signal(false);
  readonly resetResult = signal<ResetCursorResponse | null>(null);
  readonly resetError = signal<string | null>(null);
  readonly skipSaving = signal(false);
  readonly skipResult = signal<SkipMessagesResponse | null>(null);
  readonly skipError = signal<string | null>(null);
  readonly unloadSaving = signal(false);
  readonly unloadResult = signal<UnloadTopicResponse | null>(null);
  readonly unloadError = signal<string | null>(null);
  readonly terminateSaving = signal(false);
  readonly terminateResult = signal<TerminateTopicResponse | null>(null);
  readonly terminateError = signal<string | null>(null);
  readonly topicPoliciesLoading = signal(false);
  readonly topicPoliciesSaving = signal(false);
  readonly topicPoliciesState = signal<TopicPoliciesResponse | null>(null);
  readonly topicPoliciesResult = signal<TopicPoliciesUpdateResponse | null>(null);
  readonly topicPoliciesError = signal<string | null>(null);
  readonly publishSaving = signal(false);
  readonly publishResult = signal<PublishMessageResponse | null>(null);
  readonly publishError = signal<string | null>(null);
  readonly consumeSaving = signal(false);
  readonly consumeResult = signal<ConsumeMessagesResponse | null>(null);
  readonly consumeError = signal<string | null>(null);
  readonly replaySaving = signal(false);
  readonly replayResult = signal<ReplayCopyJobStatusResponse | null>(null);
  readonly replayError = signal<string | null>(null);
  readonly subscriptionSaving = signal(false);
  readonly subscriptionResult = signal<SubscriptionMutationResponse | null>(null);
  readonly subscriptionError = signal<string | null>(null);
  readonly subscriptionPendingDelete = signal<string | null>(null);

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

  readonly unloadForm = this.formBuilder.nonNullable.group({
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly terminateForm = this.formBuilder.nonNullable.group({
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly topicPoliciesForm = this.formBuilder.nonNullable.group({
    retentionTimeInMinutes: [0, [Validators.min(0)]],
    retentionSizeInMb: [0, [Validators.min(0)]],
    ttlInSeconds: [0, [Validators.min(0)]],
    compactionThresholdInBytes: [0, [Validators.min(0)]],
    maxProducers: [0, [Validators.min(0)]],
    maxConsumers: [0, [Validators.min(0)]],
    maxSubscriptions: [0, [Validators.min(0)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly publishForm = this.formBuilder.nonNullable.group({
    key: [''],
    properties: [''],
    schemaMode: ['RAW'],
    payload: ['{\n  "event": "test"\n}', [Validators.required, Validators.maxLength(20000)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly consumeForm = this.formBuilder.nonNullable.group({
    ephemeral: [true],
    subscriptionName: [''],
    maxMessages: [5, [Validators.required, Validators.min(1), Validators.max(50)]],
    waitTimeSeconds: [5, [Validators.required, Validators.min(1), Validators.max(30)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly replayForm = this.formBuilder.nonNullable.group({
    subscriptionName: ['', [Validators.required]],
    operation: ['COPY' as 'COPY' | 'REPLAY', [Validators.required]],
    destinationTopicName: ['', [Validators.required]],
    messageLimit: [100, [Validators.required, Validators.min(1), Validators.max(5000)]],
    filterText: [''],
    messagesPerSecond: [50, [Validators.required, Validators.min(1), Validators.max(1000)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly createSubscriptionForm = this.formBuilder.nonNullable.group({
    subscriptionName: ['', [Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)]],
    initialPosition: ['LATEST' as 'EARLIEST' | 'LATEST', [Validators.required]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly deleteSubscriptionForm = this.formBuilder.nonNullable.group({
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
          this.replayForm.patchValue({
            subscriptionName: firstSubscription,
            operation: 'COPY',
            destinationTopicName: this.defaultDestinationTopic(details.fullName),
            messageLimit: 100,
            filterText: '',
            messagesPerSecond: 50,
            reason: ''
          });
          this.createSubscriptionForm.patchValue({
            subscriptionName: '',
            initialPosition: 'LATEST',
            reason: ''
          });
          this.deleteSubscriptionForm.patchValue({ reason: '' });
          this.terminateForm.patchValue({ reason: '' });
          this.topicPoliciesForm.patchValue({ reason: '' });
          this.publishForm.patchValue({ reason: '' });
          this.consumeForm.patchValue({
            ephemeral: true,
            subscriptionName: firstSubscription,
            maxMessages: 5,
            waitTimeSeconds: 5,
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

  openWorkflow(workflow: 'peek' | 'reset' | 'skip' | 'unload' | 'terminate' | 'policies' | 'test-messages' | 'replay' | 'create-subscription') {
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

    if (workflow === 'unload') {
      this.unloadError.set(null);
      this.unloadResult.set(null);
    }

    if (workflow === 'terminate') {
      this.terminateError.set(null);
      this.terminateResult.set(null);
      this.terminateForm.patchValue({ reason: '' });
    }

    if (workflow === 'policies') {
      const topic = this.details();
      if (topic) {
        this.topicPoliciesLoading.set(true);
        this.topicPoliciesError.set(null);
        this.topicPoliciesResult.set(null);
        this.api.getTopicPolicies(this.environmentId(), topic.fullName)
          .pipe(takeUntilDestroyed(this.destroyRef))
          .subscribe({
            next: (response) => {
              this.topicPoliciesState.set(response);
              this.topicPoliciesForm.patchValue({
                retentionTimeInMinutes: response.policies.retentionTimeInMinutes ?? 0,
                retentionSizeInMb: response.policies.retentionSizeInMb ?? 0,
                ttlInSeconds: response.policies.ttlInSeconds ?? 0,
                compactionThresholdInBytes: response.policies.compactionThresholdInBytes ?? 0,
                maxProducers: response.policies.maxProducers ?? 0,
                maxConsumers: response.policies.maxConsumers ?? 0,
                maxSubscriptions: response.policies.maxSubscriptions ?? 0,
                reason: ''
              });
              this.topicPoliciesLoading.set(false);
            },
            error: (error: { error?: { message?: string } }) => {
              this.topicPoliciesError.set(error.error?.message ?? 'Unable to load topic policies.');
              this.topicPoliciesLoading.set(false);
            }
          });
      }
    }

    if (workflow === 'test-messages') {
      const topic = this.details();
      this.publishError.set(null);
      this.publishResult.set(null);
      this.consumeError.set(null);
      this.consumeResult.set(null);
      this.publishForm.patchValue({ reason: '' });
      if (topic) {
        this.consumeForm.patchValue({
          subscriptionName: this.consumeForm.controls.subscriptionName.value || topic.subscriptions[0] || '',
          reason: ''
        });
      }
    }

    if (workflow === 'replay') {
      const topic = this.details();
      this.replayError.set(null);
      this.replayResult.set(null);
      this.stopReplayPolling();

      if (topic && topic.subscriptions.length > 0) {
        const currentSubscription = this.replayForm.controls.subscriptionName.value;
        this.replayForm.patchValue({
          subscriptionName: currentSubscription || topic.subscriptions[0] || '',
          operation: this.replayForm.controls.operation.value || 'COPY',
          destinationTopicName: this.replayForm.controls.destinationTopicName.value || this.defaultDestinationTopic(topic.fullName),
          messageLimit: this.replayForm.controls.messageLimit.value || 100,
          filterText: this.replayForm.controls.filterText.value,
          messagesPerSecond: this.replayForm.controls.messagesPerSecond.value || 50,
          reason: this.replayForm.controls.reason.value
        });
      }
    }

    if (workflow === 'create-subscription') {
      this.subscriptionError.set(null);
      this.subscriptionResult.set(null);
      this.subscriptionPendingDelete.set(null);
      this.createSubscriptionForm.patchValue({
        subscriptionName: '',
        initialPosition: 'LATEST',
        reason: this.createSubscriptionForm.controls.reason.value
      });
    }

    this.peekLoading.set(false);
    this.peekError.set(null);
  }

  openDeleteSubscriptionWorkflow(subscriptionName: string) {
    this.activeWorkflow.set('delete-subscription');
    this.subscriptionError.set(null);
    this.subscriptionResult.set(null);
    this.subscriptionPendingDelete.set(subscriptionName);
    this.deleteSubscriptionForm.patchValue({ reason: '' });
  }

  closeWorkflow() {
    this.activeWorkflow.set(null);
    this.stopReplayPolling();
    this.subscriptionPendingDelete.set(null);
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

  submitReplayCopyJob() {
    const topic = this.details();

    if (!topic) {
      this.replayError.set('Topic details are still loading.');
      return;
    }

    if (this.replayForm.invalid) {
      this.replayForm.markAllAsTouched();
      return;
    }

    const formValue = this.replayForm.getRawValue();
    const request: ReplayCopyJobRequest = {
      topicName: topic.fullName,
      subscriptionName: formValue.subscriptionName,
      operation: formValue.operation,
      destinationTopicName: formValue.destinationTopicName,
      messageLimit: Number(formValue.messageLimit),
      filterText: formValue.filterText?.trim() ? formValue.filterText.trim() : null,
      messagesPerSecond: Number(formValue.messagesPerSecond),
      reason: formValue.reason
    };

    this.replaySaving.set(true);
    this.replayError.set(null);
    this.replayResult.set(null);
    this.stopReplayPolling();

    this.api.createReplayCopyJob(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.replayResult.set(response);
          this.replaySaving.set(false);
          this.startReplayPolling(response.jobId);
        },
        error: (error: { error?: { message?: string } }) => {
          this.replayError.set(error.error?.message ?? 'Unable to start the replay or copy job.');
          this.replaySaving.set(false);
        }
      });
  }

  submitUnloadTopic() {
    const topic = this.details();

    if (!topic) {
      this.unloadError.set('Topic details are still loading.');
      return;
    }

    if (this.unloadForm.invalid) {
      this.unloadForm.markAllAsTouched();
      return;
    }

    const formValue = this.unloadForm.getRawValue();
    const request: UnloadTopicRequest = {
      topicName: topic.fullName,
      reason: formValue.reason
    };

    this.unloadSaving.set(true);
    this.unloadError.set(null);
    this.unloadResult.set(null);

    this.api.unloadTopic(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.unloadSaving.set(false);
          this.unloadResult.set(response);
          this.applyUpdatedTopic(response.topicDetails);
        },
        error: (error: { error?: { message?: string } }) => {
          this.unloadSaving.set(false);
          this.unloadError.set(error.error?.message ?? 'Unable to unload the topic right now.');
        }
      });
  }

  submitTerminateTopic() {
    const topic = this.details();
    if (!topic) {
      this.terminateError.set('Topic details are still loading.');
      return;
    }

    if (this.terminateForm.invalid) {
      this.terminateForm.markAllAsTouched();
      return;
    }

    const request: TerminateTopicRequest = {
      topicName: topic.fullName,
      reason: this.terminateForm.controls.reason.value
    };

    this.terminateSaving.set(true);
    this.terminateError.set(null);
    this.terminateResult.set(null);

    this.api.terminateTopic(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.terminateSaving.set(false);
          this.terminateResult.set(response);
          this.applyUpdatedTopic(response.topicDetails);
        },
        error: (error: { error?: { message?: string } }) => {
          this.terminateSaving.set(false);
          this.terminateError.set(error.error?.message ?? 'Unable to terminate this topic.');
        }
      });
  }

  submitTopicPolicies() {
    const topic = this.details();
    if (!topic) {
      this.topicPoliciesError.set('Topic details are still loading.');
      return;
    }

    if (this.topicPoliciesForm.invalid) {
      this.topicPoliciesForm.markAllAsTouched();
      return;
    }

    const form = this.topicPoliciesForm.getRawValue();
    const request: TopicPoliciesUpdateRequest = {
      topicName: topic.fullName,
      policies: {
        retentionTimeInMinutes: Number(form.retentionTimeInMinutes),
        retentionSizeInMb: Number(form.retentionSizeInMb),
        ttlInSeconds: Number(form.ttlInSeconds),
        compactionThresholdInBytes: Number(form.compactionThresholdInBytes),
        maxProducers: Number(form.maxProducers),
        maxConsumers: Number(form.maxConsumers),
        maxSubscriptions: Number(form.maxSubscriptions)
      },
      reason: form.reason
    };

    this.topicPoliciesSaving.set(true);
    this.topicPoliciesError.set(null);
    this.topicPoliciesResult.set(null);

    this.api.updateTopicPolicies(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.topicPoliciesSaving.set(false);
          this.topicPoliciesResult.set(response);
          this.topicPoliciesState.set({
            environmentId: response.environmentId,
            topicName: response.topicName,
            policies: response.policies,
            editable: true,
            message: response.message
          });
          this.applyUpdatedTopic(response.topicDetails);
          this.topicPoliciesForm.patchValue({ reason: '' });
        },
        error: (error: { error?: { message?: string } }) => {
          this.topicPoliciesSaving.set(false);
          this.topicPoliciesError.set(error.error?.message ?? 'Unable to update topic policies.');
        }
      });
  }

  submitPublishMessage() {
    const topic = this.details();
    if (!topic) {
      this.publishError.set('Topic details are still loading.');
      return;
    }

    if (this.publishForm.invalid) {
      this.publishForm.markAllAsTouched();
      return;
    }

    const form = this.publishForm.getRawValue();
    const request: PublishMessageRequest = {
      topicName: topic.fullName,
      key: form.key.trim() || null,
      properties: this.parseProperties(form.properties),
      schemaMode: form.schemaMode,
      payload: form.payload,
      reason: form.reason
    };

    this.publishSaving.set(true);
    this.publishError.set(null);
    this.publishResult.set(null);

    this.api.publishMessage(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.publishSaving.set(false);
          this.publishResult.set(response);
        },
        error: (error: { error?: { message?: string } }) => {
          this.publishSaving.set(false);
          this.publishError.set(error.error?.message ?? 'Unable to publish a test message.');
        }
      });
  }

  submitConsumeMessages() {
    const topic = this.details();
    if (!topic) {
      this.consumeError.set('Topic details are still loading.');
      return;
    }

    if (this.consumeForm.invalid) {
      this.consumeForm.markAllAsTouched();
      return;
    }

    const form = this.consumeForm.getRawValue();
    const request: ConsumeMessagesRequest = {
      topicName: topic.fullName,
      subscriptionName: form.ephemeral ? null : (form.subscriptionName.trim() || null),
      ephemeral: form.ephemeral,
      maxMessages: Number(form.maxMessages),
      waitTimeSeconds: Number(form.waitTimeSeconds),
      reason: form.reason
    };

    this.consumeSaving.set(true);
    this.consumeError.set(null);
    this.consumeResult.set(null);

    this.api.consumeMessages(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.consumeSaving.set(false);
          this.consumeResult.set(response);
        },
        error: (error: { error?: { message?: string } }) => {
          this.consumeSaving.set(false);
          this.consumeError.set(error.error?.message ?? 'Unable to consume test messages.');
        }
      });
  }

  submitCreateSubscription() {
    const topic = this.details();

    if (!topic) {
      this.subscriptionError.set('Topic details are still loading.');
      return;
    }

    if (this.createSubscriptionForm.invalid) {
      this.createSubscriptionForm.markAllAsTouched();
      return;
    }

    const formValue = this.createSubscriptionForm.getRawValue();
    const request: CreateSubscriptionRequest = {
      topicName: topic.fullName,
      subscriptionName: formValue.subscriptionName.trim(),
      initialPosition: formValue.initialPosition,
      reason: formValue.reason.trim() || null
    };

    this.subscriptionSaving.set(true);
    this.subscriptionError.set(null);
    this.subscriptionResult.set(null);

    this.api.createSubscription(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.subscriptionSaving.set(false);
          this.subscriptionResult.set(response);
          this.applyUpdatedTopic(response.topicDetails);
        },
        error: (error: { error?: { message?: string } }) => {
          this.subscriptionSaving.set(false);
          this.subscriptionError.set(error.error?.message ?? 'Unable to create the subscription.');
        }
      });
  }

  submitDeleteSubscription() {
    const topic = this.details();
    const subscriptionName = this.subscriptionPendingDelete();

    if (!topic || !subscriptionName) {
      this.subscriptionError.set('Choose a subscription to delete.');
      return;
    }

    if (this.deleteSubscriptionForm.invalid) {
      this.deleteSubscriptionForm.markAllAsTouched();
      return;
    }

    this.subscriptionSaving.set(true);
    this.subscriptionError.set(null);
    this.subscriptionResult.set(null);

    this.api.deleteSubscription(this.environmentId(), topic.fullName, subscriptionName)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.subscriptionSaving.set(false);
          this.subscriptionResult.set(response);
          this.applyUpdatedTopic(response.topicDetails);
          this.subscriptionPendingDelete.set(response.subscriptionName);
        },
        error: (error: { error?: { message?: string } }) => {
          this.subscriptionSaving.set(false);
          this.subscriptionError.set(error.error?.message ?? 'Unable to delete the subscription.');
        }
      });
  }

  createSubscriptionPreview(): string {
    const topic = this.details();
    const { subscriptionName, initialPosition } = this.createSubscriptionForm.getRawValue();

    if (!topic || !subscriptionName.trim()) {
      return 'Choose a subscription name and starting position to preview the create action.';
    }

    return `This will create subscription ${subscriptionName.trim()} on ${topic.topic} and start it at ${initialPosition.toLowerCase()}.`;
  }

  deleteSubscriptionPreview(): string {
    const topic = this.details();
    const subscriptionName = this.subscriptionPendingDelete();

    if (!topic || !subscriptionName) {
      return 'Choose a subscription to preview the delete action.';
    }

    return `This will delete subscription ${subscriptionName} from ${topic.topic}. Use this only when the subscription state is no longer needed.`;
  }

  replayPreview(): string {
    const topic = this.details();
    const { subscriptionName, operation, destinationTopicName, messageLimit, filterText, messagesPerSecond } =
      this.replayForm.getRawValue();

    if (!topic || !subscriptionName || !destinationTopicName) {
      return 'Choose a subscription and destination topic to preview this replay or copy job.';
    }

    const filterSegment = filterText?.trim()
      ? ` filtered by "${filterText.trim()}"`
      : ' without additional filtering';

    return `${operation === 'COPY' ? 'Copy' : 'Replay'} up to ${messageLimit} messages from ${topic.topic} for subscription ${subscriptionName}${filterSegment} into ${destinationTopicName} at up to ${messagesPerSecond} msg/s.`;
  }

  unloadPreview(): string {
    const topic = this.details();

    if (!topic) {
      return 'Topic details are still loading.';
    }

    return `This will unload ${topic.topic} from its current broker owner so Pulsar can rebalance ownership and refresh the serving path. Use this when a topic looks stuck or you need a clean handoff after incident work.`;
  }

  terminatePreview(): string {
    const topic = this.details();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return `This will terminate ${topic.topic} so producers stop appending after the current retained tail. Use it only when the topic should become append-complete.`;
  }

  topicPolicyPreview(): string {
    const topic = this.details();
    const form = this.topicPoliciesForm.getRawValue();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return `This will align retention (${form.retentionTimeInMinutes} min / ${form.retentionSizeInMb} MB), TTL (${form.ttlInSeconds}s), compaction (${form.compactionThresholdInBytes} bytes), and producer/consumer/subscription limits for ${topic.topic}.`;
  }

  publishPreview(): string {
    const topic = this.details();
    const form = this.publishForm.getRawValue();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return `This will publish a bounded ${form.schemaMode.toLowerCase()} test message to ${topic.topic}${form.key.trim() ? ` with key ${form.key.trim()}` : ''}.`;
  }

  consumePreview(): string {
    const topic = this.details();
    const form = this.consumeForm.getRawValue();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return form.ephemeral
      ? `This will read up to ${form.maxMessages} messages from ${topic.topic} using an ephemeral test flow over ${form.waitTimeSeconds} seconds.`
      : `This will consume up to ${form.maxMessages} messages from ${topic.topic} using subscription ${form.subscriptionName || 'the selected subscription'} over ${form.waitTimeSeconds} seconds.`;
  }

  replayCanRefresh(): boolean {
    const result = this.replayResult();
    return !!result && (result.status === 'QUEUED' || result.status === 'RUNNING');
  }

  refreshReplayJob() {
    const result = this.replayResult();
    if (!result) {
      return;
    }

    this.api.getReplayCopyJob(this.environmentId(), result.jobId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.replayResult.set(response);
        },
        error: (error: { error?: { message?: string } }) => {
          this.replayError.set(error.error?.message ?? 'Unable to refresh replay job status.');
        }
      });
  }

  private startReplayPolling(jobId: string) {
    this.stopReplayPolling();

    this.replayJobPolling = timer(900, 1200)
      .pipe(
        switchMap(() => this.api.getReplayCopyJob(this.environmentId(), jobId)),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: (response) => {
          this.replayResult.set(response);
          if (response.status === 'COMPLETED' || response.status === 'FAILED') {
            this.stopReplayPolling();
          }
        },
        error: () => {
          this.stopReplayPolling();
        }
      });
  }

  private stopReplayPolling() {
    this.replayJobPolling?.unsubscribe();
    this.replayJobPolling = null;
  }

  private defaultDestinationTopic(sourceTopicName: string): string {
    return sourceTopicName.endsWith('/replay-lab')
      ? sourceTopicName.replace('/replay-lab', '/replay-output')
      : sourceTopicName.replace(/\/([^/]+)$/, '/replay-lab');
  }

  private toIsoTimestamp(value: string): string {
    if (!value) {
      return '';
    }

    return new Date(value).toISOString();
  }

  modeQueryParams() {
    return this.demoMode.queryParams({});
  }

  parseProperties(value: string): Record<string, string> {
    return value.split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .reduce<Record<string, string>>((accumulator, line) => {
        const [key, ...rest] = line.split('=');
        if (key?.trim()) {
          accumulator[key.trim()] = rest.join('=').trim();
        }
        return accumulator;
      }, {});
  }

  private applyUpdatedTopic(topic: TopicDetails) {
    this.details.set(topic);
    const firstSubscription = topic.subscriptions[0] ?? '';
    this.resetForm.patchValue({ subscriptionName: firstSubscription });
    this.skipForm.patchValue({ subscriptionName: firstSubscription });
    this.replayForm.patchValue({ subscriptionName: firstSubscription });
  }
}
