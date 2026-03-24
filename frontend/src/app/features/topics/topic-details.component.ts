import { DatePipe, DecimalPipe, JsonPipe, NgClass, UpperCasePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { combineLatest, Subscription, switchMap, timer } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import {
  ConsumeMessagesRequest,
  ConsumeMessagesResponse,
  CreateSubscriptionRequest,
  ExportMessagesResponse,
  PublishMessageRequest,
  PublishMessageResponse,
  PeekMessagesResponse,
  ReplayCopyJobEventResponse,
  ReplayCopyJobRequest,
  ReplayCopySearchExportResponse,
  ReplayCopyJobStatusResponse,
  ResetCursorRequest,
  ResetCursorResponse,
  SchemaDeleteRequest,
  SchemaDetails,
  SchemaMutationResponse,
  SchemaUpdateRequest,
  SkipMessagesRequest,
  SkipMessagesResponse,
  SubscriptionMutationResponse,
  TopicDeleteRequest,
  TopicDeleteResponse,
  TerminateTopicRequest,
  TerminateTopicResponse,
  TopicDetails,
  TopicPoliciesResponse,
  TopicPoliciesUpdateRequest,
  TopicPoliciesUpdateResponse,
  UnloadTopicRequest,
  UnloadTopicResponse
} from '../../core/models/api.models';

type ConsumeSubscriptionMode = 'AUTO_EPHEMERAL' | 'REUSE_EXISTING' | 'CREATE_NAMED';

@Component({
  selector: 'app-topic-details',
  standalone: true,
  imports: [DatePipe, DecimalPipe, JsonPipe, NgClass, ReactiveFormsModule, RouterLink, UpperCasePipe],
  templateUrl: './topic-details.component.html',
  styleUrl: './topic-details.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicDetailsComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);
  private replayJobPolling: Subscription | null = null;

  readonly details = signal<TopicDetails | null>(null);
  readonly environmentId = signal('');
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);
  readonly actionFeedback = signal<{ kind: 'success' | 'error'; message: string } | null>(null);
  readonly activeTab = signal<'overview' | 'subscriptions' | 'partitions' | 'schema' | 'operations'>('overview');
  readonly activeWorkflow = signal<'peek' | 'reset' | 'skip' | 'unload' | 'terminate' | 'policies' | 'schema' | 'test-messages' | 'replay' | 'dlq' | 'delete-topic' | 'create-subscription' | 'delete-subscription' | null>(null);
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
  readonly deleteTopicSaving = signal(false);
  readonly deleteTopicResult = signal<TopicDeleteResponse | null>(null);
  readonly deleteTopicError = signal<string | null>(null);
  readonly topicPoliciesLoading = signal(false);
  readonly topicPoliciesSaving = signal(false);
  readonly topicPoliciesState = signal<TopicPoliciesResponse | null>(null);
  readonly topicPoliciesResult = signal<TopicPoliciesUpdateResponse | null>(null);
  readonly topicPoliciesError = signal<string | null>(null);
  readonly publishSaving = signal(false);
  readonly publishResult = signal<PublishMessageResponse | null>(null);
  readonly publishError = signal<string | null>(null);
  readonly exportSaving = signal(false);
  readonly exportError = signal<string | null>(null);
  readonly exportResult = signal<ExportMessagesResponse | null>(null);
  readonly consumeSaving = signal(false);
  readonly consumeResult = signal<ConsumeMessagesResponse | null>(null);
  readonly consumeError = signal<string | null>(null);
  readonly replaySaving = signal(false);
  readonly replayResult = signal<ReplayCopyJobStatusResponse | null>(null);
  readonly replayError = signal<string | null>(null);
  readonly replayEvents = signal<ReplayCopyJobEventResponse[]>([]);
  readonly replayEventsError = signal<string | null>(null);
  readonly replayFilterFile = signal<File | null>(null);
  readonly replayExporting = signal(false);
  readonly replayExportError = signal<string | null>(null);
  readonly replayExportResult = signal<ReplayCopySearchExportResponse | null>(null);
  readonly subscriptionSaving = signal(false);
  readonly subscriptionResult = signal<SubscriptionMutationResponse | null>(null);
  readonly subscriptionError = signal<string | null>(null);
  readonly subscriptionPendingDelete = signal<string | null>(null);
  readonly replayDestinationTopic = signal<TopicDetails | null>(null);
  readonly replayDestinationLoadError = signal<string | null>(null);
  readonly schemaLoading = signal(false);
  readonly schemaSaving = signal(false);
  readonly schemaDeleting = signal(false);
  readonly schemaState = signal<SchemaDetails | null>(null);
  readonly schemaResult = signal<SchemaMutationResponse | null>(null);
  readonly schemaError = signal<string | null>(null);

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

  readonly schemaForm = this.formBuilder.nonNullable.group({
    schemaType: ['JSON', [Validators.required]],
    compatibility: ['FULL'],
    definition: ['{\n  "type": "record",\n  "name": "TopicEvent",\n  "namespace": "com.pulsaradmin",\n  "fields": [\n    { "name": "id", "type": "string" }\n  ]\n}', [Validators.required, Validators.maxLength(40000)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly schemaDeleteForm = this.formBuilder.nonNullable.group({
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
    subscriptionMode: ['AUTO_EPHEMERAL' as ConsumeSubscriptionMode, [Validators.required]],
    subscriptionName: [''],
    namedSubscriptionName: ['', [Validators.pattern(/[A-Za-z0-9._-]+/)]],
    maxMessages: [5, [Validators.required, Validators.min(1), Validators.max(50)]],
    waitTimeSeconds: [5, [Validators.required, Validators.min(1), Validators.max(30)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });

  readonly replayForm = this.formBuilder.nonNullable.group({
    subscriptionName: ['', [Validators.required]],
    operationMode: ['ACK_AND_MOVE' as 'ACK_ONLY' | 'ACK_AND_MOVE' | 'SEARCH_ONLY', [Validators.required]],
    destinationTopicName: [''],
    matchField: ['feedId'],
    autoReplicateSchema: [true],
    messageLimit: [100, [Validators.required, Validators.min(1), Validators.max(1_000_000)]],
    messageKey: [''],
    propertyFilters: [''],
    filterText: [''],
    messagesPerSecond: [50, [Validators.required, Validators.min(1), Validators.max(5000)]],
    reason: ['', [Validators.required, Validators.maxLength(240)]]
  });
  readonly deleteTopicForm = this.formBuilder.nonNullable.group({
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
            operationMode: 'ACK_AND_MOVE',
            destinationTopicName: this.defaultDestinationTopic(details.fullName),
            messageLimit: 100,
            matchField: 'feedId',
            autoReplicateSchema: true,
            messageKey: '',
            propertyFilters: '',
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
            subscriptionMode: 'AUTO_EPHEMERAL',
            subscriptionName: firstSubscription,
            namedSubscriptionName: '',
            maxMessages: 5,
            waitTimeSeconds: 5,
            reason: ''
          });
          this.loading.set(false);
          this.loadReplayDestinationPreview(this.replayForm.controls.destinationTopicName.value);
        },
        error: (error: { error?: { message?: string } }) => {
          this.loadError.set(error.error?.message ?? 'Unable to load topic details.');
          this.loading.set(false);
        }
      });

    this.replayForm.controls.destinationTopicName.valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((value) => this.loadReplayDestinationPreview(value));

    this.replayForm.controls.operationMode.valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => {
        this.syncReplayDestinationValidators();
        this.loadReplayDestinationPreview(this.replayForm.controls.destinationTopicName.value);
      });
    this.syncReplayDestinationValidators();

    this.consumeForm.controls.subscriptionMode.valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.syncConsumeSubscriptionValidators());
    this.syncConsumeSubscriptionValidators();
  }

  healthClass(status: string): string {
    return status.toLowerCase();
  }

  setActiveTab(tab: 'overview' | 'subscriptions' | 'partitions' | 'schema' | 'operations') {
    this.activeTab.set(tab);
  }

  canTerminateTopic(): boolean {
    return !this.details()?.partitioned;
  }

  openWorkflow(workflow: 'peek' | 'reset' | 'skip' | 'unload' | 'terminate' | 'policies' | 'schema' | 'test-messages' | 'replay' | 'dlq' | 'delete-topic' | 'create-subscription') {
    this.actionFeedback.set(null);
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
      if (!this.canTerminateTopic()) {
        this.terminateError.set('Partitioned topics cannot be terminated through Pulsar. Use unload, policies, or partition management workflows instead.');
        this.terminateResult.set(null);
        return;
      }
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

    if (workflow === 'schema') {
      const currentTopic = this.details();
      if (!currentTopic) {
        this.schemaError.set('Topic details are still loading.');
        return;
      }
      this.schemaLoading.set(true);
      this.schemaError.set(null);
      this.schemaResult.set(null);
      this.api.getSchemaDetails(this.environmentId(), currentTopic.fullName)
        .pipe(takeUntilDestroyed(this.destroyRef))
        .subscribe({
          next: (response) => {
            this.schemaState.set(response);
            this.schemaForm.patchValue({
              schemaType: response.present ? response.type : 'JSON',
              compatibility: response.present ? response.compatibility : 'FULL',
              definition: response.definition || this.schemaForm.controls.definition.value,
              reason: ''
            });
            this.schemaDeleteForm.patchValue({ reason: '' });
            this.schemaLoading.set(false);
          },
          error: (error: { error?: { message?: string } }) => {
            this.schemaError.set(error.error?.message ?? 'Unable to load schema details.');
            this.schemaLoading.set(false);
          }
        });
    }

    if (workflow === 'test-messages') {
      const topic = this.details();
      this.publishError.set(null);
      this.publishResult.set(null);
      this.exportError.set(null);
      this.exportResult.set(null);
      this.consumeError.set(null);
      this.consumeResult.set(null);
      this.publishForm.patchValue({ reason: '' });
      if (topic) {
        this.consumeForm.patchValue({
          subscriptionMode: this.consumeForm.controls.subscriptionMode.value || 'AUTO_EPHEMERAL',
          subscriptionName: this.consumeForm.controls.subscriptionName.value || topic.subscriptions[0] || '',
          namedSubscriptionName: this.consumeForm.controls.namedSubscriptionName.value || '',
          reason: ''
        });
      }
      this.syncConsumeSubscriptionValidators();
    }

    if (workflow === 'replay') {
      const topic = this.details();
      this.replayFilterFile.set(null);
      this.replayError.set(null);
      this.replayResult.set(null);
      this.replayExportError.set(null);
      this.replayExportResult.set(null);
      this.replayEvents.set([]);
      this.replayEventsError.set(null);
      this.replayDestinationLoadError.set(null);
      this.stopReplayPolling();

      if (topic && topic.subscriptions.length > 0) {
        const currentSubscription = this.replayForm.controls.subscriptionName.value;
        this.replayForm.patchValue({
          subscriptionName: currentSubscription || topic.subscriptions[0] || '',
          operationMode: this.replayForm.controls.operationMode.value || 'ACK_AND_MOVE',
          destinationTopicName: this.replayForm.controls.destinationTopicName.value || this.defaultDestinationTopic(topic.fullName),
          messageLimit: this.replayForm.controls.messageLimit.value || 100,
          matchField: this.replayForm.controls.matchField.value || 'feedId',
          autoReplicateSchema: this.replayForm.controls.autoReplicateSchema.value ?? true,
          messageKey: this.replayForm.controls.messageKey.value,
          propertyFilters: this.replayForm.controls.propertyFilters.value,
          filterText: this.replayForm.controls.filterText.value,
          messagesPerSecond: this.replayForm.controls.messagesPerSecond.value || 50,
          reason: this.replayForm.controls.reason.value
        });
      }
      this.syncReplayDestinationValidators();
    }

    if (workflow === 'dlq') {
      const topic = this.details();
      this.replayFilterFile.set(null);
      this.replayError.set(null);
      this.replayResult.set(null);
      this.replayExportError.set(null);
      this.replayExportResult.set(null);
      this.replayEvents.set([]);
      this.replayEventsError.set(null);
      this.replayDestinationLoadError.set(null);
      this.stopReplayPolling();

      if (topic && topic.subscriptions.length > 0) {
        const currentSubscription = this.replayForm.controls.subscriptionName.value;
        this.replayForm.patchValue({
          subscriptionName: currentSubscription || topic.subscriptions[0] || '',
          operationMode: 'ACK_ONLY',
          destinationTopicName: this.deriveDlqDestination(topic.fullName),
          messageLimit: 50,
          matchField: 'feedId',
          autoReplicateSchema: true,
          messageKey: '',
          propertyFilters: 'source=dlq',
          filterText: 'error',
          messagesPerSecond: 25,
          reason: ''
        });
      }
      this.syncReplayDestinationValidators();
    }

    if (workflow === 'delete-topic') {
      this.deleteTopicError.set(null);
      this.deleteTopicResult.set(null);
      this.deleteTopicForm.patchValue({ reason: '' });
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
    this.actionFeedback.set(null);
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
    this.deleteTopicError.set(null);
  }

  submitSchemaUpdate() {
    const topic = this.details();
    if (!topic) {
      this.schemaError.set('Topic details are still loading.');
      return;
    }
    if (this.schemaForm.invalid) {
      this.schemaForm.markAllAsTouched();
      return;
    }
    const form = this.schemaForm.getRawValue();
    const request: SchemaUpdateRequest = {
      topicName: topic.fullName,
      schemaType: form.schemaType,
      compatibility: form.compatibility.trim() || null,
      definition: form.definition,
      reason: form.reason.trim()
    };
    this.schemaSaving.set(true);
    this.schemaError.set(null);
    this.schemaResult.set(null);
    this.api.upsertSchema(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.schemaSaving.set(false);
          this.schemaResult.set(response);
          this.schemaState.set(response.schema);
          this.applyUpdatedTopic(response.topicDetails);
          this.schemaForm.patchValue({ reason: '' });
        },
        error: (error: { error?: { message?: string } }) => {
          this.schemaSaving.set(false);
          this.schemaError.set(this.decorateSchemaError(error.error?.message ?? 'Unable to save schema details.'));
        }
      });
  }

  submitSchemaDelete() {
    const topic = this.details();
    if (!topic) {
      this.schemaError.set('Topic details are still loading.');
      return;
    }
    if (this.schemaDeleteForm.invalid) {
      this.schemaDeleteForm.markAllAsTouched();
      return;
    }
    const request: SchemaDeleteRequest = {
      topicName: topic.fullName,
      reason: this.schemaDeleteForm.controls.reason.value.trim()
    };
    this.schemaDeleting.set(true);
    this.schemaError.set(null);
    this.schemaResult.set(null);
    this.api.deleteSchema(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.schemaDeleting.set(false);
          this.schemaResult.set(response);
          this.schemaState.set(response.schema);
          this.applyUpdatedTopic(response.topicDetails);
          this.schemaDeleteForm.patchValue({ reason: '' });
        },
        error: (error: { error?: { message?: string } }) => {
          this.schemaDeleting.set(false);
          this.schemaError.set(error.error?.message ?? 'Unable to delete the schema.');
        }
      });
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
    const trimmedReason = formValue.reason.trim();
    if (!trimmedReason) {
      this.replayError.set('A reason is required before starting replay or copy.');
      return;
    }

    const trimmedMatchField = formValue.matchField?.trim() ? formValue.matchField.trim() : null;
    if (this.replayFilterFile() && !trimmedMatchField) {
      this.replayError.set('Match field is required when an IDs CSV file is provided.');
      return;
    }

    if (formValue.operationMode === 'ACK_AND_MOVE') {
      const destinationTopic = formValue.destinationTopicName?.trim();
      if (destinationTopic === topic.fullName) {
        this.replayError.set('Destination topic must be different from the source topic.');
        return;
      }
    }

    const legacyOperation = formValue.operationMode === 'ACK_AND_MOVE' ? 'COPY' : 'REPLAY';

    const request: ReplayCopyJobRequest = {
      topicName: topic.fullName,
      subscriptionName: formValue.subscriptionName,
      operation: legacyOperation,
      operationMode: formValue.operationMode,
      destinationTopicName: formValue.operationMode === 'ACK_AND_MOVE' ? formValue.destinationTopicName : null,
      messageLimit: Number(formValue.messageLimit),
      filterText: formValue.filterText?.trim() ? formValue.filterText.trim() : null,
      matchField: trimmedMatchField,
      autoReplicateSchema: formValue.autoReplicateSchema,
      messagesPerSecond: Number(formValue.messagesPerSecond),
      reason: trimmedReason
    };

    const messageKey = formValue.messageKey?.trim();
    if (messageKey) {
      request.messageKey = messageKey;
    }

    const propertyFilters = this.parseFilterProperties(formValue.propertyFilters);
    if (Object.keys(propertyFilters).length > 0) {
      request.propertyFilters = propertyFilters;
    }

    this.replaySaving.set(true);
    this.replayError.set(null);
    this.replayResult.set(null);
    this.replayExportError.set(null);
    this.replayExportResult.set(null);
    this.replayEventsError.set(null);
    this.replayEvents.set([]);
    this.stopReplayPolling();

    this.api.createReplayCopyJobMultipart(this.environmentId(), request, this.replayFilterFile())
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

  onReplayFilterFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input.files && input.files.length > 0 ? input.files[0] : null;
    this.replayFilterFile.set(file);
  }

  submitDeleteTopic() {
    const topic = this.details();
    if (!topic) {
      this.deleteTopicError.set('Topic details are still loading.');
      return;
    }

    if (this.deleteTopicForm.invalid) {
      this.deleteTopicForm.markAllAsTouched();
      return;
    }

    const request: TopicDeleteRequest = {
      topicName: topic.fullName,
      reason: this.deleteTopicForm.controls.reason.value.trim()
    };

    this.deleteTopicSaving.set(true);
    this.deleteTopicError.set(null);
    this.deleteTopicResult.set(null);

    this.api.deleteTopic(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.deleteTopicSaving.set(false);
          this.deleteTopicResult.set(response);
          void this.router.navigate(['/environments', this.environmentId(), 'topics'], {
            queryParams: this.demoMode.queryParams({
              tenant: response.tenant,
              namespace: response.namespace,
              page: '0'
            })
          });
        },
        error: (error: { error?: { message?: string } }) => {
          this.deleteTopicSaving.set(false);
          this.deleteTopicError.set(error.error?.message ?? 'Unable to delete the topic.');
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
    if ((form.schemaMode || 'RAW').toUpperCase() === 'JSON') {
      try {
        JSON.parse(form.payload);
      } catch {
        this.publishError.set('JSON mode requires a valid JSON payload before the publish request can be sent.');
        return;
      }
    }
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

    const request = this.buildConsumeRequest(topic.fullName, this.consumeForm.controls.reason.value.trim());
    if (!request) {
      this.consumeForm.markAllAsTouched();
      this.consumeError.set('Choose a consume subscription mode and provide the required subscription details.');
      return;
    }

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
          this.applyUpdatedTopic(response.topicDetails);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          this.createSubscriptionForm.patchValue({
            subscriptionName: '',
            initialPosition: 'LATEST',
            reason: ''
          });
          this.closeWorkflow();
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
          this.applyUpdatedTopic(response.topicDetails);
          this.actionFeedback.set({ kind: 'success', message: response.message });
          this.subscriptionPendingDelete.set(null);
          this.deleteSubscriptionForm.patchValue({ reason: '' });
          this.closeWorkflow();
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
    const { subscriptionName, operationMode, destinationTopicName, messageLimit, messageKey, propertyFilters, filterText, messagesPerSecond, matchField } =
      this.replayForm.getRawValue();

    if (!topic || !subscriptionName) {
      return 'Choose a subscription to preview this replay or copy job.';
    }
    if (operationMode === 'ACK_AND_MOVE' && !destinationTopicName) {
      return 'Choose a destination topic for ACK + move mode.';
    }

    const filterBits = [
      messageKey?.trim() ? `key ${messageKey.trim()}` : null,
      propertyFilters?.trim() ? `properties ${propertyFilters.trim()}` : null,
      filterText?.trim() ? `text "${filterText.trim()}"` : null,
      this.replayFilterFile() ? `CSV IDs on field ${matchField || 'feedId'}` : null
    ].filter(Boolean).join(', ');

    if (operationMode === 'ACK_AND_MOVE') {
      return `ACK + move up to ${messageLimit} messages from ${topic.topic} for subscription ${subscriptionName}${filterBits ? ` filtered by ${filterBits}` : ' without additional filtering'} into ${destinationTopicName} at up to ${messagesPerSecond} msg/s. Non-matches are NACKed.`;
    }
    if (operationMode === 'SEARCH_ONLY') {
      return `Search up to ${messageLimit} messages from ${topic.topic} for subscription ${subscriptionName}${filterBits ? ` filtered by ${filterBits}` : ' without additional filtering'} at up to ${messagesPerSecond} msg/s, without ACK/NACK side effects.`;
    }
    return `ACK matched messages for up to ${messageLimit} messages from ${topic.topic} on subscription ${subscriptionName}${filterBits ? ` filtered by ${filterBits}` : ' without additional filtering'}. Non-matches are NACKed for redelivery.`;
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
    if (topic.partitioned) {
      return `Pulsar does not allow terminating partitioned topics such as ${topic.topic}. Use partition-aware workflows instead.`;
    }
    return `This will mark ${topic.topic} append-complete so producers stop writing after the current retained tail. It does not delete the topic or its retained data.`;
  }

  topicPolicyPreview(): string {
    const topic = this.details();
    const form = this.topicPoliciesForm.getRawValue();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return `This will align retention (${form.retentionTimeInMinutes} min / ${form.retentionSizeInMb} MB), TTL (${form.ttlInSeconds}s), compaction (${form.compactionThresholdInBytes} bytes), and producer/consumer/subscription limits for ${topic.topic}.`;
  }

  schemaPreview(): string {
    const topic = this.details();
    const form = this.schemaForm.getRawValue();
    if (!topic) {
      return 'Topic details are still loading.';
    }
    return `This will save ${form.schemaType.toUpperCase()} schema metadata for ${topic.topic} with ${form.compatibility || 'default'} compatibility. Review downstream producers, consumers, and replay targets before applying the change.`;
  }

  schemaWarnings(): string[] {
    const warnings: string[] = [];
    const topic = this.details();
    const current = this.schemaState();
    const form = this.schemaForm.getRawValue();
    if (!topic) {
      return warnings;
    }
    if (current?.present) {
      warnings.push(`Current schema type is ${current.type} with compatibility ${current.compatibility}.`);
    } else {
      warnings.push('This topic does not currently expose a registered schema.');
    }
    if (form.schemaType.trim().toUpperCase() === 'JSON' || form.schemaType.trim().toUpperCase() === 'AVRO') {
      warnings.push('Pulsar expects a record-style schema definition here. Generic JSON Schema documents are rejected by the live admin API.');
    }
    if (form.schemaType.trim().toUpperCase() !== (current?.type || 'NONE').toUpperCase()) {
      warnings.push('Changing schema type can break producers, consumers, and replay/copy workflows if payload encoding no longer matches.');
    }
    if ((form.compatibility || '').trim()) {
      warnings.push(`Requested compatibility will be ${form.compatibility.trim().toUpperCase()}.`);
    }
    return warnings;
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
    if (form.subscriptionMode === 'AUTO_EPHEMERAL') {
      return `This will read up to ${form.maxMessages} messages from ${topic.topic} by auto-creating a temporary test subscription over ${form.waitTimeSeconds} seconds.`;
    }
    if (form.subscriptionMode === 'CREATE_NAMED') {
      return `This will consume up to ${form.maxMessages} messages from ${topic.topic} using named subscription ${form.namedSubscriptionName || 'the provided subscription'} over ${form.waitTimeSeconds} seconds.`;
    }
    return `This will consume up to ${form.maxMessages} messages from ${topic.topic} using existing subscription ${form.subscriptionName || 'the selected subscription'} over ${form.waitTimeSeconds} seconds.`;
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

  canDownloadReplaySearchExport(): boolean {
    const result = this.replayResult();
    return !!result && result.status === 'COMPLETED' && result.searchExportReady;
  }

  downloadReplaySearchExport() {
    const result = this.replayResult();
    if (!result) {
      return;
    }
    this.replayExporting.set(true);
    this.replayExportError.set(null);
    this.replayExportResult.set(null);

    this.api.getReplayCopyJobSearchExport(this.environmentId(), result.jobId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.replayExporting.set(false);
          this.replayExportResult.set(response);
          this.downloadReplaySearchExportFile(response);
        },
        error: (error: { error?: { message?: string } }) => {
          this.replayExporting.set(false);
          this.replayExportError.set(error.error?.message ?? 'Unable to download replay search export.');
        }
      });
  }

  private startReplayPolling(jobId: string) {
    this.stopReplayPolling();

    this.replayJobPolling = timer(900, 1200)
      .pipe(
        switchMap(() => combineLatest([
          this.api.getReplayCopyJob(this.environmentId(), jobId),
          this.api.getReplayCopyJobEvents(this.environmentId(), jobId)
        ])),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ([response, events]) => {
          this.replayResult.set(response);
          this.replayEvents.set(events);
          this.replayEventsError.set(null);
          if (response.status === 'COMPLETED' || response.status === 'FAILED') {
            this.stopReplayPolling();
          }
        },
        error: (error: { error?: { message?: string } }) => {
          this.replayEventsError.set(error.error?.message ?? 'Unable to refresh replay timeline events.');
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

  private deriveDlqDestination(sourceTopicName: string): string {
    if (sourceTopicName.toLowerCase().includes('dlq')) {
      return sourceTopicName.replace(/dlq/ig, 'retry');
    }
    return this.defaultDestinationTopic(sourceTopicName);
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

  publishWarnings(): string[] {
    const topic = this.details();
    const form = this.publishForm.getRawValue();
    if (!topic) {
      return [];
    }

    const warnings: string[] = [];
    const schemaMode = (form.schemaMode || 'RAW').toUpperCase();
    const schemaType = (topic.schema.type || 'NONE').toUpperCase();

    if (schemaMode === 'JSON') {
      try {
        JSON.parse(form.payload);
      } catch {
        warnings.push('The current payload is not valid JSON, so publish will be rejected in JSON mode.');
      }
    }

    if (topic.schema.present) {
      if (schemaType.includes('JSON') && schemaMode !== 'JSON') {
        warnings.push(`This topic advertises a ${topic.schema.type} schema. RAW mode may not match the expected payload encoding.`);
      } else if (!schemaType.includes('JSON') && schemaMode === 'JSON') {
        warnings.push(`This topic advertises schema type ${topic.schema.type}. JSON mode may not match the live schema encoding.`);
      }
      warnings.push(`Schema compatibility is ${topic.schema.compatibility}.`);
    } else if (schemaMode === 'JSON') {
      warnings.push('This topic does not expose schema metadata, so JSON compatibility cannot be verified before publish.');
    }

    return warnings;
  }

  private decorateSchemaError(message: string): string {
    if (message.includes('Invalid schema definition data for JSON schema')) {
      return `${message} Use a Pulsar-compatible record definition such as an Avro-style record instead of generic JSON Schema.`;
    }
    return message;
  }

  replayWarnings(): string[] {
    const topic = this.details();
    const destination = this.replayDestinationTopic();
    const warnings: string[] = [];
    const operationMode = this.replayForm.controls.operationMode.value;

    if (!topic) {
      return warnings;
    }

    if (this.replayFilterFile()) {
      warnings.push(`CSV ID filter file ${this.replayFilterFile()!.name} will be applied using top-level field ${this.replayForm.controls.matchField.value || 'feedId'}.`);
    }

    if (operationMode === 'SEARCH_ONLY') {
      warnings.push('Search mode scans and captures matched messages only. No ACK, NACK, or move side effects are applied.');
      return warnings;
    }

    if (operationMode !== 'ACK_AND_MOVE') {
      warnings.push('Replay mode does not publish to a destination topic. It ACKs matched messages and NACKs non-matches.');
      return warnings;
    }

    if (this.replayDestinationLoadError()) {
      warnings.push(this.replayDestinationLoadError()!);
      return warnings;
    }

    if (!destination) {
      warnings.push('Destination topic details are not loaded yet, so schema compatibility cannot be previewed.');
      return warnings;
    }

    if (!topic.schema.present || !destination.schema.present) {
      warnings.push('One or both topics do not expose schema metadata, so compatibility cannot be fully verified.');
      return warnings;
    }

    if (topic.schema.type !== destination.schema.type) {
      warnings.push(`Source schema type ${topic.schema.type} does not match destination schema type ${destination.schema.type}.`);
    } else {
      warnings.push(`Source and destination both advertise schema type ${topic.schema.type}.`);
    }

    if (topic.schema.compatibility !== destination.schema.compatibility) {
      warnings.push(`Source compatibility ${topic.schema.compatibility} differs from destination compatibility ${destination.schema.compatibility}.`);
    }

    return warnings;
  }

  submitExport(source: 'PEEK' | 'CONSUME') {
    const topic = this.details();
    if (!topic) {
      this.exportError.set('Topic details are still loading.');
      return;
    }

    this.exportSaving.set(true);
    this.exportError.set(null);
    this.exportResult.set(null);

    const request = source === 'PEEK'
      ? {
          topicName: topic.fullName,
          source,
          subscriptionName: null,
          ephemeral: true,
          maxMessages: 5,
          waitTimeSeconds: 5,
          reason: 'Export bounded sampled messages for debugging'
        }
      : (() => {
          const reason = this.consumeForm.controls.reason.value.trim() || 'Export bounded consumed messages for debugging';
          const consumeRequest = this.buildConsumeRequest(topic.fullName, reason);
          if (!consumeRequest) {
            this.consumeForm.markAllAsTouched();
            this.exportError.set('Choose a consume subscription mode and provide the required subscription details before export.');
            return null;
          }
          return {
            topicName: consumeRequest.topicName,
            source,
            subscriptionName: consumeRequest.subscriptionName,
            ephemeral: consumeRequest.ephemeral,
            maxMessages: consumeRequest.maxMessages,
            waitTimeSeconds: consumeRequest.waitTimeSeconds,
            reason: consumeRequest.reason
          };
        })();

    if (!request) {
      this.exportSaving.set(false);
      return;
    }

    this.api.exportMessages(this.environmentId(), request)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.exportSaving.set(false);
          this.exportResult.set(response);
          this.downloadExport(response);
        },
        error: (error: { error?: { message?: string } }) => {
          this.exportSaving.set(false);
          this.exportError.set(error.error?.message ?? 'Unable to export bounded messages.');
        }
      });
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

  private syncConsumeSubscriptionValidators() {
    const mode = this.consumeForm.controls.subscriptionMode.value;
    const subscriptionNameControl = this.consumeForm.controls.subscriptionName;
    const namedSubscriptionNameControl = this.consumeForm.controls.namedSubscriptionName;

    if (mode === 'REUSE_EXISTING') {
      subscriptionNameControl.setValidators([Validators.required]);
    } else {
      subscriptionNameControl.clearValidators();
    }
    subscriptionNameControl.updateValueAndValidity({ emitEvent: false });

    if (mode === 'CREATE_NAMED') {
      namedSubscriptionNameControl.setValidators([Validators.required, Validators.pattern(/[A-Za-z0-9._-]+/)]);
    } else {
      namedSubscriptionNameControl.setValidators([Validators.pattern(/[A-Za-z0-9._-]+/)]);
    }
    namedSubscriptionNameControl.updateValueAndValidity({ emitEvent: false });
  }

  private buildConsumeRequest(topicName: string, reason: string): ConsumeMessagesRequest | null {
    const form = this.consumeForm.getRawValue();
    const trimmedReason = reason.trim();
    const maxMessages = Number(form.maxMessages);
    const waitTimeSeconds = Number(form.waitTimeSeconds);

    if (form.subscriptionMode === 'AUTO_EPHEMERAL') {
      return {
        topicName,
        subscriptionName: null,
        ephemeral: true,
        maxMessages,
        waitTimeSeconds,
        reason: trimmedReason
      };
    }

    if (form.subscriptionMode === 'REUSE_EXISTING') {
      const selectedSubscription = form.subscriptionName.trim();
      if (!selectedSubscription) {
        return null;
      }
      return {
        topicName,
        subscriptionName: selectedSubscription,
        ephemeral: false,
        maxMessages,
        waitTimeSeconds,
        reason: trimmedReason
      };
    }

    const namedSubscription = form.namedSubscriptionName.trim();
    if (!namedSubscription) {
      return null;
    }
    return {
      topicName,
      subscriptionName: namedSubscription,
      ephemeral: false,
      maxMessages,
      waitTimeSeconds,
      reason: trimmedReason
    };
  }

  parseFilterProperties(value: string): Record<string, string> {
    return value.split(',')
      .map((item) => item.trim())
      .filter(Boolean)
      .reduce<Record<string, string>>((accumulator, item) => {
        const [key, ...rest] = item.split('=');
        if (key?.trim() && rest.length > 0) {
          accumulator[key.trim()] = rest.join('=').trim();
        }
        return accumulator;
      }, {});
  }

  private syncReplayDestinationValidators() {
    const operationMode = this.replayForm.controls.operationMode.value;
    const destinationControl = this.replayForm.controls.destinationTopicName;
    if (operationMode === 'ACK_AND_MOVE') {
      destinationControl.setValidators([Validators.required]);
    } else {
      destinationControl.clearValidators();
    }
    destinationControl.updateValueAndValidity({ emitEvent: false });
  }

  private applyUpdatedTopic(topic: TopicDetails) {
    this.details.set(topic);
    const firstSubscription = topic.subscriptions[0] ?? '';
    this.resetForm.patchValue({ subscriptionName: firstSubscription });
    this.skipForm.patchValue({ subscriptionName: firstSubscription });
    this.replayForm.patchValue({ subscriptionName: firstSubscription });
  }

  private loadReplayDestinationPreview(topicName: string | null | undefined) {
    if (this.replayForm.controls.operationMode.value !== 'ACK_AND_MOVE') {
      this.replayDestinationTopic.set(null);
      this.replayDestinationLoadError.set(null);
      return;
    }

    const currentTopic = this.details();
    const trimmed = topicName?.trim();

    if (!trimmed || !currentTopic || trimmed === currentTopic.fullName) {
      this.replayDestinationTopic.set(null);
      this.replayDestinationLoadError.set(trimmed === currentTopic?.fullName ? 'Destination topic must be different from the source topic.' : null);
      return;
    }

    this.replayDestinationLoadError.set(null);
    this.api.getTopicDetails(this.environmentId(), trimmed)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.replayDestinationTopic.set(response);
          this.replayDestinationLoadError.set(null);
        },
        error: () => {
          this.replayDestinationTopic.set(null);
          this.replayDestinationLoadError.set('Destination topic is not present in the current synced snapshot, so schema compatibility cannot be previewed.');
        }
      });
  }

  private downloadExport(response: ExportMessagesResponse) {
    const blob = new Blob([response.content], { type: response.contentType });
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = response.fileName;
    anchor.click();
    window.URL.revokeObjectURL(url);
  }

  private downloadReplaySearchExportFile(response: ReplayCopySearchExportResponse) {
    const blob = new Blob([response.content], { type: response.contentType || 'application/json' });
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = response.fileName;
    anchor.click();
    window.URL.revokeObjectURL(url);
  }
}
