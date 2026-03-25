import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicDetailsComponent } from './topic-details.component';

describe('TopicDetailsComponent', () => {
  it('renders the tabbed workspace and defaults to overview', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: true,
              partitions: 12,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Overview');
    expect(fixture.nativeElement.textContent).toContain('Subscriptions');
    expect(fixture.nativeElement.textContent).toContain('Operator Notes');
  });

  it('loads backend peek messages when the peek workflow opens', async () => {
    const peekMessages = jasmine.createSpy('peekMessages').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      requestedCount: 5,
      returnedCount: 1,
      truncated: false,
      messages: [
        {
          messageId: 'ledger:91:2048',
          key: 'payment-10412',
          publishTime: '2026-03-17T17:18:42Z',
          eventTime: '2026-03-17T17:18:41Z',
          producerName: 'payments-producer-2',
          summary: 'Payment authorized but settlement consumer is lagging behind the live stream.',
          payload: '{ "paymentId": "10412" }',
          schemaVersion: '9'
        }
      ]
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages,
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    fixture.componentInstance.openWorkflow('peek');
    fixture.detectChanges();

    expect(peekMessages).toHaveBeenCalledWith('prod', 'persistent://acme/orders/payment-events', 5);
    expect(fixture.nativeElement.textContent).toContain('payment-10412');
  });

  it('submits a reset cursor request and shows the result message', async () => {
    const resetCursor = jasmine.createSpy('resetCursor').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      target: 'LATEST',
      effectiveTimestamp: null,
      message: 'Cursor reset to the latest position for subscription payment-settlement.'
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor,
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openWorkflow('reset');
    component.resetForm.patchValue({
      subscriptionName: 'payment-settlement',
      target: 'LATEST',
      timestamp: '',
      reason: 'Clear backlog after incident validation'
    });

    component.submitResetCursor();
    fixture.detectChanges();

    expect(resetCursor).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      target: 'LATEST',
      timestamp: null,
      reason: 'Clear backlog after incident validation'
    });
    expect(fixture.nativeElement.textContent).toContain('Cursor reset to the latest position');
  });

  it('submits a skip messages request and shows the result message', async () => {
    const skipMessages = jasmine.createSpy('skipMessages').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      skippedMessages: 25,
      message: 'Skipped 25 messages for subscription payment-settlement.'
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages,
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openWorkflow('skip');
    component.skipForm.patchValue({
      subscriptionName: 'payment-settlement',
      messageCount: 25,
      reason: 'Skip poison messages after alert triage'
    });

    component.submitSkipMessages();
    fixture.detectChanges();

    expect(skipMessages).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      messageCount: 25,
      reason: 'Skip poison messages after alert triage'
    });
    expect(fixture.nativeElement.textContent).toContain('Skipped 25 messages');
  });

  it('submits an unload topic request and shows the result message', async () => {
    const unloadTopic = jasmine.createSpy('unloadTopic').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      message: 'Unloaded topic persistent://acme/orders/payment-events. Brokers can now rebalance ownership and refresh the live serving path.',
      topicDetails: {
        fullName: 'persistent://acme/orders/payment-events',
        tenant: 'acme',
        namespace: 'orders',
        topic: 'payment-events',
        partitioned: false,
        partitions: 0,
        health: 'CRITICAL',
        stats: {
          backlog: 18720,
          producers: 5,
          subscriptions: 2,
          consumers: 3,
          publishRateIn: 190.5,
          dispatchRateOut: 48.3,
          throughputIn: 6200,
          throughputOut: 2100,
          storageSize: 5880120
        },
        schema: {
          type: 'JSON',
          version: '9',
          compatibility: 'BACKWARD',
          present: true
        },
        ownerTeam: 'Payments',
        notes: 'Backlog-heavy topic.',
        partitionSummaries: [],
        subscriptions: ['payment-settlement', 'payment-alerts']
      }
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic,
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openWorkflow('unload');
    component.unloadForm.patchValue({
      reason: 'Refresh broker ownership after incident cleanup'
    });

    component.submitUnloadTopic();
    fixture.detectChanges();

    expect(unloadTopic).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      reason: 'Refresh broker ownership after incident cleanup'
    });
    expect(fixture.nativeElement.textContent).toContain('Unloaded topic persistent://acme/orders/payment-events');
  });

  it('creates a subscription and updates the topic details', async () => {
    const createSubscription = jasmine.createSpy('createSubscription').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-review',
      action: 'CREATE',
      initialPosition: 'EARLIEST',
      message: 'Created subscription payment-review at earliest for topic persistent://acme/orders/payment-events.',
      topicDetails: {
        fullName: 'persistent://acme/orders/payment-events',
        tenant: 'acme',
        namespace: 'orders',
        topic: 'payment-events',
        partitioned: false,
        partitions: 0,
        health: 'CRITICAL',
        stats: {
          backlog: 18720,
          producers: 5,
          subscriptions: 3,
          consumers: 3,
          publishRateIn: 190.5,
          dispatchRateOut: 48.3,
          throughputIn: 6200,
          throughputOut: 2100,
          storageSize: 5880120
        },
        schema: {
          type: 'JSON',
          version: '9',
          compatibility: 'BACKWARD',
          present: true
        },
        ownerTeam: 'Payments',
        notes: 'Backlog-heavy topic.',
        partitionSummaries: [],
        subscriptions: ['payment-alerts', 'payment-review', 'payment-settlement']
      }
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createSubscription,
            deleteSubscription: jasmine.createSpy('deleteSubscription'),
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openWorkflow('create-subscription');
    component.createSubscriptionForm.patchValue({
      subscriptionName: 'payment-review',
      initialPosition: 'EARLIEST',
      reason: 'Create review subscription'
    });

    component.submitCreateSubscription();
    fixture.detectChanges();

    expect(createSubscription).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-review',
      initialPosition: 'EARLIEST',
      reason: 'Create review subscription'
    });
    expect(component.activeWorkflow()).toBeNull();
    expect(component.actionFeedback()?.message).toContain('Created subscription payment-review');
    expect(fixture.nativeElement.textContent).toContain('Created subscription payment-review');
  });

  it('deletes a subscription and updates the topic details', async () => {
    const deleteSubscription = jasmine.createSpy('deleteSubscription').and.returnValue(of({
      environmentId: 'prod',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-alerts',
      action: 'DELETE',
      initialPosition: null,
      message: 'Deleted subscription payment-alerts from topic persistent://acme/orders/payment-events.',
      topicDetails: {
        fullName: 'persistent://acme/orders/payment-events',
        tenant: 'acme',
        namespace: 'orders',
        topic: 'payment-events',
        partitioned: false,
        partitions: 0,
        health: 'CRITICAL',
        stats: {
          backlog: 18720,
          producers: 5,
          subscriptions: 1,
          consumers: 3,
          publishRateIn: 190.5,
          dispatchRateOut: 48.3,
          throughputIn: 6200,
          throughputOut: 2100,
          storageSize: 5880120
        },
        schema: {
          type: 'JSON',
          version: '9',
          compatibility: 'BACKWARD',
          present: true
        },
        ownerTeam: 'Payments',
        notes: 'Backlog-heavy topic.',
        partitionSummaries: [],
        subscriptions: ['payment-settlement']
      }
    }));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createSubscription: jasmine.createSpy('createSubscription'),
            deleteSubscription,
            createReplayCopyJob: jasmine.createSpy('createReplayCopyJob'),
            getReplayCopyJob: jasmine.createSpy('getReplayCopyJob')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openDeleteSubscriptionWorkflow('payment-alerts');
    component.deleteSubscriptionForm.patchValue({
      reason: 'Remove stale test subscription'
    });

    component.submitDeleteSubscription();
    fixture.detectChanges();

    expect(deleteSubscription).toHaveBeenCalledWith(
      'prod',
      'persistent://acme/orders/payment-events',
      'payment-alerts'
    );
    expect(component.activeWorkflow()).toBeNull();
    expect(component.actionFeedback()?.message).toContain('Deleted subscription payment-alerts');
    expect(fixture.nativeElement.textContent).toContain('Deleted subscription payment-alerts');
  });

  it('submits a replay copy job and shows the queued result', async () => {
    const createReplayCopyJobMultipart = jasmine.createSpy('createReplayCopyJobMultipart').and.returnValue(of({
      jobId: 'job-1a2b3c4d',
      jobType: 'COPY',
      environmentId: 'prod',
      status: 'QUEUED',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      messagesPerSecond: 50,
      autoReplicateSchema: true,
      scannedMessages: 0,
      matchedMessages: 0,
      nonMatchedMessages: 0,
      ackedMessages: 0,
      nackedMessages: 0,
      movedMessages: 0,
      failedMessages: 0,
      publishedMessages: 0,
      searchMatchedMessages: 0,
      searchExportId: null,
      searchExportReady: false,
      searchExportFileName: null,
      progressPercent: 0,
      messagesPerSecondActual: 0,
      estimatedRemainingSeconds: 0,
      startedAt: '2026-03-17T18:00:00Z',
      completedAt: null,
      lastMessageId: null,
      lastError: null,
      statusMessage: 'Queued for worker pickup.',
      createdAt: '2026-03-17T18:00:00Z',
      updatedAt: '2026-03-17T18:00:00Z'
    }));
    const getReplayCopyJob = jasmine.createSpy('getReplayCopyJob').and.returnValue(of({
      jobId: 'job-1a2b3c4d',
      jobType: 'COPY',
      environmentId: 'prod',
      status: 'COMPLETED',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      messagesPerSecond: 50,
      autoReplicateSchema: true,
      scannedMessages: 36,
      matchedMessages: 36,
      nonMatchedMessages: 0,
      ackedMessages: 36,
      nackedMessages: 0,
      movedMessages: 36,
      failedMessages: 0,
      publishedMessages: 36,
      searchMatchedMessages: 0,
      searchExportId: null,
      searchExportReady: false,
      searchExportFileName: null,
      progressPercent: 100,
      messagesPerSecondActual: 36,
      estimatedRemainingSeconds: 0,
      startedAt: '2026-03-17T18:00:00Z',
      completedAt: '2026-03-17T18:00:01Z',
      lastMessageId: 'ledger:1:9',
      lastError: null,
      statusMessage: 'Copy job completed in mock mode. Matching messages were republished to the destination topic.',
      createdAt: '2026-03-17T18:00:00Z',
      updatedAt: '2026-03-17T18:00:01Z'
    }));
    const getReplayCopyJobEvents = jasmine.createSpy('getReplayCopyJobEvents').and.returnValue(of([]));

    await TestBed.configureTestingModule({
      imports: [TopicDetailsComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ topic: 'persistent://acme/orders/payment-events' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getTopicDetails: () => of({
              fullName: 'persistent://acme/orders/payment-events',
              tenant: 'acme',
              namespace: 'orders',
              topic: 'payment-events',
              partitioned: false,
              partitions: 0,
              health: 'CRITICAL',
              stats: {
                backlog: 18720,
                producers: 5,
                subscriptions: 2,
                consumers: 3,
                publishRateIn: 190.5,
                dispatchRateOut: 48.3,
                throughputIn: 6200,
                throughputOut: 2100,
                storageSize: 5880120
              },
              schema: {
                type: 'JSON',
                version: '9',
                compatibility: 'BACKWARD',
                present: true
              },
              ownerTeam: 'Payments',
              notes: 'Backlog-heavy topic.',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            }),
            peekMessages: jasmine.createSpy('peekMessages'),
            resetCursor: jasmine.createSpy('resetCursor'),
            skipMessages: jasmine.createSpy('skipMessages'),
            unloadTopic: jasmine.createSpy('unloadTopic'),
            createReplayCopyJobMultipart,
            getReplayCopyJob,
            getReplayCopyJobEvents
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const component = fixture.componentInstance;
    component.openWorkflow('replay');
    component.replayForm.patchValue({
      subscriptionName: 'payment-settlement',
      operationMode: 'ACK_AND_MOVE',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      messagesPerSecond: 50,
      reason: 'Copy incident-related messages into replay lab'
    });

    component.submitReplayCopyJob();
    fixture.detectChanges();

    expect(createReplayCopyJobMultipart).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      operation: 'COPY',
      operationMode: 'ACK_AND_MOVE',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      autoReplicateSchema: true,
      messagesPerSecond: 50,
      reason: 'Copy incident-related messages into replay lab'
    }, null);
    expect(fixture.nativeElement.textContent).toContain('Queued for worker pickup.');
  });
});
