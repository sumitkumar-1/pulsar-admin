import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicDetailsComponent } from './topic-details.component';

describe('TopicDetailsComponent', () => {
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

  it('submits a replay copy job and shows the queued result', async () => {
    const createReplayCopyJob = jasmine.createSpy('createReplayCopyJob').and.returnValue(of({
      jobId: 'job-1a2b3c4d',
      jobType: 'COPY',
      environmentId: 'prod',
      status: 'QUEUED',
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      messagesPerSecond: 50,
      filterText: 'payment-10412',
      matchedMessages: 0,
      publishedMessages: 0,
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
      filterText: 'payment-10412',
      matchedMessages: 36,
      publishedMessages: 36,
      statusMessage: 'Copy job completed in mock mode. Matching messages were republished to the destination topic.',
      createdAt: '2026-03-17T18:00:00Z',
      updatedAt: '2026-03-17T18:00:01Z'
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
            createReplayCopyJob,
            getReplayCopyJob
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
      operation: 'COPY',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      filterText: 'payment-10412',
      messagesPerSecond: 50,
      reason: 'Copy incident-related messages into replay lab'
    });

    component.submitReplayCopyJob();
    fixture.detectChanges();

    expect(createReplayCopyJob).toHaveBeenCalledWith('prod', {
      topicName: 'persistent://acme/orders/payment-events',
      subscriptionName: 'payment-settlement',
      operation: 'COPY',
      destinationTopicName: 'persistent://acme/dev/replay-lab',
      messageLimit: 120,
      filterText: 'payment-10412',
      messagesPerSecond: 50,
      reason: 'Copy incident-related messages into replay lab'
    });
    expect(fixture.nativeElement.textContent).toContain('Queued for worker pickup.');
  });
});
