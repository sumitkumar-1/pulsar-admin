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
            peekMessages
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
});
