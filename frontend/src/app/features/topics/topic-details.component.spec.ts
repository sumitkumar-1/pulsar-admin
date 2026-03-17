import { TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, provideRouter } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicDetailsComponent } from './topic-details.component';

describe('TopicDetailsComponent', () => {
  it('renders topic stats and subscriptions', async () => {
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
              notes: 'Backlog-heavy topic',
              partitionSummaries: [],
              subscriptions: ['payment-settlement', 'payment-alerts']
            })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicDetailsComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('payment-events');
    expect(compiled.textContent).toContain('payment-settlement');
    expect(compiled.textContent).toContain('18,720');
  });
});
