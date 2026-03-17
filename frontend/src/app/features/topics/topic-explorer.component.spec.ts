import { TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, provideRouter } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicExplorerComponent } from './topic-explorer.component';

describe('TopicExplorerComponent', () => {
  it('renders grouped topic results', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ search: 'payment', page: '0', pageSize: '25' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getEnvironmentHealth: () => of({
              environmentId: 'prod',
              status: 'HEALTHY',
              brokerUrl: 'broker',
              adminUrl: 'admin',
              pulsarVersion: '4.0.2',
              message: 'healthy'
            }),
            getTopics: () => of({
              items: [
                {
                  fullName: 'persistent://acme/orders/payment-events',
                  tenant: 'acme',
                  namespace: 'orders',
                  topic: 'payment-events',
                  partitioned: false,
                  partitions: 0,
                  schemaPresent: true,
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
                  summary: 'Backlog-heavy topic'
                }
              ],
              page: 0,
              pageSize: 25,
              total: 1
            })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('payment-events');
    expect(compiled.textContent).toContain('Backlog-heavy topic');
  });
});
