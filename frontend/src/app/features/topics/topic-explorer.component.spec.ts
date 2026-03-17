import { TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { convertToParamMap, provideRouter } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicExplorerComponent } from './topic-explorer.component';

describe('TopicExplorerComponent', () => {
  it('renders grouped topic results', async () => {
    const api = {
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
      }),
      createTopic: jasmine.createSpy('createTopic')
    };

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
          useValue: api
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('payment-events');
    expect(compiled.textContent).toContain('Backlog-heavy topic');
  });

  it('creates a topic from the dialog and refreshes the grid', async () => {
    const createTopic = jasmine.createSpy('createTopic').and.returnValue(of({
      fullName: 'persistent://acme/orders/payment-events-retry',
      tenant: 'acme',
      namespace: 'orders',
      topic: 'payment-events-retry',
      partitioned: false,
      partitions: 0,
      health: 'INACTIVE',
      stats: {
        backlog: 0,
        producers: 0,
        subscriptions: 0,
        consumers: 0,
        publishRateIn: 0,
        dispatchRateOut: 0,
        throughputIn: 0,
        throughputOut: 0,
        storageSize: 0
      },
      schema: {
        type: 'NONE',
        version: '-',
        compatibility: 'N/A',
        present: false
      },
      ownerTeam: 'Unassigned',
      notes: 'Created from dialog',
      partitionSummaries: [],
      subscriptions: []
    }));

    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ page: '0', pageSize: '25' }))
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
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 }),
            createTopic
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    const component = fixture.componentInstance;
    fixture.detectChanges();

    component.openCreateTopicDialog();
    component.createTopicForm.patchValue({
      tenant: 'acme',
      namespace: 'orders',
      topic: 'payment-events-retry',
      partitions: 0,
      notes: 'Created from dialog'
    });
    component.submitCreateTopic();

    expect(createTopic).toHaveBeenCalledWith('prod', jasmine.objectContaining({
      tenant: 'acme',
      namespace: 'orders',
      topic: 'payment-events-retry'
    }));
    expect(component.dialogOpen()).toBeFalse();
    expect(component.actionFeedback()?.kind).toBe('success');
  });
});
