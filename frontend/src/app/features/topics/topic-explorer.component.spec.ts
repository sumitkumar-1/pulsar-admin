import { TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { convertToParamMap, provideRouter } from '@angular/router';
import { of, throwError } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicExplorerComponent } from './topic-explorer.component';

describe('TopicExplorerComponent', () => {
  it('shows a sync-in-progress state when the first snapshot is still building', async () => {
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
            getEnvironmentHealth: () => throwError(() => ({ error: { message: 'No synced metadata found for environment: prod' } })),
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [],
              namespaces: []
            }),
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 }),
            getEnvironmentSyncStatus: () => of({
              environmentId: 'prod',
              syncStatus: 'SYNCING',
              syncMessage: 'Metadata sync is running in the background.',
              lastSyncedAt: null,
              tenantCount: 0,
              namespaceCount: 0,
              topicCount: 0
            })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('Building the first environment snapshot');
    expect(compiled.textContent).toContain('Metadata sync is running in the background.');
  });

  it('shows the namespace list first when no namespace is selected', async () => {
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 1 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 1 }]
            }),
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('Pick a tenant and namespace to begin.');
    expect(compiled.textContent).not.toContain('Selected Namespace');
  });

  it('renders a namespace-first workspace and selected topic results', async () => {
    const api = {
      getEnvironmentHealth: () => of({
        environmentId: 'prod',
        status: 'HEALTHY',
        brokerUrl: 'broker',
        adminUrl: 'admin',
        pulsarVersion: '4.0.2',
        message: 'healthy'
      }),
      getCatalogSummary: () => of({
        environmentId: 'prod',
        tenants: [
          { name: 'acme', namespaceCount: 2, topicCount: 1 }
        ],
        namespaces: [
          { tenant: 'acme', namespace: 'orders', topicCount: 1 },
          { tenant: 'acme', namespace: 'replay', topicCount: 0 }
        ]
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
      createTopic: jasmine.createSpy('createTopic'),
      createTenant: jasmine.createSpy('createTenant'),
      createNamespace: jasmine.createSpy('createNamespace')
    };

    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({
              tenant: 'acme',
              namespace: 'orders',
              search: 'payment',
              page: '0',
              pageSize: '25'
            }))
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
    expect(compiled.textContent).toContain('2 active workspaces');
    expect(compiled.textContent).toContain('Selected Namespace');
    expect(compiled.textContent).toContain('acme/orders');
    expect(compiled.textContent).toContain('Topics');
    expect(compiled.textContent).toContain('Namespace');
    expect(compiled.textContent).toContain('Platform');
    expect(compiled.textContent).toContain('payment-events');
    expect(compiled.textContent).toContain('Backlog-heavy topic');
  });

  it('filters the visible topic cards using the active search term', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({
              tenant: 'acme',
              namespace: 'orders',
              search: 'invoice',
              page: '0',
              pageSize: '25'
            }))
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 2 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 2 }]
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
                  health: 'HEALTHY',
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
                  summary: 'Payment event topic'
                },
                {
                  fullName: 'persistent://acme/orders/invoice-events',
                  tenant: 'acme',
                  namespace: 'orders',
                  topic: 'invoice-events',
                  partitioned: false,
                  partitions: 0,
                  schemaPresent: true,
                  health: 'HEALTHY',
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
                  summary: 'Invoice event topic'
                }
              ],
              page: 0,
              pageSize: 25,
              total: 2
            })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('invoice-events');
    expect(compiled.textContent).not.toContain('payment-events');
    expect(compiled.textContent).toContain('1 visible on this page for "invoice"');
  });

  it('shows topic paging controls when the selected namespace has more topics than the current page', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({
              tenant: 'acme',
              namespace: 'orders',
              page: '0',
              pageSize: '25'
            }))
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 1585 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 1585 }]
            }),
            getTopics: () => of({
              items: Array.from({ length: 25 }, (_, index) => ({
                fullName: `persistent://acme/orders/topic-${index}`,
                tenant: 'acme',
                namespace: 'orders',
                topic: `topic-${index}`,
                partitioned: false,
                partitions: 0,
                schemaPresent: false,
                health: 'HEALTHY',
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
                summary: 'Topic summary'
              })),
              page: 0,
              pageSize: 25,
              total: 1585
            })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('Showing 1-25 of 1,585 topics');
    expect(compiled.textContent).toContain('Previous');
    expect(compiled.textContent).toContain('Next');
  });

  it('prefills the create topic dialog from the selected namespace', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({
              tenant: 'acme',
              namespace: 'orders',
              page: '0',
              pageSize: '25'
            }))
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 1 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 1 }]
            }),
            getTopics: () => of({
              items: [],
              page: 0,
              pageSize: 25,
              total: 0
            }),
            createTopic: jasmine.createSpy('createTopic'),
            createTenant: jasmine.createSpy('createTenant'),
            createNamespace: jasmine.createSpy('createNamespace')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    const component = fixture.componentInstance;
    fixture.detectChanges();

    component.openCreateTopicDialog();

    expect(component.createTopicForm.controls.tenant.value).toBe('acme');
    expect(component.createTopicForm.controls.namespace.value).toBe('orders');
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 0 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 0 }]
            }),
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 }),
            createTopic,
            createTenant: jasmine.createSpy('createTenant'),
            createNamespace: jasmine.createSpy('createNamespace')
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

  it('creates a namespace from the dialog and refreshes the catalog', async () => {
    const createNamespace = jasmine.createSpy('createNamespace').and.returnValue(of({
      environmentId: 'prod',
      resourceType: 'NAMESPACE',
      resourceName: 'acme/orders',
      message: 'Created namespace acme/orders and refreshed the environment catalog.',
      catalogSummary: {
        environmentId: 'prod',
        tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 0 }],
        namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 0 }]
      }
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 0, topicCount: 0 }],
              namespaces: []
            }),
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 }),
            createTopic: jasmine.createSpy('createTopic'),
            createTenant: jasmine.createSpy('createTenant'),
            createNamespace
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    const component = fixture.componentInstance;
    fixture.detectChanges();

    component.openCreateNamespaceDialog();
    component.createNamespaceForm.patchValue({
      tenant: 'acme',
      namespace: 'orders'
    });
    component.submitCreateNamespace();

    expect(createNamespace).toHaveBeenCalledWith('prod', {
      tenant: 'acme',
      namespace: 'orders'
    });
    expect(component.namespaceDialogOpen()).toBeFalse();
    expect(component.actionFeedback()?.kind).toBe('success');
  });

  it('opens tenant deletion in a modal dialog shell', async () => {
    await TestBed.configureTestingModule({
      imports: [TopicExplorerComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({
              tenant: 'acme',
              namespace: 'orders',
              page: '0',
              pageSize: '25'
            }))
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
            getCatalogSummary: () => of({
              environmentId: 'prod',
              tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 0 }],
              namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 0 }]
            }),
            getTopics: () => of({ items: [], page: 0, pageSize: 25, total: 0 })
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TopicExplorerComponent);
    const component = fixture.componentInstance;
    fixture.detectChanges();

    component.setActiveTab('namespace');
    component.openDeleteTenantDialog();
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.querySelector('.dialog-backdrop')).not.toBeNull();
    expect(compiled.querySelector('.dialog-shell')).not.toBeNull();
    expect(compiled.textContent).toContain('Delete acme');
  });
});
