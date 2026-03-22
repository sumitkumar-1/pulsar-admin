import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TenantYamlSyncComponent } from './tenant-yaml-sync.component';

describe('TenantYamlSyncComponent', () => {
  const previewResponse = {
    previewId: 'preview-1',
    environmentId: 'prod',
    tenant: 'acme',
    namespace: 'orders',
    valid: true,
    message: 'Preview generated for namespace acme/orders.',
    errors: [],
    totalCreates: 0,
    totalUpdates: 1,
    totalRemovals: 1,
    dangerousRemovals: 1,
    blockedChanges: 1,
    requiredConfirmations: ['remove-topic:persistent://acme/orders/order-events'],
    changes: [
      {
        action: 'REMOVE',
        resourceType: 'TOPIC',
        resourceName: 'persistent://acme/orders/order-events',
        displayName: 'order-events',
        summary: 'Topic will be removed from the namespace and requires explicit confirmation first.',
        severity: 'danger',
        iconKey: 'delete',
        riskFlags: ['Retained backlog: 12 messages', 'Subscriptions present: orders-sub'],
        requiresConfirmation: true,
        confirmationKey: 'remove-topic:persistent://acme/orders/order-events',
        fieldChanges: []
      }
    ]
  };

  const applyResponse = {
    previewId: 'preview-1',
    environmentId: 'prod',
    tenant: 'acme',
    namespace: 'orders',
    message: 'Applied the previewed YAML changes for namespace acme/orders.',
    appliedChanges: previewResponse.changes,
    applyResults: [
      {
        action: 'REMOVE',
        resourceType: 'TOPIC',
        resourceName: 'persistent://acme/orders/order-events',
        status: 'APPLIED',
        message: 'Topic removed from namespace sync scope.'
      }
    ],
    appliedCount: 1,
    skippedCount: 0,
    failedCount: 0,
    catalogSummary: {
      environmentId: 'prod',
      tenants: [{ name: 'acme', namespaceCount: 1, topicCount: 0 }],
      namespaces: [{ tenant: 'acme', namespace: 'orders', topicCount: 0 }]
    }
  };

  const currentYamlResponse = {
    environmentId: 'prod',
    tenant: 'acme',
    namespace: 'orders',
    yaml: 'tenant: acme\nnamespace: orders\npolicies:\n  retentionTimeInMinutes: 60\ntopics: []\n',
    message: 'Loaded the current namespace state into editable YAML.',
    generatedAt: '2026-03-21T12:00:00Z'
  };

  it('loads the current namespace YAML into the editor on init', async () => {
    await TestBed.configureTestingModule({
      imports: [TenantYamlSyncComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ tenant: 'acme', namespace: 'orders' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getCurrentNamespaceYaml: jasmine.createSpy('getCurrentNamespaceYaml').and.returnValue(of(currentYamlResponse)),
            validateTenantYaml: jasmine.createSpy('validateTenantYaml'),
            previewTenantYaml: jasmine.createSpy('previewTenantYaml'),
            applyTenantYaml: jasmine.createSpy('applyTenantYaml')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TenantYamlSyncComponent);
    fixture.detectChanges();

    expect(fixture.componentInstance.yamlForm.controls.yaml.value).toContain('tenant: acme');
    expect(fixture.nativeElement.textContent).toContain('Namespace YAML Sync');
  });

  it('resets editor content back to the current namespace YAML', async () => {
    await TestBed.configureTestingModule({
      imports: [TenantYamlSyncComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ tenant: 'acme', namespace: 'orders' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: {
            getCurrentNamespaceYaml: jasmine.createSpy('getCurrentNamespaceYaml').and.returnValue(of(currentYamlResponse)),
            validateTenantYaml: jasmine.createSpy('validateTenantYaml'),
            previewTenantYaml: jasmine.createSpy('previewTenantYaml'),
            applyTenantYaml: jasmine.createSpy('applyTenantYaml')
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TenantYamlSyncComponent);
    fixture.detectChanges();

    fixture.componentInstance.yamlForm.patchValue({ yaml: 'tenant: changed\nnamespace: changed\n' });
    fixture.componentInstance.resetToCurrentYaml();

    expect(fixture.componentInstance.yamlForm.controls.yaml.value).toContain('tenant: acme');
    expect(fixture.componentInstance.yamlForm.controls.namespace.value).toBe('orders');
  });

  it('blocks apply until dangerous removals are confirmed', async () => {
    const api = {
      getCurrentNamespaceYaml: jasmine.createSpy('getCurrentNamespaceYaml').and.returnValue(of(currentYamlResponse)),
      validateTenantYaml: jasmine.createSpy('validateTenantYaml'),
      previewTenantYaml: jasmine.createSpy('previewTenantYaml').and.returnValue(of(previewResponse)),
      applyTenantYaml: jasmine.createSpy('applyTenantYaml').and.returnValue(of(applyResponse))
    };

    await TestBed.configureTestingModule({
      imports: [TenantYamlSyncComponent],
      providers: [
        provideRouter([]),
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ envId: 'prod' })),
            queryParamMap: of(convertToParamMap({ tenant: 'acme', namespace: 'orders' }))
          }
        },
        {
          provide: PulsarApiService,
          useValue: api
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(TenantYamlSyncComponent);
    fixture.detectChanges();

    fixture.componentInstance.yamlForm.patchValue({
      reason: 'Remove deprecated topic from namespace sync'
    });
    fixture.componentInstance.previewYaml();

    expect(fixture.componentInstance.stage()).toBe('confirm');
    expect(fixture.componentInstance.canApply()).toBeFalse();

    fixture.componentInstance.applyYaml();
    expect(api.applyTenantYaml).not.toHaveBeenCalled();

    fixture.componentInstance.toggleDangerousChange(
      'remove-topic:persistent://acme/orders/order-events',
      true
    );
    expect(fixture.componentInstance.canApply()).toBeTrue();

    fixture.componentInstance.applyYaml();
    expect(api.applyTenantYaml).toHaveBeenCalledWith('prod', {
      previewId: 'preview-1',
      reason: 'Remove deprecated topic from namespace sync',
      confirmedChangeKeys: ['remove-topic:persistent://acme/orders/order-events']
    });
  });
});
