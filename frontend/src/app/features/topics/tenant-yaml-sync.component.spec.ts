import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TenantYamlSyncComponent } from './tenant-yaml-sync.component';

describe('TenantYamlSyncComponent', () => {
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
    expect(fixture.nativeElement.textContent).toContain('Namespace YAML Authoring');
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
});
