import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { EnvironmentOverviewComponent } from './environment-overview.component';

describe('EnvironmentOverviewComponent', () => {
  it('renders environment sync state', async () => {
    await TestBed.configureTestingModule({
      imports: [EnvironmentOverviewComponent],
      providers: [
        provideRouter([]),
        {
          provide: PulsarApiService,
          useValue: {
            getEnvironments: () => of([
              {
                id: 'prod',
                name: 'Production',
                kind: 'prod',
                status: 'HEALTHY',
                region: 'us-east-1',
                clusterLabel: 'cluster-a',
                summary: 'Primary traffic',
                syncStatus: 'SYNCED',
                lastSyncedAt: '2026-03-17T10:00:00Z',
                lastTestStatus: 'SUCCESS'
              }
            ]),
            getEnvironment: () => of(),
            createEnvironment: () => of(),
            updateEnvironment: () => of(),
            testEnvironmentConnection: () => of(),
            syncEnvironment: () => of(),
            deleteEnvironment: () => of()
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(EnvironmentOverviewComponent);
    fixture.detectChanges();

    const compiled = fixture.nativeElement as HTMLElement;
    expect(compiled.textContent).toContain('SYNCED');
    expect(compiled.textContent).toContain('SUCCESS');
  });

  it('requires a valid environment form before save', async () => {
    await TestBed.configureTestingModule({
      imports: [EnvironmentOverviewComponent],
      providers: [
        provideRouter([]),
        {
          provide: PulsarApiService,
          useValue: {
            getEnvironments: () => of([]),
            getEnvironment: () => of(),
            createEnvironment: jasmine.createSpy('createEnvironment'),
            updateEnvironment: jasmine.createSpy('updateEnvironment'),
            testEnvironmentConnection: () => of(),
            syncEnvironment: () => of(),
            deleteEnvironment: () => of()
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(EnvironmentOverviewComponent);
    const component = fixture.componentInstance;

    component.environmentForm.patchValue({
      id: '',
      name: 'Invalid',
      kind: 'dev',
      region: '',
      clusterLabel: '',
      summary: '',
      brokerUrl: '',
      adminUrl: '',
      authMode: 'none',
      credentialReference: '',
      tlsEnabled: false
    });

    component.saveEnvironment();

    expect(component.environmentForm.invalid).toBeTrue();
  });
});
