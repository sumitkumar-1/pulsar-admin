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

  it('opens add environment dialog from the toolbar', async () => {
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
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;

    const button = Array.from(root.querySelectorAll('button'))
      .find((element) => element.textContent?.includes('Add Environment')) as HTMLButtonElement | undefined;

    button?.click();
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Add environment');
    expect(fixture.nativeElement.textContent).toContain('Create environment');
  });

  it('shows only one add environment button when the list is empty', async () => {
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
    fixture.detectChanges();

    const buttons = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('button'))
      .filter((element) => element.textContent?.includes('Add Environment'));

    expect(buttons.length).toBe(1);
  });

  it('loads the edit dialog with environment values', async () => {
    await TestBed.configureTestingModule({
      imports: [EnvironmentOverviewComponent],
      providers: [
        provideRouter([]),
        {
          provide: PulsarApiService,
          useValue: {
            getEnvironments: () => of([
              {
                id: 'dev',
                name: 'Development',
                kind: 'dev',
                status: 'HEALTHY',
                region: 'local',
                clusterLabel: 'cluster-dev',
                summary: 'Daily workflows',
                syncStatus: 'SYNCED',
                lastSyncedAt: '2026-03-17T10:00:00Z',
                lastTestStatus: 'SUCCESS'
              }
            ]),
            getEnvironment: () => of({
              id: 'dev',
              name: 'Development',
              kind: 'dev',
              status: 'HEALTHY',
              region: 'local',
              clusterLabel: 'cluster-dev',
              summary: 'Daily workflows',
              syncStatus: 'SYNCED',
              lastSyncedAt: '2026-03-17T10:00:00Z',
              lastTestStatus: 'SUCCESS',
              brokerUrl: 'pulsar://dev-brokers:6650',
              adminUrl: 'https://dev-admin.internal',
              authMode: 'none',
              credentialReference: '',
              tlsEnabled: false,
              syncMessage: 'Metadata synced successfully.',
              lastTestMessage: 'Connection successful.',
              lastTestedAt: '2026-03-17T10:00:00Z',
              deleted: false
            }),
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
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;

    const editButton = Array.from(root.querySelectorAll('.card-actions button'))
      .find((element) => element.textContent?.includes('Edit')) as HTMLButtonElement | undefined;

    editButton?.click();
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Edit environment');
    const component = fixture.componentInstance;
    expect(component.environmentForm.getRawValue().name).toBe('Development');
  });
});
