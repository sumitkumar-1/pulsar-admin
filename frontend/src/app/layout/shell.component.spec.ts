import { TestBed } from '@angular/core/testing';
import { provideRouter, Router } from '@angular/router';
import { of } from 'rxjs';
import { PulsarApiService } from '../core/api/pulsar-api.service';
import { DemoModeService } from '../core/demo-mode.service';
import { ShellComponent } from './shell.component';

describe('ShellComponent', () => {
  it('navigates when an environment is selected', async () => {
    const navigate = jasmine.createSpy().and.resolveTo(true);
    const refreshEnvironments = jasmine.createSpy();

    await TestBed.configureTestingModule({
      imports: [ShellComponent],
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
                lastSyncedAt: null,
                lastTestStatus: 'SUCCESS'
              }
            ])
          },
          refreshEnvironments
        },
        {
          provide: DemoModeService,
          useValue: {
            isMockMode: () => false,
            queryParams: (params: Record<string, string>) => params,
            setMode: jasmine.createSpy().and.resolveTo(true)
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(ShellComponent);
    const component = fixture.componentInstance;
    TestBed.inject(Router).navigate = navigate;

    component.onEnvironmentSelected('prod');

    expect(navigate).toHaveBeenCalledWith(
      ['/environments', 'prod', 'topics'],
      { queryParams: {} }
    );
  });

  it('switches data mode from the topbar toggle', async () => {
    const setMode = jasmine.createSpy().and.resolveTo(true);
    const refreshEnvironments = jasmine.createSpy();

    await TestBed.configureTestingModule({
      imports: [ShellComponent],
      providers: [
        provideRouter([]),
        {
          provide: PulsarApiService,
          useValue: {
            getEnvironments: () => of([]),
            refreshEnvironments
          }
        },
        {
          provide: DemoModeService,
          useValue: {
            isMockMode: () => false,
            queryParams: (params: Record<string, string>) => params,
            setMode
          }
        }
      ]
    }).compileComponents();

    const fixture = TestBed.createComponent(ShellComponent);
    fixture.detectChanges();

    const buttons = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.mode-toggle-button'));
    const mockButton = buttons.find((button) => button.textContent?.includes('Mock')) as HTMLButtonElement | undefined;

    mockButton?.click();
    await fixture.whenStable();

    expect(setMode).toHaveBeenCalledWith('mock');
    expect(refreshEnvironments).toHaveBeenCalled();
  });
});
