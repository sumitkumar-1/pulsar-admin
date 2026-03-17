import { AsyncPipe, NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { PulsarApiService } from '../../core/api/pulsar-api.service';

@Component({
  selector: 'app-environment-overview',
  standalone: true,
  imports: [AsyncPipe, NgClass, RouterLink],
  templateUrl: './environment-overview.component.html',
  styleUrl: './environment-overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class EnvironmentOverviewComponent {
  readonly environments$ = inject(PulsarApiService).getEnvironments();

  statusClass(status: string): string {
    return status.toLowerCase();
  }
}
