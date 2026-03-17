import { DecimalPipe, NgClass } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { combineLatest, switchMap } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { TopicDetails } from '../../core/models/api.models';

@Component({
  selector: 'app-topic-details',
  standalone: true,
  imports: [DecimalPipe, NgClass, RouterLink],
  templateUrl: './topic-details.component.html',
  styleUrl: './topic-details.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TopicDetailsComponent {
  private readonly api = inject(PulsarApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);

  readonly details = signal<TopicDetails | null>(null);
  readonly environmentId = signal('');
  readonly loading = signal(true);
  readonly loadError = signal<string | null>(null);

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(
        switchMap(([params, queryParams]) => {
        const envId = params.get('envId') ?? '';
        const topic = queryParams.get('topic') ?? '';

        this.environmentId.set(envId);
        this.loading.set(true);
        this.loadError.set(null);

        return this.api.getTopicDetails(envId, topic);
      }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: (details) => {
          this.details.set(details);
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.loadError.set(error.error?.message ?? 'Unable to load topic details.');
          this.loading.set(false);
        }
      });
  }

  healthClass(status: string): string {
    return status.toLowerCase();
  }
}
