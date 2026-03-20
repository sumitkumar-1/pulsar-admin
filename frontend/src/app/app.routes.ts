import { Routes } from '@angular/router';
import { ShellComponent } from './layout/shell.component';
import { EnvironmentOverviewComponent } from './features/environments/environment-overview.component';
import { TopicExplorerComponent } from './features/topics/topic-explorer.component';
import { TopicDetailsComponent } from './features/topics/topic-details.component';
import { NamespaceDetailsComponent } from './features/topics/namespace-details.component';
import { TenantYamlSyncComponent } from './features/topics/tenant-yaml-sync.component';

export const routes: Routes = [
  {
    path: '',
    component: ShellComponent,
    children: [
      {
        path: 'environments',
        children: [
          {
            path: '',
            component: EnvironmentOverviewComponent
          },
          {
            path: ':envId/topics',
            component: TopicExplorerComponent
          },
          {
            path: ':envId/topic-details',
            component: TopicDetailsComponent
          },
          {
            path: ':envId/namespace-details',
            component: NamespaceDetailsComponent
          },
          {
            path: ':envId/namespace-yaml',
            component: TenantYamlSyncComponent
          }
        ]
      },
      {
        path: '',
        pathMatch: 'full',
        redirectTo: 'environments'
      }
    ]
  },
  {
    path: '**',
    redirectTo: 'environments'
  }
];
