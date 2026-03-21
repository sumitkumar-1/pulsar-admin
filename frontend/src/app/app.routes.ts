import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    loadComponent: () => import('./layout/shell.component').then((m) => m.ShellComponent),
    children: [
      {
        path: 'environments',
        children: [
          {
            path: '',
            loadComponent: () =>
              import('./features/environments/environment-overview.component').then(
                (m) => m.EnvironmentOverviewComponent
              )
          },
          {
            path: ':envId/topics',
            loadComponent: () =>
              import('./features/topics/topic-explorer.component').then((m) => m.TopicExplorerComponent)
          },
          {
            path: ':envId/topic-details',
            loadComponent: () =>
              import('./features/topics/topic-details.component').then((m) => m.TopicDetailsComponent)
          },
          {
            path: ':envId/namespace-details',
            loadComponent: () =>
              import('./features/topics/namespace-details.component').then((m) => m.NamespaceDetailsComponent)
          },
          {
            path: ':envId/namespace-yaml',
            loadComponent: () =>
              import('./features/topics/tenant-yaml-sync.component').then((m) => m.TenantYamlSyncComponent)
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
