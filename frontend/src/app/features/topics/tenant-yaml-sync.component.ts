import { DatePipe, NgClass } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  ViewChild,
  computed,
  inject,
  signal
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { EditorSelection, EditorState } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { basicSetup } from 'codemirror';
import { yaml as yamlLanguage } from '@codemirror/lang-yaml';
import { combineLatest } from 'rxjs';
import { PulsarApiService } from '../../core/api/pulsar-api.service';
import { DemoModeService } from '../../core/demo-mode.service';
import {
  NamespaceYamlCurrentResponse,
  TenantYamlApplyResponse,
  TenantYamlDiffEntry,
  TenantYamlPreviewResponse
} from '../../core/models/api.models';

type YamlStage = 'edit' | 'validate' | 'review' | 'confirm';

@Component({
  selector: 'app-tenant-yaml-sync',
  standalone: true,
  imports: [DatePipe, NgClass, ReactiveFormsModule, RouterLink],
  templateUrl: './tenant-yaml-sync.component.html',
  styleUrl: './tenant-yaml-sync.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TenantYamlSyncComponent {
  private readonly api = inject(PulsarApiService);
  private readonly demoMode = inject(DemoModeService);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);
  private readonly formBuilder = inject(FormBuilder);

  @ViewChild('editorHost')
  set editorHost(value: ElementRef<HTMLDivElement> | undefined) {
    this._editorHost = value;
    if (value) {
      queueMicrotask(() => this.initializeEditor());
    }
  }

  private _editorHost?: ElementRef<HTMLDivElement>;

  private editorView: EditorView | null = null;
  private syncingEditor = false;

  readonly environmentId = signal('');
  readonly tenant = signal('');
  readonly namespace = signal('');
  readonly loading = signal(true);
  readonly loadingCurrentYaml = signal(false);
  readonly validationState = signal<TenantYamlPreviewResponse | null>(null);
  readonly previewState = signal<TenantYamlPreviewResponse | null>(null);
  readonly applyState = signal<TenantYamlApplyResponse | null>(null);
  readonly currentYamlState = signal<NamespaceYamlCurrentResponse | null>(null);
  readonly error = signal<string | null>(null);
  readonly validating = signal(false);
  readonly previewing = signal(false);
  readonly applying = signal(false);
  readonly stage = signal<YamlStage>('edit');
  readonly confirmedDangerousKeys = signal<string[]>([]);

  readonly yamlForm = this.formBuilder.nonNullable.group({
    tenant: ['', [Validators.required]],
    namespace: ['', [Validators.required]],
    yaml: ['', [Validators.required, Validators.minLength(20)]],
    reason: ['', [Validators.maxLength(240)]]
  });

  readonly stageCards = [
    { key: 'edit', title: 'Edit', copy: 'Work from live namespace YAML or upload a file.' },
    { key: 'validate', title: 'Validate', copy: 'Check syntax, supported fields, and scope.' },
    { key: 'review', title: 'Review Changes', copy: 'Inspect creates, updates, removals, and impact analysis.' },
    { key: 'confirm', title: 'Confirm & Apply', copy: 'Acknowledge dangerous removals, add a reason, then apply.' }
  ] as const;

  readonly groupedChanges = computed(() => {
    const preview = this.previewState();
    if (!preview) {
      return [];
    }

    const groups = new Map<string, { key: string; title: string; changes: TenantYamlDiffEntry[] }>();
    for (const change of preview.changes) {
      const key = this.groupKeyFor(change);
      if (!groups.has(key)) {
        groups.set(key, {
          key,
          title: this.groupTitleFor(key),
          changes: []
        });
      }
      groups.get(key)!.changes.push(change);
    }

    return Array.from(groups.values());
  });

  readonly dangerousChanges = computed(() =>
    (this.previewState()?.changes ?? []).filter((change) => change.requiresConfirmation)
  );

  readonly previewIssues = computed(() => this.previewState()?.errors ?? []);

  readonly missingConfirmations = computed(() => {
    const confirmed = new Set(this.confirmedDangerousKeys());
    return (this.previewState()?.requiredConfirmations ?? []).filter((key) => !confirmed.has(key));
  });

  readonly canApply = computed(() => {
    const preview = this.previewState();
    return !!preview?.previewId
      && preview.valid
      && this.missingConfirmations().length === 0
      && this.yamlForm.controls.reason.valid
      && !this.applying();
  });

  constructor() {
    combineLatest([this.route.paramMap, this.route.queryParamMap])
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(([params, queryParams]) => {
        const envId = params.get('envId') ?? '';
        const tenant = queryParams.get('tenant') ?? '';
        const namespace = queryParams.get('namespace') ?? '';
        this.environmentId.set(envId);
        this.tenant.set(tenant);
        this.namespace.set(namespace);
        this.yamlForm.patchValue({
          tenant,
          namespace,
          yaml: '',
          reason: ''
        });
        this.clearDerivedState();
        this.stage.set('edit');
        this.loadCurrentYaml(true);
      });

    this.yamlForm.controls.yaml.valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((value) => {
        if (this.syncingEditor) {
          return;
        }
        this.syncEditorDocument(value);
      });
  }

  loadCurrentYaml(forceEditorReset = false) {
    const environmentId = this.environmentId();
    const tenant = this.tenant();
    const namespace = this.namespace();

    if (!environmentId || !tenant || !namespace) {
      this.currentYamlState.set(null);
      this.loading.set(false);
      return;
    }

    this.loadingCurrentYaml.set(true);
    this.error.set(null);

    this.api.getCurrentNamespaceYaml(environmentId, tenant, namespace)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.currentYamlState.set(response);
          if (forceEditorReset || !this.yamlForm.controls.yaml.value.trim()) {
            this.patchYamlText(response.yaml);
            this.yamlForm.patchValue({
              tenant: response.tenant,
              namespace: response.namespace,
              reason: this.yamlForm.controls.reason.value
            });
          }
          this.loadingCurrentYaml.set(false);
          this.loading.set(false);
        },
        error: (error: { error?: { message?: string } }) => {
          this.error.set(error.error?.message ?? 'Unable to load the current namespace YAML.');
          this.loadingCurrentYaml.set(false);
          this.loading.set(false);
        }
      });
  }

  resetToCurrentYaml() {
    const current = this.currentYamlState();
    if (!current) {
      this.loadCurrentYaml(true);
      return;
    }

    this.patchYamlText(current.yaml);
    this.yamlForm.patchValue({
      tenant: current.tenant,
      namespace: current.namespace
    });
    this.clearDerivedState();
    this.stage.set('edit');
  }

  onYamlFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];

    if (!file) {
      return;
    }

    const fileName = file.name.toLowerCase();
    if (!fileName.endsWith('.yaml') && !fileName.endsWith('.yml')) {
      this.error.set('Upload a .yaml or .yml file.');
      input.value = '';
      return;
    }

    const reader = new FileReader();
    reader.onload = () => {
      this.patchYamlText(String(reader.result ?? ''));
      this.clearDerivedState();
      this.error.set(null);
      this.stage.set('edit');
      input.value = '';
    };
    reader.onerror = () => {
      this.error.set('Unable to read the selected YAML file.');
      input.value = '';
    };
    reader.readAsText(file);
  }

  formatYamlEditor() {
    const raw = this.yamlForm.controls.yaml.value;
    const normalized = raw
      .replace(/\t/g, '  ')
      .split('\n')
      .map((line) => line.replace(/[ \t]+$/g, ''))
      .join('\n')
      .replace(/\n{3,}/g, '\n\n')
      .trimEnd() + '\n';
    this.patchYamlText(normalized);
  }

  validateYaml() {
    if (this.scopeOrYamlInvalid()) {
      this.markScopeAndYamlTouched();
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.stage.set('validate');
    this.validating.set(true);
    this.error.set(null);
    this.validationState.set(null);

    this.api.validateTenantYaml(this.environmentId(), {
      tenant: form.tenant,
      namespace: form.namespace,
      yaml: form.yaml
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.validationState.set(response);
        this.validating.set(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to validate YAML.');
        this.validating.set(false);
      }
    });
  }

  previewYaml() {
    if (this.scopeOrYamlInvalid()) {
      this.markScopeAndYamlTouched();
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.stage.set('review');
    this.previewing.set(true);
    this.error.set(null);
    this.previewState.set(null);
    this.applyState.set(null);
    this.confirmedDangerousKeys.set([]);

    this.api.previewTenantYaml(this.environmentId(), {
      tenant: form.tenant,
      namespace: form.namespace,
      yaml: form.yaml
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.previewState.set(response);
        if (response.valid && response.requiredConfirmations.length > 0) {
          this.stage.set('confirm');
        }
        this.previewing.set(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to preview YAML changes.');
        this.previewing.set(false);
      }
    });
  }

  applyYaml() {
    const preview = this.previewState();
    if (!preview?.previewId) {
      this.error.set('Generate a preview before applying changes.');
      return;
    }
    if (this.missingConfirmations().length > 0) {
      this.error.set('Confirm the dangerous removals before applying this namespace YAML sync.');
      this.stage.set('confirm');
      return;
    }
    if (this.yamlForm.controls.reason.invalid) {
      this.yamlForm.controls.reason.markAsTouched();
      this.stage.set('confirm');
      return;
    }

    const form = this.yamlForm.getRawValue();
    this.applying.set(true);
    this.error.set(null);
    this.applyState.set(null);

    this.api.applyTenantYaml(this.environmentId(), {
      previewId: preview.previewId,
      reason: form.reason,
      confirmedChangeKeys: this.confirmedDangerousKeys()
    }).pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: (response) => {
        this.applyState.set(response);
        this.applying.set(false);
        this.loadCurrentYaml(false);
      },
      error: (error: { error?: { message?: string } }) => {
        this.error.set(error.error?.message ?? 'Unable to apply YAML changes.');
        this.applying.set(false);
      }
    });
  }

  toggleDangerousChange(confirmationKey: string | null, checked: boolean) {
    if (!confirmationKey) {
      return;
    }

    const next = new Set(this.confirmedDangerousKeys());
    if (checked) {
      next.add(confirmationKey);
    } else {
      next.delete(confirmationKey);
    }
    this.confirmedDangerousKeys.set(Array.from(next));
  }

  isDangerousChangeConfirmed(confirmationKey: string | null) {
    return !!confirmationKey && this.confirmedDangerousKeys().includes(confirmationKey);
  }

  setStage(stage: YamlStage) {
    this.stage.set(stage);
  }

  actionIcon(change: TenantYamlDiffEntry) {
    switch (change.iconKey) {
      case 'add':
        return '+';
      case 'edit':
        return '~';
      case 'delete':
        return '-';
      default:
        return '•';
    }
  }

  severityLabel(change: TenantYamlDiffEntry) {
    switch (change.severity) {
      case 'danger':
        return 'High Risk';
      case 'caution':
        return 'Needs Review';
      default:
        return 'Ready';
    }
  }

  modeQueryParams() {
    return this.demoMode.queryParams({});
  }

  backQueryParams() {
    return this.demoMode.queryParams({
      tenant: this.tenant(),
      namespace: this.namespace()
    });
  }

  private groupKeyFor(change: TenantYamlDiffEntry) {
    if (change.resourceType.startsWith('NAMESPACE')) {
      return 'namespace';
    }
    if (change.resourceType.startsWith('TOPIC_POLICY')) {
      return 'topic-policies';
    }
    return 'topics';
  }

  private groupTitleFor(key: string) {
    switch (key) {
      case 'namespace':
        return 'Namespace changes';
      case 'topic-policies':
        return 'Topic policy changes';
      default:
        return 'Topic lifecycle changes';
    }
  }

  private clearDerivedState() {
    this.validationState.set(null);
    this.previewState.set(null);
    this.applyState.set(null);
    this.confirmedDangerousKeys.set([]);
  }

  private scopeOrYamlInvalid() {
    return this.yamlForm.controls.tenant.invalid
      || this.yamlForm.controls.namespace.invalid
      || this.yamlForm.controls.yaml.invalid;
  }

  private markScopeAndYamlTouched() {
    this.yamlForm.controls.tenant.markAsTouched();
    this.yamlForm.controls.namespace.markAsTouched();
    this.yamlForm.controls.yaml.markAsTouched();
  }

  private patchYamlText(value: string) {
    this.syncingEditor = true;
    this.yamlForm.controls.yaml.setValue(value);
    this.syncingEditor = false;
    this.syncEditorDocument(value);
  }

  private initializeEditor() {
    if (!this._editorHost || this.editorView) {
      return;
    }

    this.editorView = new EditorView({
      state: EditorState.create({
        doc: this.yamlForm.controls.yaml.value,
        extensions: [
          basicSetup,
          yamlLanguage(),
          EditorView.lineWrapping,
          EditorView.updateListener.of((update) => {
            if (!update.docChanged) {
              return;
            }
            const nextValue = update.state.doc.toString();
            this.syncingEditor = true;
            this.yamlForm.controls.yaml.setValue(nextValue);
            this.syncingEditor = false;
            this.validationState.set(null);
            this.previewState.set(null);
            this.applyState.set(null);
          }),
          EditorView.theme({
            '&': {
              minHeight: '34rem',
              fontSize: '0.96rem',
              borderRadius: '1rem',
              border: '1px solid rgba(148, 163, 184, 0.24)',
              background: 'rgba(248, 250, 252, 0.98)'
            },
            '.cm-scroller': {
              fontFamily: '"SFMono-Regular", "JetBrains Mono", monospace',
              lineHeight: '1.55'
            },
            '.cm-gutters': {
              background: 'rgba(241, 245, 249, 0.96)',
              borderRight: '1px solid rgba(148, 163, 184, 0.18)',
              color: '#64748b'
            },
            '.cm-activeLineGutter, .cm-activeLine': {
              background: 'rgba(219, 234, 254, 0.4)'
            },
            '.cm-content': {
              padding: '0.9rem 0'
            },
            '&.cm-focused': {
              outline: '2px solid rgba(37, 99, 235, 0.22)',
              outlineOffset: '2px'
            }
          })
        ]
      }),
      parent: this._editorHost.nativeElement
    });
  }

  private syncEditorDocument(value: string) {
    if (!this.editorView) {
      return;
    }
    const current = this.editorView.state.doc.toString();
    if (current === value) {
      return;
    }
    this.editorView.dispatch({
      changes: { from: 0, to: current.length, insert: value },
      selection: EditorSelection.cursor(Math.min(value.length, this.editorView.state.selection.main.head))
    });
  }
}
