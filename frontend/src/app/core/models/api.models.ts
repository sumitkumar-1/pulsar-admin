export type EnvironmentStatus = 'HEALTHY' | 'DEGRADED' | 'OFFLINE';
export type TopicHealth = 'HEALTHY' | 'ATTENTION' | 'CRITICAL' | 'INACTIVE';

export interface EnvironmentSummary {
  id: string;
  name: string;
  kind: string;
  status: EnvironmentStatus;
  region: string;
  clusterLabel: string;
  summary: string;
  syncStatus: string;
  lastSyncedAt: string | null;
  lastTestStatus: string;
}

export interface EnvironmentHealth {
  environmentId: string;
  status: EnvironmentStatus;
  brokerUrl: string;
  adminUrl: string;
  pulsarVersion: string;
  message: string;
}

export interface EnvironmentDetails extends EnvironmentSummary {
  brokerUrl: string;
  adminUrl: string;
  authMode: string;
  credentialReference: string | null;
  tlsEnabled: boolean;
  syncMessage: string | null;
  lastTestMessage: string | null;
  lastTestedAt: string | null;
  deleted: boolean;
}

export interface EnvironmentUpsertRequest {
  id: string;
  name: string;
  kind: string;
  region: string;
  clusterLabel: string;
  summary: string;
  brokerUrl: string;
  adminUrl: string;
  authMode: string;
  credentialReference: string;
  tlsEnabled: boolean;
}

export interface EnvironmentConnectionTestResult {
  environmentId: string;
  successful: boolean;
  status: string;
  message: string;
  testedAt: string;
  syncTriggered: boolean;
}

export interface EnvironmentSyncStatus {
  environmentId: string;
  syncStatus: string;
  syncMessage: string;
  lastSyncedAt: string | null;
  tenantCount: number;
  namespaceCount: number;
  topicCount: number;
}

export interface TenantSummary {
  name: string;
  namespaceCount: number;
  topicCount: number;
}

export interface NamespaceSummary {
  tenant: string;
  namespace: string;
  topicCount: number;
}

export interface CatalogSummary {
  environmentId: string;
  tenants: TenantSummary[];
  namespaces: NamespaceSummary[];
}

export interface CatalogMutationResponse {
  environmentId: string;
  resourceType: 'TENANT' | 'NAMESPACE';
  resourceName: string;
  message: string;
  catalogSummary: CatalogSummary;
}

export interface TenantDetails {
  environmentId: string;
  tenant: string;
  adminRoles: string[];
  allowedClusters: string[];
  namespaceCount: number;
  topicCount: number;
  lastSyncedAt: string | null;
}

export interface TenantUpdateRequest {
  tenant: string;
  adminRoles: string[];
  allowedClusters: string[];
  reason: string;
}

export interface TenantDeleteRequest {
  tenant: string;
  reason: string;
}

export interface TenantMutationResponse {
  environmentId: string;
  tenant: string;
  action: 'UPDATE' | 'DELETE';
  message: string;
  tenantDetails: TenantDetails;
  catalogSummary: CatalogSummary;
}

export interface TopicPolicies {
  retentionTimeInMinutes: number | null;
  retentionSizeInMb: number | null;
  ttlInSeconds: number | null;
  compactionThresholdInBytes: number | null;
  maxProducers: number | null;
  maxConsumers: number | null;
  maxSubscriptions: number | null;
}

export interface NamespacePolicies {
  retentionTimeInMinutes: number | null;
  retentionSizeInMb: number | null;
  messageTtlInSeconds: number | null;
  deduplicationEnabled: boolean | null;
  backlogQuotaLimitInBytes: number | null;
  backlogQuotaLimitTimeInSeconds: number | null;
  dispatchRatePerTopicInMsg: number | null;
  dispatchRatePerTopicInByte: number | null;
  publishRateInMsg: number | null;
  publishRateInByte: number | null;
}

export interface TopicStatsSummary {
  backlog: number;
  producers: number;
  subscriptions: number;
  consumers: number;
  publishRateIn: number;
  dispatchRateOut: number;
  throughputIn: number;
  throughputOut: number;
  storageSize: number;
}

export interface TopicListItem {
  fullName: string;
  tenant: string;
  namespace: string;
  topic: string;
  partitioned: boolean;
  partitions: number;
  schemaPresent: boolean;
  health: TopicHealth;
  stats: TopicStatsSummary;
  summary: string;
}

export interface CreateTopicRequest {
  domain: 'persistent' | 'non-persistent';
  tenant: string;
  namespace: string;
  topic: string;
  partitions: number;
  notes: string | null;
}

export interface CreateTenantRequest {
  tenant: string;
  adminRoles: string[];
  allowedClusters: string[];
}

export interface CreateNamespaceRequest {
  tenant: string;
  namespace: string;
}

export interface CreateSubscriptionRequest {
  topicName: string;
  subscriptionName: string;
  initialPosition: 'EARLIEST' | 'LATEST';
  reason: string | null;
}

export interface SubscriptionMutationResponse {
  environmentId: string;
  topicName: string;
  subscriptionName: string;
  action: 'CREATE' | 'DELETE';
  initialPosition: 'EARLIEST' | 'LATEST' | null;
  message: string;
  topicDetails: TopicDetails;
}

export interface TopicPartitionSummary {
  partitionName: string;
  backlog: number;
  consumers: number;
  publishRateIn: number;
  dispatchRateOut: number;
  health: TopicHealth;
}

export interface SchemaSummary {
  type: string;
  version: string;
  compatibility: string;
  present: boolean;
}

export interface SchemaDetails {
  environmentId: string;
  topicName: string;
  present: boolean;
  type: string;
  version: string;
  compatibility: string;
  definition: string;
  editable: boolean;
  message: string;
}

export interface SchemaUpdateRequest {
  topicName: string;
  schemaType: string;
  compatibility: string | null;
  definition: string;
  reason: string;
}

export interface SchemaDeleteRequest {
  topicName: string;
  reason: string;
}

export interface SchemaMutationResponse {
  environmentId: string;
  topicName: string;
  action: 'UPSERT' | 'DELETE';
  message: string;
  schema: SchemaDetails;
  topicDetails: TopicDetails;
}

export interface TopicDetails {
  fullName: string;
  tenant: string;
  namespace: string;
  topic: string;
  partitioned: boolean;
  partitions: number;
  health: TopicHealth;
  stats: TopicStatsSummary;
  schema: SchemaSummary;
  ownerTeam: string;
  notes: string;
  partitionSummaries: TopicPartitionSummary[];
  subscriptions: string[];
}

export interface TopicPoliciesResponse {
  environmentId: string;
  topicName: string;
  policies: TopicPolicies;
  editable: boolean;
  message: string;
}

export interface TopicPoliciesUpdateRequest {
  topicName: string;
  policies: TopicPolicies;
  reason: string;
}

export interface TopicPoliciesUpdateResponse {
  environmentId: string;
  topicName: string;
  policies: TopicPolicies;
  message: string;
  topicDetails: TopicDetails;
}

export interface NamespaceDetails {
  environmentId: string;
  tenant: string;
  namespace: string;
  topicCount: number;
  topics: TopicListItem[];
  policies: NamespacePolicies;
  lastSyncedAt: string | null;
  syncMessage: string | null;
}

export interface NamespacePoliciesUpdateRequest {
  tenant: string;
  namespace: string;
  policies: NamespacePolicies;
  reason: string;
}

export interface NamespacePoliciesResponse {
  environmentId: string;
  tenant: string;
  namespace: string;
  policies: NamespacePolicies;
  message: string;
  namespaceDetails: NamespaceDetails;
}

export interface NamespaceYamlCurrentResponse {
  environmentId: string;
  tenant: string;
  namespace: string;
  yaml: string;
  message: string;
  generatedAt: string;
}

export interface NamespaceDeleteRequest {
  tenant: string;
  namespace: string;
  reason: string;
}

export interface NamespaceMutationResponse {
  environmentId: string;
  tenant: string;
  namespace: string;
  action: 'DELETE';
  message: string;
  catalogSummary: CatalogSummary;
}

export interface PeekMessage {
  messageId: string;
  key: string;
  publishTime: string;
  eventTime: string;
  producerName: string;
  summary: string;
  payload: string;
  schemaVersion: string;
}

export interface PeekMessagesResponse {
  environmentId: string;
  topicName: string;
  requestedCount: number;
  returnedCount: number;
  truncated: boolean;
  messages: PeekMessage[];
}

export interface TerminateTopicRequest {
  topicName: string;
  reason: string;
}

export interface TerminateTopicResponse {
  environmentId: string;
  topicName: string;
  lastMessageId: string | null;
  message: string;
  topicDetails: TopicDetails;
}

export interface ResetCursorRequest {
  topicName: string;
  subscriptionName: string;
  target: string;
  timestamp: string | null;
  reason: string;
}

export interface ResetCursorResponse {
  environmentId: string;
  topicName: string;
  subscriptionName: string;
  target: string;
  effectiveTimestamp: string | null;
  message: string;
}

export interface SkipMessagesRequest {
  topicName: string;
  subscriptionName: string;
  messageCount: number;
  reason: string;
}

export interface SkipMessagesResponse {
  environmentId: string;
  topicName: string;
  subscriptionName: string;
  skippedMessages: number;
  message: string;
}

export interface ClearBacklogRequest {
  topicName: string;
  subscriptionName: string;
  reason: string;
}

export interface ClearBacklogResponse {
  environmentId: string;
  topicName: string;
  subscriptionName: string;
  cleared: boolean;
  message: string;
  clearedAt: string;
}

export interface UnloadTopicRequest {
  topicName: string;
  reason: string;
}

export interface UnloadTopicResponse {
  environmentId: string;
  topicName: string;
  message: string;
  topicDetails: TopicDetails;
}

export interface TopicDeleteRequest {
  topicName: string;
  reason: string;
}

export interface TopicDeleteResponse {
  environmentId: string;
  topicName: string;
  tenant: string;
  namespace: string;
  message: string;
  catalogSummary: CatalogSummary;
}

export interface PublishMessageRequest {
  topicName: string;
  key: string | null;
  properties: Record<string, string>;
  schemaMode: string | null;
  payload: string;
  reason: string;
}

export interface PublishMessageResponse {
  environmentId: string;
  topicName: string;
  messageId: string;
  key: string | null;
  properties: Record<string, string>;
  schemaMode: string;
  publishedAt: string;
  message: string;
  warnings?: string[];
}

export interface ConsumedMessage {
  messageId: string;
  key: string | null;
  publishTime: string | null;
  eventTime: string | null;
  properties: Record<string, string>;
  producerName: string;
  payload: string;
}

export interface ConsumeMessagesRequest {
  topicName: string;
  subscriptionName: string | null;
  ephemeral: boolean;
  maxMessages: number;
  waitTimeSeconds: number;
  reason: string;
}

export interface ConsumeMessagesResponse {
  environmentId: string;
  topicName: string;
  subscriptionName: string;
  ephemeral: boolean;
  requestedCount: number;
  receivedCount: number;
  waitTimeSeconds: number;
  completed: boolean;
  completedAt: string;
  message: string;
  messages: ConsumedMessage[];
  warnings?: string[];
}

export interface ReplayCopyJobRequest {
  topicName: string;
  subscriptionName: string;
  operation?: 'REPLAY' | 'COPY';
  operationMode?: 'ACK_ONLY' | 'ACK_AND_MOVE' | 'SEARCH_ONLY';
  destinationTopicName: string | null;
  messageLimit: number;
  autoReplicateSchema?: boolean;
  messagesPerSecond: number;
  reason: string;
}

export interface ReplayCopyJobStatusResponse {
  jobId: string;
  jobType: 'REPLAY' | 'COPY' | 'SEARCH';
  environmentId: string;
  status: 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED';
  topicName: string;
  subscriptionName: string;
  destinationTopicName: string;
  messageLimit: number;
  messagesPerSecond: number;
  autoReplicateSchema: boolean;
  scannedMessages: number;
  matchedMessages: number;
  nonMatchedMessages: number;
  ackedMessages: number;
  nackedMessages: number;
  movedMessages: number;
  failedMessages: number;
  publishedMessages: number;
  searchMatchedMessages: number;
  searchExportId: string | null;
  searchExportReady: boolean;
  searchExportFileName: string | null;
  progressPercent: number;
  messagesPerSecondActual: number;
  estimatedRemainingSeconds: number;
  startedAt: string | null;
  completedAt: string | null;
  lastMessageId: string | null;
  lastError: string | null;
  statusMessage: string;
  warnings?: string[];
  createdAt: string;
  updatedAt: string;
}

export interface ReplayCopySearchExportResponse {
  environmentId: string;
  jobId: string;
  topicName: string;
  exportedCount: number;
  fileName: string;
  contentType: string;
  content: string;
  exportedAt: string;
  message: string;
}

export interface ReplayCopyJobEventResponse {
  id: number;
  jobId: string;
  eventType: string;
  details: Record<string, unknown>;
  createdAt: string;
}

export interface ExportMessagesRequest {
  topicName: string;
  source: 'PEEK' | 'CONSUME';
  subscriptionName: string | null;
  ephemeral: boolean;
  maxMessages: number;
  waitTimeSeconds: number;
  reason: string;
}

export interface ExportMessagesResponse {
  environmentId: string;
  topicName: string;
  source: 'PEEK' | 'CONSUME';
  exportedCount: number;
  fileName: string;
  contentType: string;
  content: string;
  exportedAt: string;
  message: string;
  warnings?: string[];
}

export interface TenantYamlPreviewRequest {
  tenant: string;
  namespace: string;
  yaml: string;
}

export interface TenantYamlApplyRequest {
  previewId: string;
  reason: string;
  confirmedChangeKeys: string[];
}

export interface TenantYamlFieldChange {
  field: string;
  currentValue: string | null;
  desiredValue: string | null;
}

export interface TenantYamlDiffEntry {
  action: string;
  resourceType: string;
  resourceName: string;
  displayName: string;
  summary: string;
  severity: string;
  iconKey: string;
  riskFlags: string[];
  requiresConfirmation: boolean;
  confirmationKey: string | null;
  fieldChanges: TenantYamlFieldChange[];
}

export interface TenantYamlPreviewResponse {
  previewId: string | null;
  environmentId: string;
  tenant: string;
  namespace: string;
  valid: boolean;
  message: string;
  errors: string[];
  totalCreates: number;
  totalUpdates: number;
  totalRemovals: number;
  dangerousRemovals: number;
  blockedChanges: number;
  requiredConfirmations: string[];
  changes: TenantYamlDiffEntry[];
}

export interface TenantYamlApplyResultEntry {
  action: string;
  resourceType: string;
  resourceName: string;
  status: string;
  message: string;
}

export interface TenantYamlApplyResponse {
  previewId: string;
  environmentId: string;
  tenant: string;
  namespace: string;
  message: string;
  appliedChanges: TenantYamlDiffEntry[];
  applyResults: TenantYamlApplyResultEntry[];
  appliedCount: number;
  skippedCount: number;
  failedCount: number;
  catalogSummary: CatalogSummary;
}

export interface PlatformArtifactSummary {
  name: string;
  tenant: string | null;
  namespace: string | null;
  status: string;
  details: string;
}

export interface PlatformArtifactDetails {
  environmentId: string;
  artifactType: string;
  name: string;
  tenant: string | null;
  namespace: string | null;
  status: string;
  details: string;
  archive: string | null;
  className: string | null;
  inputTopic: string | null;
  outputTopic: string | null;
  parallelism: number | null;
  configs: string;
  editable: boolean;
}

export interface PlatformArtifactMutationRequest {
  artifactType: string;
  name: string;
  tenant: string | null;
  namespace: string | null;
  archive: string | null;
  className: string | null;
  inputTopic: string | null;
  outputTopic: string | null;
  parallelism: number | null;
  configs: string | null;
  reason: string;
}

export interface PlatformArtifactDeleteRequest {
  artifactType: string;
  name: string;
  tenant: string | null;
  namespace: string | null;
  reason: string;
}

export interface PlatformArtifactMutationResponse {
  environmentId: string;
  artifactType: string;
  action: 'UPSERT' | 'DELETE';
  name: string;
  message: string;
  artifact: PlatformArtifactDetails | null;
  platformSummary: PlatformSummary;
}

export interface PlatformSummary {
  environmentId: string;
  functions: PlatformArtifactSummary[];
  sources: PlatformArtifactSummary[];
  sinks: PlatformArtifactSummary[];
  connectors: PlatformArtifactSummary[];
}

export interface TopicPage {
  items: TopicListItem[];
  page: number;
  pageSize: number;
  total: number;
}
