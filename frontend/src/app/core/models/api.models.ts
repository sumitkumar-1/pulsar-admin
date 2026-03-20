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
}

export interface ReplayCopyJobRequest {
  topicName: string;
  subscriptionName: string;
  operation: 'REPLAY' | 'COPY';
  destinationTopicName: string;
  messageLimit: number;
  filterText: string | null;
  messagesPerSecond: number;
  reason: string;
}

export interface ReplayCopyJobStatusResponse {
  jobId: string;
  jobType: 'REPLAY' | 'COPY';
  environmentId: string;
  status: 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED';
  topicName: string;
  subscriptionName: string;
  destinationTopicName: string;
  messageLimit: number;
  messagesPerSecond: number;
  filterText: string | null;
  matchedMessages: number;
  publishedMessages: number;
  statusMessage: string;
  createdAt: string;
  updatedAt: string;
}

export interface TenantYamlPreviewRequest {
  tenant: string;
  namespace: string;
  yaml: string;
}

export interface TenantYamlApplyRequest {
  previewId: string;
  reason: string;
}

export interface TenantYamlDiffEntry {
  action: string;
  resourceType: string;
  resourceName: string;
  summary: string;
}

export interface TenantYamlPreviewResponse {
  previewId: string | null;
  environmentId: string;
  tenant: string;
  namespace: string;
  valid: boolean;
  message: string;
  errors: string[];
  changes: TenantYamlDiffEntry[];
}

export interface TenantYamlApplyResponse {
  previewId: string;
  environmentId: string;
  tenant: string;
  namespace: string;
  message: string;
  appliedChanges: TenantYamlDiffEntry[];
  catalogSummary: CatalogSummary;
}

export interface TopicPage {
  items: TopicListItem[];
  page: number;
  pageSize: number;
  total: number;
}
