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

export interface TopicPage {
  items: TopicListItem[];
  page: number;
  pageSize: number;
  total: number;
}
