/*
* Module:
* Deploy a CosmosDB instance, SQL API
* 
* Description:
* This module deploys a CosmosDB instance while configuring the SQL API
* 
* More information:
* https://docs.microsoft.com/en-us/azure/templates/microsoft.documentdb/databaseaccounts?tabs=bicep
*/

@description('Cosmos DB account name')
param accountName string

@description('Location for the Cosmos DB account')
param location string = resourceGroup().location

@description('The primary replica region for the Cosmos DB account')
param primaryRegion string

@description('The secondary replica region for the Cosmos DB account')
param secondaryRegion string = ''

@description('The default consistency level of the Cosmos DB account')
@allowed([
  'Eventual'
  'ConsistentPrefix'
  'Session'
  'BoundedStaleness'
  'Strong'
])
param defaultConsistencyLevel string = 'Session'

@description('The backup policy of the Cosmos DB account')
@allowed([
  'Continuous'
  'Periodic'
])
param defaultBackupPolicy string

@description('Periodic backup mode, backup interval in minutes (default: one backup per 24 hours)')
@minValue(60)
@maxValue(1440)
param backupIntervalInMinutes int = 1440

@description('Periodic backup mode, backup retention interval in hours (default: 7 days retention)')
@minValue(8)
@maxValue(720)
param backupRetentionIntervalInHours int = 168

@description('Max stale requests. Required for BoundedStaleness. Valid ranges, Single Region: 10 to 1000000. Multi Region: 100000 to 1000000')
@minValue(10)
@maxValue(1000000)
param maxStalenessPrefix int = 100000

@description('Max lag time (seconds). Required for BoundedStaleness. Valid ranges, Single Region: 5 to 84600. Multi Region: 300 to 86400')
@minValue(5)
@maxValue(86400)
param maxIntervalInSeconds int = 300

@description('Enable automatic failover for regions')
param automaticFailover bool = true

@description('Ids of the subnets to associate')
param subnetIds array

@description('Role assignments at the database level')
param roleAssignments array = []

@description('Configure as serverless')
param serverless bool = false

@description('Enable or disable public network access. Use Disabled for private endpoint-only access.')
@allowed([
  'Enabled'
  'Disabled'
])
param publicNetworkAccess string = 'Disabled'

var consistencyPolicy = {
  Eventual: {
    defaultConsistencyLevel: 'Eventual'
  }
  ConsistentPrefix: {
    defaultConsistencyLevel: 'ConsistentPrefix'
  }
  Session: {
    defaultConsistencyLevel: 'Session'
  }
  BoundedStaleness: {
    defaultConsistencyLevel: 'BoundedStaleness'
    maxStalenessPrefix: maxStalenessPrefix
    maxIntervalInSeconds: maxIntervalInSeconds
  }
  Strong: {
    defaultConsistencyLevel: 'Strong'
  }
}

var backupPolicy = {
  Continuous: {
    type: 'Continuous'
  }
  Periodic: {
    type: 'Periodic'
    periodicModeProperties: {
      backupIntervalInMinutes: backupIntervalInMinutes
      backupRetentionIntervalInHours: backupRetentionIntervalInHours
    }
  }
}

var locations = [
  {
    locationName: primaryRegion
    failoverPriority: 0
    isZoneRedundant: false
  }
  {
    locationName: secondaryRegion
    failoverPriority: 1
    isZoneRedundant: false
  }
]

var serverlessLocations = [
  {
    locationName: primaryRegion
    failoverPriority: 0
    isZoneRedundant: false
  }
]

resource cosmosDbSqlApi 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: accountName
  location: location
  kind: 'GlobalDocumentDB'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    minimalTlsVersion: 'Tls12'
    consistencyPolicy: consistencyPolicy[defaultConsistencyLevel]
    locations: serverless ? serverlessLocations : locations
    databaseAccountOfferType: 'Standard'
    enableAutomaticFailover: automaticFailover
    publicNetworkAccess: publicNetworkAccess
    isVirtualNetworkFilterEnabled: true
    networkAclBypass: 'None'
    virtualNetworkRules: [for subnetId in subnetIds: {
      ignoreMissingVNetServiceEndpoint: false
      id: subnetId
    }]
    ipRules: []
    backupPolicy: backupPolicy[defaultBackupPolicy]
    capabilities: [
      serverless ? { name: 'EnableServerless' } : null
    ]    
  }
}

resource cosmosDbATP 'Microsoft.Security/advancedThreatProtectionSettings@2019-01-01' = {
  name: 'current'
  scope: cosmosDbSqlApi
  properties: {
    isEnabled: true
  }
}

// create custom role for CosmosDb
resource sqlRoleDefinition 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2021-04-15' = [for roleAssignment in roleAssignments: {
  name: '${accountName}/${guid('sql-role-definition-', roleAssignment.identityPrincipalId, cosmosDbSqlApi.id)}'
  properties: {
    roleName: roleAssignment.roleDefinitionName
    type: 'CustomRole'
    assignableScopes: [
      cosmosDbSqlApi.id
    ]
    permissions: [
      {
        dataActions: roleAssignment.dataActions
      }
    ]
  }
}]

// assign newly created role to ManagedIdentity and CosmosDb instance at the database level
resource cosmosDbRoleAssignment 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = [for roleAssignment in roleAssignments: {
  name: '${accountName}/${guid(guid('sql-role-definition-', roleAssignment.identityPrincipalId, cosmosDbSqlApi.id), roleAssignment.identityPrincipalId, cosmosDbSqlApi.id)}'
  properties: {
    principalId: roleAssignment.identityPrincipalId
    roleDefinitionId: resourceId('Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions', accountName, guid('sql-role-definition-', roleAssignment.identityPrincipalId, cosmosDbSqlApi.id))
    scope: cosmosDbSqlApi.id
  }
  dependsOn: [
    sqlRoleDefinition
  ]
}]

// output the Id of the CosmosDb instance
output cosmosDbId string = cosmosDbSqlApi.id

// output the principal ID of the system-assigned managed identity
output cosmosDbIdentityPrincipalId string = cosmosDbSqlApi.identity.principalId
