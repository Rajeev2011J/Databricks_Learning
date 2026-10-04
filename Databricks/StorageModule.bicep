@description('Name of the Storage Account')
param storageAccountName string

@description('Storage Account SKU')
param StorageSkuName string = 'Standard_LRS'

@description('Name of the existing VNet')
param virtualNetworkName string

@description('Name of the existing Subnet')
param subnetName string

@description('Is Storage Account ADLS')
param isAdls bool = true

@description('Location for all resources')
param location string = resourceGroup().location

@description('Environment name to fetch principals')
param environment string

@description('Resource type for role assignments')
param resourceType string

// Load the role assignments from the JSON file
var roleAssignmentsData = json(loadTextContent('./Parameters/roleAssignments.json')).environments[environment][resourceType]

// Load the container details from the JSON file
var containerDetailsData = json(loadTextContent('./Parameters/containerDetails.json')).environments[environment].storageAccount

// Load the subnet details from the JSON file
var subnetDetailsData = json(loadTextContent('./Parameters/subnetDetails.json')).environments[environment].storageAccount.subnets

// Define a variable to hold the additional subnets
var additionalSubnets = [for subnetId in (subnetDetailsData ?? []): {
  id: subnetId
  action: 'Allow'
}]

// Reference the existing VNet
resource existingVNet 'Microsoft.Network/virtualNetworks@2021-02-01' existing = {
  name: virtualNetworkName
}


module storageModule '../../CompliantAzureServices/StorageAccount/storage_module_incl_vnet.bicep' = {
  name: 'storageModule'
  params: {
    storageAccountName: storageAccountName
    StorageSkuName: StorageSkuName
    virtualNetworkSubnetId: resourceId('Microsoft.Network/virtualNetworks/subnets', virtualNetworkName, subnetName)
    isAdls: isAdls
    location: location
  }
}

output storageAccountId string = storageModule.outputs.storageAccountId
output storageAccount object = storageModule.outputs.storageAccount

// output the principal ID of the system-assigned managed identity
output storageAccountIdentityPrincipalId string = updateNetworkRules.identity.principalId

// Declare the storage account as an existing resource
resource existingStorageAccount 'Microsoft.Storage/storageAccounts@2021-04-01' existing = {
  name: storageAccountName
}

// Update network rules to include additional subnet IDs if available
resource updateNetworkRules 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: StorageSkuName
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    allowCrossTenantReplication: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    isHnsEnabled: isAdls
    encryption: {
      requireInfrastructureEncryption: false
      services: {
        blob: {
          enabled: true
          keyType: 'Account'
        }
        file: {
          enabled: true
          keyType: 'Account'
        }
      }
      keySource: 'Microsoft.Storage'
    }
    networkAcls: {
      virtualNetworkRules: concat([
        {
          id: resourceId('Microsoft.Network/virtualNetworks/subnets', virtualNetworkName, subnetName)
          action: 'Allow'
        }
      ], additionalSubnets)
      defaultAction: 'Deny'
    }
  }
  dependsOn: [
    storageModule
  ]
}

// Create containers in the storage account
resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = [for container in containerDetailsData: {
  name: '${storageAccountName}/default/${container.name}'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageModule
  ]
}]

resource roleAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principal in roleAssignmentsData: {
  name: guid(existingStorageAccount.id, principal.objectId, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: existingStorageAccount
  dependsOn: [
    storageModule // Ensure role assignments run after the storage module completes
  ]
  properties: {
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Fully qualified roleDefinitionId
    principalId: principal.objectId
    principalType: 'ServicePrincipal'
  }
}]