@description('Name of the Storage Account')
param storageAccountName string

@description('Storage Account SKU')
param StorageSkuName string = 'Standard_LRS'


@description('Is Storage Account ADLS')
param isAdls bool = true

@description('Location for all resources')
param location string = resourceGroup().location

@description('Environment name to fetch principals')
param environment string

// Load the subnet details from the JSON file
var subnetDetailsData = json(loadTextContent('./Parameters/tfstateStorageSubnetRules.json')).environments[environment].storageAccount.subnets

// Define a variable to hold the additional subnets
var additionalSubnets = [for subnetId in (subnetDetailsData ?? []): {
  id: subnetId
  action: 'Allow'
}]


// @description('Role definition id')
// param roleDefinitionId string

// @description('Principal id')
// param principalId string






module storageModule '../../CompliantAzureServices/StorageAccount/storage_module.bicep' = {
  name: 'storageModule'
  params: {
    storageAccountName: storageAccountName
    StorageSkuName: StorageSkuName
    isAdls: isAdls
    location: location
  }
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
      virtualNetworkRules: additionalSubnets
      defaultAction: 'Deny'
    }
  }
  dependsOn: [
    storageModule
  ]
}

// Create a container for Terraform state
resource tfstateContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = {
  name: '${storageAccountName}/default/tfstate'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageModule
  ]
}

// output the principal ID of the system-assigned managed identity
output storageAccountIdentityPrincipalId string = updateNetworkRules.identity.principalId

