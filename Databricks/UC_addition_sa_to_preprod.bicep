@description('Resource ID of the Access Connector')
param accessConnectorResourceId string

@description('Object ID of the Access Connector')
param accessConnectorObjectId string

@description('Name of the Storage account')
param StorageAccountName string



var BlobContributorRoleDefinition =  '/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe'



//accessConnectorObjectId
//identify the right storageaccount there
resource sa 'Microsoft.Storage/storageAccounts@2022-09-01' existing = {
  name: StorageAccountName
}


// Add access rules for databricks specific
// must also specify other updates because of incompliant part for update.
// storage account; allow the databricks to do networking access.
// https://learn.microsoft.com/en-us/azure/templates/microsoft.storage/storageaccounts?pivots=deployment-language-bicep#resourceaccessrule
resource storageAccounts_resouceAccessrule 'Microsoft.Storage/storageAccounts@2025-01-01' = {
  name: StorageAccountName
  location: 'westeurope'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    //publicNetworkAccess: 'Disabled' //Needs to be enabled from specific VNET's for the infra to function
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
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
      resourceAccessRules: [
        {
          tenantId: '6e93a626-8aca-4dc1-9191-ce291b4b75a1'
          resourceId: accessConnectorResourceId
        }
      ]
      defaultAction: 'Deny'
      bypass: 'AzureServices'
      }
    }
  }


resource roleassignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  // Generate a unique but deterministic resource name
  name: guid(StorageAccountName, resourceGroup().id, accessConnectorObjectId, BlobContributorRoleDefinition)
  scope: sa
  properties: {
      principalId: accessConnectorObjectId
      roleDefinitionId: BlobContributorRoleDefinition
      principalType: 'ServicePrincipal'
  }
}

// output the principal ID of the system-assigned managed identity
output storageAccountIdentityPrincipalId string = storageAccounts_resouceAccessrule.identity.principalId
