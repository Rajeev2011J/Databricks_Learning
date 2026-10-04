/*
This pipeline deploys a function app and required supporting resources.
This pipeline is made in Bicep. For more information see:
https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/bicep-tutorial-create-first-bicep?tabs=azure-powershell
*/

// Parameters (injected from the YML pipeline)
@description('Name of the Storage Account to create')
param storageAccountName string


@description('Name of databricks identityPrincipalId')
param databricksIdentityPrincipalId string

@description('Environment name to fetch principals')
param environment string

@description('Name of Function App')
param functionAppName string

@description('Name of User assigned managed identity')
param functionAppUmiName string

@description('Name of the slot')
param slotName string

@description('Location for all resources')
param location string = resourceGroup().location

@description('Name of the app service plan for the function apps')
param appservicePlanName string

@description('SKU Name of App Service Plan')
param appservicePlanSkuName string = 'S1'

@description('name of resourceGroupName')
param resourceGroupName string = resourceGroup().name //'rg-wr-radar-api-test'

@description('Name of the Virtual Network (VNET)')
param vnetName string
param vnetAddressSpace string = '10.0.0.0/16'
param vnet object = {
    name: vnetName
    address: vnetAddressSpace
    tags: {
        createdby: 'bicep'
        How: 'pipeline'
    }
    dhcpOptions: {}
}

@description('VNET subnets')
param subnets array = [
  {
    name: 'subnet1'
    addressSpace: '10.0.0.0/24'
    serviceEndpoints: [
      {
        service: 'Microsoft.Web'
      }
      {
        service: 'Microsoft.AzureCosmosDB'
      }
      {
        service: 'Microsoft.Storage'
      }
    ]
    nsg: 'nsg-subnet1'
    delegations: [
      {
        name: 'delegation'
        properties: {
          serviceName: 'Microsoft.Web/serverfarms'
        }
      }      
    ]
    udrName: ''
    privateLinkServiceNetworkPolicies: true
    privateEndpointNetworkPolicies: true
  }
  {
    name: 'subnet2'
    addressSpace: '10.0.2.0/24'
    serviceEndpoints: [
      {
        service: 'Microsoft.AzureCosmosDB'
      }
    ]
    nsg: 'nsg-subnet2'
    delegations: []
    udrName: ''
    privateLinkServiceNetworkPolicies: true
    privateEndpointNetworkPolicies: true
  }
]

param nsgRules array = [
  {
    name: 'nsg-subnet1'
    rules: []
  }
  {
    name: 'nsg-subnet2'
    rules: []
  }
]

@description('Subnet ID of the TAS Azure Devops Build agents')
param agentsVirtualNetworkSubnetId string

@description('Allowed logAnalyticsWorkspaceName')
param logAnalyticsWorkspaceName string = ''

/*
parameters for cosmos db
*/
param baseName string = 'radarapicosmos'
param resourcesPostfix string = '1'
param provisionGremlinApi bool = true

var externalSubnetIds = json(loadTextContent('./subnetDetails.json')).environments[environment].cosmosDb.subnets

/* Module 1: Deploy VNET 
   The VNET provides means to keep network traffic between Azure Services private.
*/

var scmIpSecurityRestrictionsSettings = json(loadTextContent('./scmIpSecurityRestrictions.json')).environments[environment].functionapp.scmIpSecurityRestrictionsSettings


module vnetModule './Templates/Vnet/vnet_module.bicep' = {
  name: 'vnetDeploy'
  params: {
    vnet: vnet
    subnets: subnets
    nsgs: nsgRules
    location: location
  }
}



/* Module 2: Deploy Storage account
   This storage account is used by the FunctionApp deployed in this template. 
*/
module storageModule './Templates/StorageAccount/storage_module_incl_vnet.bicep' = {
  name: 'storageForFunctionAppDeploy'
  params: {
    storageAccountName: storageAccountName
    location: location
    virtualNetworkSubnetId: vnetModule.outputs.subnetIds[0]
  }
  dependsOn: [
    vnetModule
  ]
}

// Module3: Deploy LogAnalytics Workspace
module logAnalyticsModule './Templates/LogAnalytics/loganalytics_module.bicep' = {
  name: 'logAnalyticsDeploy'
  params: {
      logAnalyticsWorkspaceName: logAnalyticsWorkspaceName
      dailyQuotaGb: 10
      retentionInDays: 120
      skuName: 'PerGB2018'
  }
  dependsOn: [
  ]
}

// Module 4: Deploy application insights for function app logging
module appinsightsModule './Templates/ApplicationInsights/appinsights_loganalytics_module.bicep' = {
  name: 'appinsightsDeploy'
  params: {
      functionAppName: functionAppName
      workspaceResourceId: logAnalyticsModule.outputs.logAnalyticsWorkspaceId 
      location: location
  }
   dependsOn: [
      logAnalyticsModule
  ]
}

// Module 5: Deploy an AppService plan for hosting the FunctionApp
module appservicePlanModule './Templates/AppServicePlan/appserviceplan_module.bicep' = {
  name: 'appservicePlanDeploy'
  params: {
     appservicePlanName: appservicePlanName
     skuName: appservicePlanSkuName
     location: location
  }
}

/* Module 6: Deploy Managed Identity 
   The (User Assigned) Managed Identity is used by the FunctionApp as identity with which to access other services.
*/
module managedIdentityModule './Templates/ManagedIdentity/managedidentity_module.bicep' = {
  name: 'managedIdentityDeploy'
  params:{
    location: location
    managedIdentityName: functionAppUmiName
  }
}

/* Module 7: Deploy Function App
   The Module will deploy functionapp
*/ 
module functionappDeploy './functionapp_template.bicep' = {
  name: 'functionAppDeploy1'
  params: {
    functionAppName: functionAppName
    location: location
    storageAccount: storageModule.outputs.storageAccount
    storageAccountName: storageAccountName
    appservicePlanId: appservicePlanModule.outputs.appservicePlanId
    managedIdentityId: managedIdentityModule.outputs.managedIdentityId
    instrumentationKey: appinsightsModule.outputs.instrumentationKey
    subnetId: vnetModule.outputs.subnetIds[0]
    slotName: slotName
    agentsVirtualNetworkSubnetId: agentsVirtualNetworkSubnetId
    scmIpSecurityRestrictionsSettings: scmIpSecurityRestrictionsSettings
  }
  dependsOn: [
    storageModule
    vnetModule
    appinsightsModule
    appservicePlanModule
    managedIdentityModule
  ]
}

//  outputs functionAppId string = functionappDeploy.outputs.functionAppId

// /* Module 8a: Deploy CosmosDB account, Gremlin-API */
// module cosmosDbGremlinApi './Templates/CosmosDB/account/cosmosdb_gremlinapi_module.bicep' = if (provisionGremlinApi) {
//   name: 'cosmosdbgremlinapiDeploy'
//   params: {
//     accountName: '${baseName}gremlinapi${resourcesPostfix}'
//     location: location
//     primaryRegion: 'westeurope'
//     secondaryRegion: 'northeurope'
//     automaticFailover: false
//     defaultConsistencyLevel: 'BoundedStaleness'
//     maxIntervalInSeconds: 300
//     maxStalenessPrefix: 100000
//     subnetIds: [
//       vnetModule.outputs.subnetIds[1]
//     ]
//     defaultBackupPolicy: 'Periodic'
//   }
//   dependsOn: [
//     vnetModule
//   ]
// }

resource existingFunctionApp 'Microsoft.Web/sites@2022-09-01' existing = {
  name: functionAppName
  scope: resourceGroup(resourceGroupName) // resourceGroup().name
  dependsOn: [
    functionappDeploy
  ]
}

// // Retrieve existing function app properties
// resource existingFunctionApp 'Microsoft.Web/sites@2021-02-01' existing = {
//   name: 'myFunctionApp'
//   scope: resourceGroup()
// }

// // Define the new properties
// var newProperties = {
//   siteConfig: {
//     alwaysOn: true
//     netFrameworkVersion: 'v8.0'
//     appSettings: [
//         {
//           name: 'FUNCTIONS_WORKER_RUNTIME'
//           value: 'dotnet-isolated'
//         }
//         {
//           name: 'FUNCTIONS_EXTENSION_VERSION'
//           value: '~4'
//         }
//     ]
//   }
// }

// // Merge existing and new properties
// resource updateFunctionApp 'Microsoft.Web/sites@2021-02-01' = {
//   name: functionAppName
//   location: location
//   properties: union(existingFunctionApp.properties, newProperties)
//   dependsOn: [
//     functionappDeploy
//     existingFunctionApp
//   ]
// }

// Combine internal and external subnet IDs
var combinedSubnetIds = concat(
  [vnetModule.outputs.subnetIds[0]], // Internal subnet created in this deployment
  externalSubnetIds ?? []            // External subnets from config
)

/* Module 8b: Deploy CosmosDB account, GSQL-API */
module cosmosDbSQLApi './cosmosdb_sqlapi_module.bicep' = if (provisionGremlinApi) {
  name: 'cosmosdbSQLDeploy'
  params: {
    accountName: '${baseName}sqlapi${resourcesPostfix}'
    location: location
    primaryRegion: 'westeurope'
    secondaryRegion: 'northeurope'
    automaticFailover: false
    defaultConsistencyLevel: 'BoundedStaleness'
    maxIntervalInSeconds: 300
    maxStalenessPrefix: 100000
    subnetIds: combinedSubnetIds
    // subnetIds: vnetModule.outputs.subnetIds
    defaultBackupPolicy: 'Periodic'
    publicNetworkAccess: 'Enabled'  // Required for VNet service endpoints
    roleAssignments: [
      {
        roleDefinitionName: 'DatabricksMassUpdaterRole'
        identityPrincipalId: databricksIdentityPrincipalId //'33902b23-5d59-40d8-8a5b-554e0eefb385'         // Databricks app reg
        dataActions: [
          'Microsoft.DocumentDB/databaseAccounts/readMetadata'
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/*'
        ]
      }
      {
        roleDefinitionName: 'APIDataReaderRole'
        identityPrincipalId: existingFunctionApp.identity.principalId //functionappDeploy.outputs.functionAppId //'efbf925c-d5c7-4fe5-8c88-a10ff16bcf54' //functionappDeploy.outputs.funcAppsPrincIds //  '70a09b74-7f4a-47c1-863d-d1b7b866398f'         // Function App managed Identity
        dataActions: [
          'Microsoft.DocumentDB/databaseAccounts/readMetadata'
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/*'
        ]
      }
    ]                                     // This does something in cusotm role in template https://dev.azure.com/raboweb/Compliant%20Azure%20Services/_git/CompliantAzureServices?path=/CosmosDB/account/cosmosdb_sqlapi_module.bicep&version=GBmaster
  }
  dependsOn: [
    vnetModule
    functionappDeploy
    existingFunctionApp
  ]
}


// /* Module 9a: Create a database without specifying throughput at the database level */
// module cosmosDbGremlinApiDatabase './Templates/CosmosDB/database/cosmosdb_gremlinapi_database_module.bicep' = if (provisionGremlinApi) {
//   name: 'cosmosdbgremlinapidatabaseDeploy'
//   params: {
//     accountName: '${baseName}gremlinapi${resourcesPostfix}'
//     databaseName: 'cosmosdbgremlinapidatabase'
//   }
//   dependsOn: [
//     cosmosDbGremlinApi
//   ]
// }


/* Module 9b: Create a database without specifying throughput at the database level */
// https://dev.azure.com/raboweb/Compliant%20Azure%20Services/_git/CompliantAzureServices?path=/CosmosDB/database/cosmosdb_sqlapi_database_module.bicep&version=GBmaster
module cosmosDbSQLApiDatabase './Templates/CosmosDB/database/cosmosdb_sqlapi_database_module.bicep' = if (provisionGremlinApi) {
  name: 'cosmosdbsqlapidatabaseDeploy'
  params: {
    accountName: '${baseName}sqlapi${resourcesPostfix}'
    databaseName: 'radarapisqldb'
  }
  dependsOn: [
    cosmosDbSQLApi
  ]
}

/*
  10a. Add KONG VNET whitelist to function app network
*/

/*
  10b. set up authentication/integration?
*/

/*
  11. CREATE Express route in Dev for Database
*/
