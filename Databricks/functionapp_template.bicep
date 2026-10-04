/*
 * Module:
 * Deploy a function with ASP, appsettings, vnet integration and a deployment slot
 * 
 * Description:
 * This module deploys function app with an ASP, appsettings, vnet integration and a deployment slot
 */

 @description('Name of Function App')
 param functionAppName string
 
 @description('Location')
 param location string
 
 @description('Storage object')
 param storageAccount object
 
 @description('Storage accountName')
 param storageAccountName string
 
 @description('Instrumentation key for App insights')
 param instrumentationKey string
 
 @description('ID of appservice plan')
 param appservicePlanId string
 
 @description('ID of managed identity')
 param managedIdentityId string
 
 @description('ID of the subnet')
 param subnetId string
 
 @description('Name of the slot')
 param slotName string

 @description('SCM IP security restrictions array passed from root')
 param scmIpSecurityRestrictionsSettings array

 param agentsVirtualNetworkSubnetId string

 @description('Azure AD Tenant ID for authentication')
 param tenantId string

 @description('Azure AD Client ID (App Registration) for authentication')
 param clientId string


 resource siteConfig 'Microsoft.Web/sites/config@2022-03-01' = {
  name: '${functionAppName}/web'
  dependsOn: [
    functionappDeploy
    functionapp1VnetModule
    appsettingsModule
  ]
  properties: {
    netFrameworkVersion: 'v10.0'
  }
}


// Module 1: Deploy function app infrastructure
 module functionappDeploy './functionapp_module.bicep' = {
   name: 'functionAppDeploy'
   params: {
     functionAppName: functionAppName
     location: location
     appservicePlanId: appservicePlanId
     managedIdentityId: managedIdentityId
     agentsVirtualNetworkSubnetId: agentsVirtualNetworkSubnetId
     scmIpSecurityRestrictionsSettings: scmIpSecurityRestrictionsSettings
     tenantId: tenantId
     clientId: clientId
   }
   dependsOn: []
 }
 

// Module 2: Connect functionapp to vnet
 module functionapp1VnetModule './Templates/FunctionApp/functionapp_vnet_module.bicep' = {
   name: 'functionappVnetConnect'
   params: {
     functionAppName: functionAppName
     subnetId: subnetId
   }
   dependsOn: [
     functionappDeploy
   ]  
 }


// Module 3: Deploy Appsettings to the function app
 module appsettingsModule './Templates/FunctionApp/functionapp_appsettings_module.bicep' = {
   name: 'appsettingDeploy'
   params: {
     functionappName: functionAppName
     appsettings:   {
       'FUNCTIONS_EXTENSION_VERSION': '~4'
       'AzureWebJobsStorage': 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};AccountKey=${listKeys(storageAccount.resourceId, storageAccount.apiVersion).keys[0].value};EndpointSuffix=${environment().suffixes.storage}'
       'APPINSIGHTS_INSTRUMENTATIONKEY': instrumentationKey
       'WEBSITE_RUN_FROM_PACKAGE': '1'
       'FUNCTIONS_WORKER_RUNTIME': 'dotnet-isolated'
       'CONTAINER': 'ClientData'
       'DATABASE': 'CosmosDocDB'
       'RADARCONTAINER': 'RDMClientData'
     } 
   }
   dependsOn: [
     functionapp1VnetModule
     functionappDeploy
   ]
 }

//  resource siteConfig 'Microsoft.Web/sites/config@2022-03-01' = {
//   name: '${functionAppName}/web'
//   properties: {
//     netFrameworkVersion: 'v10.0'
//   }
// }

 
// Module 4: create a deployment slot
 module deploymentSlotModule './Templates/FunctionApp/functionapp_slot_module.bicep' = {
   name: 'deploymentSlotDeply'
   params: {
     functionAppName: functionAppName
     slotName: slotName
     servicePlanId: appservicePlanId
     location: location
   }
   dependsOn: [
     functionappDeploy
     appsettingsModule
     functionapp1VnetModule
   ]
 }
 