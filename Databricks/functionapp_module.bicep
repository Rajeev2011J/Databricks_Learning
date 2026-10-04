/*
 * Module:
 * Deploy a .NET function with ASP
 * 
 * Description:
 * This module deploys a .NET function app with an ASP
 * 
 * More information:
 * https://docs.microsoft.com/en-us/azure/templates/microsoft.web/sites?tabs=bicep
 */


// Parameters
@description('Name of Function App')
param functionAppName string

@description('Location')
param location string

@description('Id of appservice plan')
param appservicePlanId string

@description('Id of the managed identity')
param managedIdentityId string

@description('Azure AD Tenant ID for authentication')
param tenantId string

@description('Azure AD Client ID (App Registration) for authentication')
param clientId string

// @description('ID of managed identity')
// param additionalAgentsSubnetId string

param agentsVirtualNetworkSubnetId string

@description('ipSecurityRestrictionsSettings array')
param ipSecurityRestrictionsSettings array = []

 @description('SCM IP security restrictions array passed from root')
 param scmIpSecurityRestrictionsSettings array

// @description('scmIpSecurityRestrictionsSettings array')
// param scmIpSecurityRestrictionsSettings array = [
//     {
//           name: 'DevOps-agents subnet'
//           description: 'allow access from DevOps-agents for deployment'
//           vnetSubnetResourceId: agentsVirtualNetworkSubnetId
//           action: 'Allow'
//           priority: 100
//     }
//     {
//           name: 'Additional-agent-subnet'
//           description: 'allowing the access to new devops agents for the deployment'
//           vnetSubnetResourceId: additionalAgentsSubnetId
//           action: 'Allow'
//           priority: 200

//     }
// ]

// param agentsVirtualNetworkSubnetId string = '/subscriptions/0cea37a3-6bdc-43cb-be5f-e6d390b05a3c/resourceGroups/rg-erconnect-azdo-prd/providers/Microsoft.Network/virtualNetworks/vnet-erconnect-azdo-prd-we/subnets/snet-workload-azdo-prd-we'

// param workspaceId string
// FunctionApp (for demo application)
resource function_app 'Microsoft.Web/sites@2022-09-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned, UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    httpsOnly: true
    serverFarmId: appservicePlanId
    clientAffinityEnabled: true
    clientCertEnabled: true
    clientCertMode: 'Optional'
    siteConfig: {
      alwaysOn: true
      ftpsState: 'Disabled'
      minTlsVersion: '1.3'
      scmMinTlsVersion: '1.3'
      scmIpSecurityRestrictionsDefaultAction: 'Deny'
      scmIpSecurityRestrictions: scmIpSecurityRestrictionsSettings
      ipSecurityRestrictionsDefaultAction: 'Deny'
      ipSecurityRestrictions: ipSecurityRestrictionsSettings
    }
    authsettingsV2: {
      platform: {
        enabled: true
      }
      globalValidation: {
        requireAuthentication: true
        unauthenticatedClientAction: 'Return401'
      }
      identityProviders: {
        azureActiveDirectory: {
          enabled: true
          registration: {
            openIdIssuer: 'https://sts.windows.net/${tenantId}/v2.0'
            clientId: clientId
          }
          validation: {
            allowedAudiences: [
              'api://${clientId}'
            ]
          }
        }
      }
    }
  }

  dependsOn: [
  ]
}

output functionAppId string = function_app.id
