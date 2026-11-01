// Azure resources for PulseCity SmartGrid, deployed into one resource group:
//   az deployment group create -g rg-pulsecity -f infra/main.bicep -p alertEmail=you@example.com
//
// Creates an ADLS Gen2 storage account, a Premium Azure Databricks workspace (Unity Catalog),
// an Access Connector (managed identity) with Storage Blob Data Contributor on the storage
// account, and a monthly budget alert on the resource group.

@description('Azure region for every resource.')
param location string = resourceGroup().location

@description('Short name used in resource names.')
@maxLength(12)
param prefix string = 'pulsecity'

@description('Storage account name. Must be globally unique, 3-24 lowercase letters and digits.')
@maxLength(24)
param storageAccountName string = take('st${prefix}${uniqueString(resourceGroup().id)}', 24)

@description('Address that receives budget alerts.')
param alertEmail string

@description('Monthly budget for the resource group, in the subscription currency. 0 skips the budget.')
param monthlyBudget int = 30

@description('First day of the budget period. Leave the default.')
param budgetStartDate string = utcNow('yyyy-MM-01')

var containerName = 'lakehouse'
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var tags = {
  project: 'pulsecity-smartgrid'
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true // ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    accessTier: 'Hot'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

resource accessConnector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: 'dbac-${prefix}'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

resource connectorCanUseStorage 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, accessConnector.id, storageBlobDataContributor)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      storageBlobDataContributor
    )
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: 'dbw-${prefix}'
  location: location
  tags: tags
  sku: {
    name: 'premium' // required for Unity Catalog
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      'rg-${prefix}-databricks-managed'
    )
  }
}

resource budget 'Microsoft.Consumption/budgets@2023-05-01' = if (monthlyBudget > 0) {
  name: 'budget-${prefix}'
  properties: {
    category: 'Cost'
    amount: monthlyBudget
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    notifications: {
      actual50: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 50
        thresholdType: 'Actual'
        contactEmails: [alertEmail]
      }
      actual90: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 90
        thresholdType: 'Actual'
        contactEmails: [alertEmail]
      }
      forecast100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: [alertEmail]
      }
    }
  }
}

output workspaceUrl string = 'https://${workspace.properties.workspaceUrl}'
output accessConnectorId string = accessConnector.id
output storageAccountName string = storage.name
output storageRoot string = 'abfss://${containerName}@${storage.name}.dfs.${environment().suffixes.storage}/${prefix}'
