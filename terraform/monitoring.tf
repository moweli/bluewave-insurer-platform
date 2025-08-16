# Monitoring and Observability Resources

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-logs-001"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.tags
}

# Application Insights
resource "azurerm_application_insights" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-appinsights-001"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "web"

  retention_in_days = var.log_retention_days

  tags = local.tags
}

# Action Group for Alerts
resource "azurerm_monitor_action_group" "main" {
  name                = "${var.prefix}-${var.environment}-action-group"
  resource_group_name = azurerm_resource_group.rg.name
  short_name          = "bw-alerts"

  email_receiver {
    name                    = "sendtodevops"
    email_address           = var.alert_email
    use_common_alert_schema = true
  }

  tags = local.tags
}

# Budget Alert
resource "azurerm_consumption_budget_resource_group" "main" {
  name              = "${var.prefix}-${var.environment}-budget"
  resource_group_id = azurerm_resource_group.rg.id

  amount     = var.monthly_budget
  time_grain = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  notification {
    enabled        = true
    threshold      = 50.0
    operator       = "GreaterThan"
    threshold_type = "Actual"

    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 80.0
    operator       = "GreaterThan"
    threshold_type = "Actual"

    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 100.0
    operator       = "GreaterThan"
    threshold_type = "Forecasted"

    contact_emails = [var.alert_email]
  }

  lifecycle {
    ignore_changes = [time_period[0].start_date]
  }
}

# Diagnostic Settings for Storage Account
resource "azurerm_monitor_diagnostic_setting" "storage" {
  name               = "${var.prefix}-${var.environment}-storage-diag"
  target_resource_id = azurerm_storage_account.sa.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  metric {
    category = "Transaction"
    enabled  = true
  }

  metric {
    category = "Capacity"
    enabled  = true
  }

  enabled_log {
    category = "StorageRead"
  }

  enabled_log {
    category = "StorageWrite"
  }

  enabled_log {
    category = "StorageDelete"
  }
}

# Diagnostic Settings for Event Hubs
resource "azurerm_monitor_diagnostic_setting" "eventhubs" {
  name               = "${var.prefix}-${var.environment}-eventhubs-diag"
  target_resource_id = azurerm_eventhub_namespace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  metric {
    category = "AllMetrics"
    enabled  = true
  }

  enabled_log {
    category = "ArchiveLogs"
  }

  enabled_log {
    category = "OperationalLogs"
  }

  enabled_log {
    category = "AutoScaleLogs"
  }
}

# Diagnostic Settings for Key Vault
resource "azurerm_monitor_diagnostic_setting" "keyvault" {
  name               = "${var.prefix}-${var.environment}-keyvault-diag"
  target_resource_id = azurerm_key_vault.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  metric {
    category = "AllMetrics"
    enabled  = true
  }

  enabled_log {
    category = "AuditEvent"
  }

  enabled_log {
    category = "AzurePolicyEvaluationDetails"
  }
}

# Storage Account Availability Alert
resource "azurerm_monitor_metric_alert" "storage_availability" {
  name                = "${var.prefix}-${var.environment}-storage-availability"
  resource_group_name = azurerm_resource_group.rg.name
  scopes              = [azurerm_storage_account.sa.id]
  description         = "Alert when storage account availability drops below 99.9%"

  severity    = 2
  frequency   = "PT5M"
  window_size = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "Availability"
    aggregation      = "Average"
    operator         = "LessThan"
    threshold        = 99.9
  }

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }

  tags = local.tags
}

# Event Hub Throttled Requests Alert
resource "azurerm_monitor_metric_alert" "eventhub_throttled" {
  name                = "${var.prefix}-${var.environment}-eventhub-throttled"
  resource_group_name = azurerm_resource_group.rg.name
  scopes              = [azurerm_eventhub_namespace.main.id]
  description         = "Alert when Event Hub has throttled requests"

  severity    = 2
  frequency   = "PT5M"
  window_size = "PT15M"

  criteria {
    metric_namespace = "Microsoft.EventHub/namespaces"
    metric_name      = "ThrottledRequests"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.main.id
  }

  tags = local.tags
}