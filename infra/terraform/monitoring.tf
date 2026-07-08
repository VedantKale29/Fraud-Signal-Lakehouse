# Observability (SS6.1): alarms on the metrics the Python layer emits
# (common/metrics.py -> namespace FraudSignalLakehouse), an alert topic,
# and the billing guardrail.

resource "aws_sns_topic" "alerts" {
  name              = "fsl-alerts-${var.env}"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Freshness: silver hasn't been updated in > 26h => the daily DAG is broken
resource "aws_cloudwatch_metric_alarm" "silver_freshness" {
  alarm_name          = "fsl-silver-freshness-${var.env}"
  namespace           = "FraudSignalLakehouse"
  metric_name         = "silver_age_hours"
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 26
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching" # no metric = pipeline dead = alert
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Volume anomaly: a day with 0 rows out of silver is a silent failure
resource "aws_cloudwatch_metric_alarm" "silver_rows" {
  alarm_name          = "fsl-silver-zero-rows-${var.env}"
  namespace           = "FraudSignalLakehouse"
  metric_name         = "silver_rows_written"
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_budgets_budget" "cap" {
  name         = "fsl-monthly-cap"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}

resource "aws_cloudwatch_dashboard" "ops" {
  dashboard_name = "fsl-ops-${var.env}"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6,
        properties = {
          title   = "Silver rows written / day"
          metrics = [["FraudSignalLakehouse", "silver_rows_written"]]
          region  = var.aws_region, stat = "Sum", period = 86400
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6,
        properties = {
          title   = "Quarantine rows / day (rising = upstream data rot)"
          metrics = [["FraudSignalLakehouse", "quarantine_rows_written"]]
          region  = var.aws_region, stat = "Sum", period = 86400
        }
      }
    ]
  })
}
