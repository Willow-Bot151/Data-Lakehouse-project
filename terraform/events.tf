resource "aws_cloudwatch_event_rule" "scheduler" {
    name        = "Lambda_schedule"
    description = "Schedule lambda cronjob every 10 minutes"
    schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_lambda_function.ingestion_lambda_handler.arn
}