output "state_machine_name" {
  description = "Deployed Step Functions state machine name."
  value       = aws_sfn_state_machine.jodi_oil_pipeline.name
}

output "state_machine_arn" {
  description = "Deployed Step Functions state machine ARN."
  value       = aws_sfn_state_machine.jodi_oil_pipeline.arn
}

output "step_functions_log_group_name" {
  description = "CloudWatch log group used by Step Functions."
  value       = aws_cloudwatch_log_group.step_functions.name
}
