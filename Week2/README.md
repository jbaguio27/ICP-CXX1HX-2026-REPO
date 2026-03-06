# Week 2

Week 2 covers raw data landing in S3 for the AWS-only pipeline.

## Deliverables
- [S3 Upload Runbook](./docs/s3_upload.md)
- [UI Runbook + Screenshot Guide](./docs/ui_runbook/README.md)

## Scope
- Upload JODI-Oil CSV files (2021-2025) to S3 raw prefix.
- Use `.env` `RAW_URI` as the source for target raw path.
- Verify raw objects exist before Spark Silver processing.
- No manifest generation step is used in this project.

## Current Status
- Raw CSV files are uploaded to S3 via AWS Console UI.
