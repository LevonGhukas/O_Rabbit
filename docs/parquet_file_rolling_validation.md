# Parquet File Rolling & Artifact Validation Specification

## Overview

O_Rabbit streams data from relational and document databases directly into Apache Parquet format, splitting large result sets into rolled files according to target file size thresholds and verifying all objects prior to manifest commitment.

## Target File Sizing & Part Rolling

1. **Target File Bytes (`target_file_bytes`)**:
   - Extraction tasks stream Arrow record batches into an open Parquet file writer.
   - When the serialized bytes reach `target_file_bytes` (e.g. 128MB / 256MB), the writer seals the file and rolls over to the next part suffix (`part-00001-00000.parquet`, `part-00001-00001.parquet`, etc.).
2. **Row Count Tracking**:
   - Each rolled file tracks its exact row count and byte size.
   - Upon upload, the SHA-256 checksum is calculated and reported in `ReportTaskResult`.

## Artifact Integrity & Manifest Durability

1. **Pre-Commit Verification**:
   - The master server validates that all reported Parquet keys exist in object storage with matching sizes and SHA-256 digests before finalizing the run.
2. **Atomic Manifest Commit**:
   - A durable manifest (`manifest.json`) is written atomically to S3 target prefix with manifest versioning (`v2`).
   - If the master restarts or loses leadership during committing, reconciliation reads the durable manifest and converges idempotently.
