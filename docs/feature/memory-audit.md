# Memory operation audit

Status: shipped

Inspect the durable history of the multi-model `Database` through `audit_records` or trace a single operation with `inspect_operation`. The append-only `audit_journal_v1` records query plans, model requests and responses, evidence IDs, configuration changes, mutation snapshots, compaction, failures and repairs.

A snapshot retains reviewable text and metadata. Sanitization removes raw vectors, blob bytes, credentials, tokens and endpoints before persistence. The journal records which decision was made and with what inputs. A proposal's replay cursor and a dream run's completion state live with their respective operations.

See the [audit schema and sanitization](../../crates/mmdb/src/audit.rs).
