/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * SPDX-License-Identifier: MIT
 *-------------------------------------------------------------------------
 */

//! Single-flight native SQL collection; only delivered batches advance the cursor.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{task::JoinHandle, time};
use tokio_postgres::{config::SslMode, error::SqlState, types::ToSql, Client, Config, NoTls, Row};

use crate::{
    identity, Error, ExportScope, NativeSpan, OtlpSink, MAX_BATCH_BYTES, MAX_BATCH_RECORDS,
    MAX_CONFIG_BYTES, MAX_RECORD_BYTES, OPERATION_TIMEOUT, SCHEMA_VERSION,
};

const CAPABILITY_SQL: &str =
    "SELECT documentdb_api_internal.backend_telemetry_capabilities()::text";
const READ_SQL: &str =
    "SELECT documentdb_api_internal.backend_telemetry_read($1::text::uuid, $2, $3)::text";
const DISCOVER_SQL: &str = "SELECT \
    to_regprocedure('documentdb_api_internal.backend_telemetry_capabilities()') IS NOT NULL \
    AND to_regprocedure('documentdb_api_internal.backend_telemetry_read(uuid,bigint,integer)') IS NOT NULL";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LossCounters {
    admission: u64,
    contention: u64,
    overwrite: u64,
    oversize: u64,
    clock: u64,
    identity: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capabilities {
    schema_version: u32,
    database_oid: u32,
    recording_enabled: bool,
    context_enabled: bool,
    storage_allocated: bool,
    capacity: usize,
    max_record_bytes: usize,
    max_batch_records: usize,
    max_batch_bytes: usize,
    admission_per_second: u32,
    producer_instance_id: Option<String>,
    epoch: Option<String>,
    oldest_sequence: Option<u64>,
    latest_sequence: Option<u64>,
    loss: Option<LossCounters>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Source {
    database_oid: u32,
    producer_instance_id: String,
    epoch: String,
}

/// A source-bound cursor held in memory, not a durable delivery checkpoint.
#[derive(Clone, Debug, Serialize)]
pub struct NativeCheckpoint {
    source: Source,
    after_sequence: u64,
}

/// Describes an explicit binding or recovery without claiming exact loss counts.
#[derive(Debug, Serialize)]
pub struct NativeResumeReport {
    checkpoint: NativeCheckpoint,
    history_gap: bool,
    source_changed: bool,
    replay_possible: bool,
}

impl NativeResumeReport {
    /// Earlier source history is unavailable or continuity cannot be established.
    #[must_use]
    pub const fn history_gap(&self) -> bool {
        self.history_gap
    }

    /// The configured database now reports a different producer or epoch.
    #[must_use]
    pub const fn source_changed(&self) -> bool {
        self.source_changed
    }

    /// No acknowledged cursor was supplied, so retained spans may be replayed.
    #[must_use]
    pub const fn replay_possible(&self) -> bool {
        self.replay_possible
    }
}

struct SourceSnapshot {
    source: Source,
    oldest_sequence: u64,
    latest_sequence: u64,
}

impl SourceSnapshot {
    fn resume(&self, previous: Option<&NativeCheckpoint>) -> Result<NativeResumeReport, Error> {
        let first_cursor = self.oldest_sequence.saturating_sub(1);
        let same_source = previous.filter(|checkpoint| checkpoint.source == self.source);
        if same_source.is_some_and(|checkpoint| checkpoint.after_sequence > self.latest_sequence) {
            return Err(Error::Invalid(
                "native resume cursor exceeds source history",
            ));
        }
        let source_changed = previous.is_some() && same_source.is_none();
        let history_gap = source_changed
            || same_source.map_or(first_cursor > 0, |checkpoint| {
                checkpoint.after_sequence < first_cursor
            });
        let after_sequence = same_source.map_or(first_cursor, |checkpoint| {
            checkpoint.after_sequence.max(first_cursor)
        });
        Ok(NativeResumeReport {
            checkpoint: NativeCheckpoint {
                source: self.source.clone(),
                after_sequence,
            },
            history_gap,
            source_changed,
            replay_possible: previous.is_none(),
        })
    }
}

impl TryFrom<Capabilities> for Source {
    type Error = Error;

    fn try_from(value: Capabilities) -> Result<Self, Self::Error> {
        if value.schema_version != SCHEMA_VERSION
            || !value.recording_enabled
            || !value.context_enabled
            || !value.storage_allocated
        {
            return Err(Error::NativeUnavailable);
        }
        if value.database_oid == 0
            || value.capacity == 0
            || value.admission_per_second == 0
            || value.max_record_bytes != MAX_RECORD_BYTES
            || value.max_batch_records != MAX_BATCH_RECORDS
            || value.max_batch_bytes != MAX_BATCH_BYTES
            || value.loss.is_none()
        {
            return Err(Error::Invalid("native capability limits"));
        }
        validate_range(
            value.oldest_sequence.ok_or(Error::NativeUnavailable)?,
            value.latest_sequence.ok_or(Error::NativeUnavailable)?,
        )?;
        let source = Self {
            database_oid: value.database_oid,
            producer_instance_id: value.producer_instance_id.ok_or(Error::NativeUnavailable)?,
            epoch: value.epoch.ok_or(Error::NativeUnavailable)?,
        };
        identity::validate_hex(&source.producer_instance_id, 32)?;
        identity::validate_hex(&source.epoch, 32)?;
        Ok(source)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    epoch: String,
    after_sequence: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadBatch {
    schema_version: u32,
    database_oid: u32,
    producer_instance_id: String,
    epoch: String,
    oldest_sequence: u64,
    latest_sequence: u64,
    epoch_reset: bool,
    gap: bool,
    next_cursor: Cursor,
    loss: LossCounters,
    records: Vec<Value>,
}

#[derive(Debug)]
struct PendingBatch {
    next_sequence: u64,
    loss: LossCounters,
    records: Vec<NativeSpan>,
}

impl PendingBatch {
    fn decode(bytes: &[u8], source: &Source, after_sequence: u64) -> Result<Self, Error> {
        if bytes.len() > MAX_BATCH_BYTES {
            return Err(Error::Invalid("native SQL batch byte limit"));
        }
        let batch: ReadBatch = serde_json::from_slice(bytes)
            .map_err(|_error| Error::Invalid("native SQL batch schema"))?;
        if batch.schema_version != SCHEMA_VERSION {
            return Err(Error::NativeUnavailable);
        }
        if batch.epoch_reset
            || batch.database_oid != source.database_oid
            || batch.producer_instance_id != source.producer_instance_id
            || batch.epoch != source.epoch
        {
            return Err(Error::NativeSourceChanged);
        }
        validate_range(batch.oldest_sequence, batch.latest_sequence)?;
        if batch.gap || (batch.oldest_sequence > 0 && after_sequence < batch.oldest_sequence - 1) {
            return Err(Error::NativeGap);
        }
        if batch.records.len() > MAX_BATCH_RECORDS
            || batch.next_cursor.epoch != source.epoch
            || batch.next_cursor.after_sequence < after_sequence
            || batch.next_cursor.after_sequence > batch.latest_sequence
            || (batch.next_cursor.after_sequence == after_sequence
                && batch.latest_sequence > after_sequence)
        {
            return Err(Error::Invalid("native SQL cursor"));
        }
        let mut records = Vec::new();
        let mut sequence = after_sequence;
        for value in batch.records {
            let bytes =
                serde_json::to_vec(&value).map_err(|_error| Error::Invalid("native SQL record"))?;
            let record = NativeSpan::from_json(&bytes)?;
            if record.producer_instance_id() != source.producer_instance_id
                || record.epoch() != source.epoch
                || record.sequence() <= sequence
                || record.sequence() < batch.oldest_sequence
                || record.sequence() > batch.next_cursor.after_sequence
            {
                return Err(Error::Invalid("native SQL record source or sequence"));
            }
            sequence = record.sequence();
            records.push(record);
        }
        Ok(Self {
            next_sequence: batch.next_cursor.after_sequence,
            loss: batch.loss,
            records,
        })
    }
}

struct Session {
    client: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Session {
    async fn connect(uri: &str) -> Result<Self, Error> {
        if uri.len() > MAX_CONFIG_BYTES {
            return Err(Error::Invalid("native connection configuration byte limit"));
        }
        let mut config: Config = uri
            .parse()
            .map_err(|_error| Error::Invalid("native connection configuration"))?;
        // This PoC uses an isolated local endpoint; never silently downgrade TLS.
        if config.get_ssl_mode() != SslMode::Disable {
            return Err(Error::Invalid(
                "native demo requires explicit sslmode=disable",
            ));
        }
        config.connect_timeout(OPERATION_TIMEOUT);
        config.application_name("documentdb-backend-telemetry");
        let (client, connection) = time::timeout(OPERATION_TIMEOUT, config.connect(NoTls))
            .await
            .map_err(|_error| Error::NativeTimeout)?
            .map_err(|error| sql_error(&error))?;
        let session = Self {
            client,
            connection: tokio::spawn(connection),
        };
        time::timeout(
            OPERATION_TIMEOUT,
            session.client.batch_execute(
                "SET default_transaction_read_only = on; SET statement_timeout = '2s'",
            ),
        )
        .await
        .map_err(|_error| Error::NativeTimeout)?
        .map_err(|error| sql_error(&error))?;
        Ok(session)
    }

    async fn query_one(&self, sql: &str, parameters: &[&(dyn ToSql + Sync)]) -> Result<Row, Error> {
        if self.connection.is_finished() {
            return Err(Error::NativeFailed);
        }
        time::timeout(OPERATION_TIMEOUT, self.client.query_one(sql, parameters))
            .await
            .map_err(|_error| Error::NativeTimeout)?
            .map_err(|error| sql_error(&error))
    }

    async fn snapshot(&self) -> Result<SourceSnapshot, Error> {
        let json: String = self
            .query_one(CAPABILITY_SQL, &[])
            .await?
            .try_get(0)
            .map_err(|error| sql_error(&error))?;
        if json.len() > MAX_CONFIG_BYTES {
            return Err(Error::Invalid("native capability byte limit"));
        }
        let capabilities: Capabilities = serde_json::from_str(&json)
            .map_err(|_error| Error::Invalid("native capability schema"))?;
        let oldest_sequence = capabilities
            .oldest_sequence
            .ok_or(Error::NativeUnavailable)?;
        let latest_sequence = capabilities
            .latest_sequence
            .ok_or(Error::NativeUnavailable)?;
        Ok(SourceSnapshot {
            source: Source::try_from(capabilities)?,
            oldest_sequence,
            latest_sequence,
        })
    }

    async fn source(&self) -> Result<Source, Error> {
        Ok(self.snapshot().await?.source)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

/// A sanitized native-collection acknowledgement, not an execution span or plan.
#[derive(Debug, Serialize)]
pub struct NativeCollectionReport {
    schema_version: u32,
    origin: &'static str,
    producer_instance_id: String,
    epoch: String,
    database_oid: u32,
    after_sequence: u64,
    exported_records: usize,
    loss: LossCounters,
}

impl NativeCollectionReport {
    /// Serializes source identity, the delivered cursor, and producer counters.
    ///
    /// # Errors
    /// Reports serialization failure without exposing SQL or credentials.
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|_error| Error::Invalid("native collection report"))
    }

    /// Returns the number of records delivered by this poll.
    #[must_use]
    pub const fn exported_records(&self) -> usize {
        self.exported_records
    }
}

/// Owns one SQL connection and at most one immutable native export batch.
pub struct NativeCollector {
    session: Session,
    source: Source,
    sink: OtlpSink,
    after_sequence: u64,
    pending: Option<PendingBatch>,
}

impl fmt::Debug for NativeCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeCollector")
            .field("source", &self.source)
            .field("after_sequence", &self.after_sequence)
            .finish_non_exhaustive()
    }
}

impl NativeCollector {
    /// Connects to the approved local SQL source and discovers its real producer identity.
    ///
    /// # Errors
    /// Rejects implicit/required TLS, unavailable signatures or recording, invalid
    /// protocol/capability data, denied access, timeouts, and exporter setup failures.
    pub async fn connect(
        uri: &str,
        endpoint: &str,
        namespace: String,
        cluster: String,
    ) -> Result<Self, Error> {
        Ok(Self::open(uri, endpoint, namespace, cluster).await?.0)
    }

    /// Reconnects at an acknowledged cursor, or at the oldest retained record.
    ///
    /// The returned report makes initial replay, missing history, and changed
    /// source identities explicit. It does not infer lost span counts from the
    /// instance-wide sequence range.
    ///
    /// # Errors
    /// Reports the same configuration and discovery failures as [`Self::connect`],
    /// and rejects a cursor beyond the unchanged source's latest sequence.
    pub async fn resume(
        uri: &str,
        endpoint: &str,
        namespace: String,
        cluster: String,
        previous: Option<&NativeCheckpoint>,
    ) -> Result<(Self, NativeResumeReport), Error> {
        let (mut collector, snapshot) = Self::open(uri, endpoint, namespace, cluster).await?;
        let report = snapshot.resume(previous)?;
        collector.after_sequence = report.checkpoint.after_sequence;
        Ok((collector, report))
    }

    /// Returns the last acknowledged cursor for reconnection within this process.
    #[must_use]
    pub fn checkpoint(&self) -> NativeCheckpoint {
        NativeCheckpoint {
            source: self.source.clone(),
            after_sequence: self.after_sequence,
        }
    }

    /// Returns the bounded number of records whose delivery is not yet confirmed.
    #[must_use]
    pub fn pending_records(&self) -> usize {
        self.pending.as_ref().map_or(0, |batch| batch.records.len())
    }

    /// Reads or retries one immutable batch and advances only after successful delivery.
    ///
    /// # Errors
    /// Stops on invalid data, missing history, changed source/epoch, denied/failed SQL,
    /// or delivery failure. A failed export keeps the same bounded pending batch.
    pub async fn export_next(&mut self) -> Result<NativeCollectionReport, Error> {
        if self.pending.is_none() {
            if self.session.source().await? != self.source {
                return Err(Error::NativeSourceChanged);
            }
            let sequence = i64::try_from(self.after_sequence)
                .map_err(|_error| Error::Invalid("native cursor range"))?;
            let limit = i32::try_from(MAX_BATCH_RECORDS)
                .map_err(|_error| Error::Invalid("native batch limit"))?;
            let json: String = self
                .session
                .query_one(READ_SQL, &[&self.source.epoch, &sequence, &limit])
                .await?
                .try_get(0)
                .map_err(|error| sql_error(&error))?;
            self.pending = Some(PendingBatch::decode(
                json.as_bytes(),
                &self.source,
                self.after_sequence,
            )?);
        }
        let batch = self
            .pending
            .as_ref()
            .ok_or(Error::Invalid("native pending batch"))?;
        if !batch.records.is_empty() {
            self.sink.export(&batch.records).await?;
        }
        let batch = self
            .pending
            .take()
            .ok_or(Error::Invalid("native pending batch"))?;
        self.after_sequence = batch.next_sequence;
        Ok(NativeCollectionReport {
            schema_version: SCHEMA_VERSION,
            origin: "native_collection",
            producer_instance_id: self.source.producer_instance_id.clone(),
            epoch: self.source.epoch.clone(),
            database_oid: self.source.database_oid,
            after_sequence: self.after_sequence,
            exported_records: batch.records.len(),
            loss: batch.loss,
        })
    }

    async fn open(
        uri: &str,
        endpoint: &str,
        namespace: String,
        cluster: String,
    ) -> Result<(Self, SourceSnapshot), Error> {
        identity::validate_label(&namespace)?;
        identity::validate_label(&cluster)?;
        let session = Session::connect(uri).await?;
        let available: bool = session
            .query_one(DISCOVER_SQL, &[])
            .await?
            .try_get(0)
            .map_err(|error| sql_error(&error))?;
        if !available {
            return Err(Error::NativeUnavailable);
        }
        let snapshot = session.snapshot().await?;
        let scope = ExportScope::new(
            namespace,
            cluster,
            snapshot.source.producer_instance_id.clone(),
        )?;
        Ok((
            Self {
                session,
                source: snapshot.source.clone(),
                sink: OtlpSink::new(endpoint, scope)?,
                after_sequence: 0,
                pending: None,
            },
            snapshot,
        ))
    }
}

const fn validate_range(oldest: u64, latest: u64) -> Result<(), Error> {
    if latest > i64::MAX.unsigned_abs()
        || (latest == 0 && oldest != 0)
        || (latest > 0 && (oldest == 0 || oldest > latest))
    {
        return Err(Error::Invalid("native sequence range"));
    }
    Ok(())
}

fn sql_error(error: &tokio_postgres::Error) -> Error {
    if error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) {
        Error::PermissionDenied
    } else {
        Error::NativeFailed
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use serde_json::{json, Value};

    use super::{Capabilities, NativeCheckpoint, PendingBatch, Source, SourceSnapshot};
    use crate::{Error, MAX_BATCH_BYTES};

    type TestResult = Result<(), Box<dyn StdError>>;

    fn source() -> Source {
        Source {
            database_oid: 123,
            producer_instance_id: "0123456789abcdef0123456789abcdef".to_owned(),
            epoch: "fedcba9876543210fedcba9876543210".to_owned(),
        }
    }

    #[test]
    fn recovery_preserves_acknowledged_progress_and_reports_lost_history() -> TestResult {
        let snapshot = SourceSnapshot {
            source: source(),
            oldest_sequence: 10,
            latest_sequence: 20,
        };
        let initial = snapshot.resume(None)?;
        assert_eq!(initial.checkpoint.after_sequence, 9);
        assert!(initial.history_gap && initial.replay_possible);
        assert!(!initial.source_changed);

        let mut previous = NativeCheckpoint {
            source: source(),
            after_sequence: 15,
        };
        let resumed = snapshot.resume(Some(&previous))?;
        assert_eq!(resumed.checkpoint.after_sequence, 15);
        assert!(!resumed.history_gap);
        assert!(!resumed.replay_possible);
        assert!(!resumed.source_changed);

        previous.after_sequence = 5;
        let gap = snapshot.resume(Some(&previous))?;
        assert_eq!(gap.checkpoint.after_sequence, 9);
        assert!(gap.history_gap);
        assert!(!gap.replay_possible);

        previous.after_sequence = 21;
        assert_eq!(
            snapshot.resume(Some(&previous)).err(),
            Some(Error::Invalid(
                "native resume cursor exceeds source history"
            ))
        );
        previous.source.epoch = "11111111111111111111111111111111".to_owned();
        let changed = snapshot.resume(Some(&previous))?;
        assert_eq!(changed.checkpoint.after_sequence, 9);
        assert!(changed.source_changed && changed.history_gap);
        Ok(())
    }

    #[test]
    fn recovery_binds_an_empty_source_without_inventing_loss() -> TestResult {
        let report = SourceSnapshot {
            source: source(),
            oldest_sequence: 0,
            latest_sequence: 0,
        }
        .resume(None)?;
        assert_eq!(report.checkpoint.after_sequence, 0);
        assert!(!report.history_gap);
        assert!(report.replay_possible);
        Ok(())
    }

    fn batch() -> Result<Value, serde_json::Error> {
        let record: Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/native-find-v1.json"))?;
        Ok(json!({
            "schema_version": 1, "database_oid": 123,
            "producer_instance_id": source().producer_instance_id,
            "epoch": source().epoch, "oldest_sequence": 1, "latest_sequence": 1,
            "epoch_reset": false, "gap": false,
            "next_cursor": {"epoch": source().epoch, "after_sequence": 1},
            "loss": {"admission": 0, "contention": 0, "overwrite": 0,
                     "oversize": 0, "clock": 0, "identity": 0},
            "records": [record]
        }))
    }

    #[test]
    fn native_batch_preserves_records_and_valid_empty_progress() -> TestResult {
        let mut batch = batch()?;
        let decoded = PendingBatch::decode(&serde_json::to_vec(&batch)?, &source(), 0)?;
        assert_eq!(decoded.next_sequence, 1);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].sequence(), 1);
        batch["records"] = json!([]);
        let decoded = PendingBatch::decode(&serde_json::to_vec(&batch)?, &source(), 1)?;
        assert!(decoded.records.is_empty());
        assert_eq!(decoded.next_sequence, 1);
        batch["latest_sequence"] = json!(2);
        batch["next_cursor"]["after_sequence"] = json!(2);
        assert_eq!(
            PendingBatch::decode(&serde_json::to_vec(&batch)?, &source(), 1)?.next_sequence,
            2
        );
        Ok(())
    }

    #[test]
    fn native_batch_rejects_changed_lost_or_inconsistent_history() -> TestResult {
        for (field, value, expected) in [
            ("database_oid", json!(456), Error::NativeSourceChanged),
            ("epoch_reset", json!(true), Error::NativeSourceChanged),
            ("gap", json!(true), Error::NativeGap),
            ("schema_version", json!(2), Error::NativeUnavailable),
            (
                "unexpected",
                json!("private"),
                Error::Invalid("native SQL batch schema"),
            ),
            (
                "latest_sequence",
                json!(u64::MAX),
                Error::Invalid("native sequence range"),
            ),
        ] {
            let mut batch = batch()?;
            batch[field] = value;
            assert_eq!(
                PendingBatch::decode(&serde_json::to_vec(&batch)?, &source(), 0).err(),
                Some(expected)
            );
        }
        let mut batch = batch()?;
        batch["next_cursor"]["after_sequence"] = json!(0);
        assert_eq!(
            PendingBatch::decode(&serde_json::to_vec(&batch)?, &source(), 0).err(),
            Some(Error::Invalid("native SQL cursor"))
        );
        assert_eq!(
            PendingBatch::decode(&vec![b' '; MAX_BATCH_BYTES + 1], &source(), 0).err(),
            Some(Error::Invalid("native SQL batch byte limit"))
        );
        Ok(())
    }

    #[test]
    fn capabilities_require_actual_recording_and_protocol_support() -> TestResult {
        let mut value = batch()?;
        for field in ["epoch_reset", "gap", "next_cursor", "records"] {
            value.as_object_mut().ok_or("missing object")?.remove(field);
        }
        for (field, property) in [
            ("recording_enabled", json!(true)),
            ("context_enabled", json!(true)),
            ("storage_allocated", json!(true)),
            ("capacity", json!(2_000)),
            ("max_record_bytes", json!(2_048)),
            ("max_batch_records", json!(128)),
            ("max_batch_bytes", json!(262_144)),
            ("admission_per_second", json!(64)),
        ] {
            value[field] = property;
        }
        let ready = Source::try_from(serde_json::from_value::<Capabilities>(value.clone())?)?;
        assert_eq!(ready.epoch, source().epoch);
        value["recording_enabled"] = json!(false);
        assert_eq!(
            Source::try_from(serde_json::from_value::<Capabilities>(value)?).err(),
            Some(Error::NativeUnavailable)
        );
        Ok(())
    }
}
