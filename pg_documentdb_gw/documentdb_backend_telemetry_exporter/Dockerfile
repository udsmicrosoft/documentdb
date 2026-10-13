# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT

FROM debian:trixie-slim
ARG EXPORTER_BINARY=target/release/documentdb_backend_telemetry_exporter
COPY ${EXPORTER_BINARY} /usr/local/bin/documentdb_backend_telemetry_exporter
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/documentdb_backend_telemetry_exporter"]
