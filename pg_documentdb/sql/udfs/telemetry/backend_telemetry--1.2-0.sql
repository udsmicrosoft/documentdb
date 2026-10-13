-- Copyright (c) Microsoft Corporation. All rights reserved.
-- SPDX-License-Identifier: MIT

CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.backend_telemetry_capabilities()
RETURNS jsonb
LANGUAGE C VOLATILE PARALLEL UNSAFE SECURITY INVOKER
AS 'MODULE_PATHNAME', $$backend_telemetry_capabilities$$;

CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.backend_telemetry_read(
	expected_epoch uuid,
	after_sequence bigint,
	max_records integer DEFAULT 128)
RETURNS jsonb
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE SECURITY INVOKER
AS 'MODULE_PATHNAME', $$backend_telemetry_read$$;

REVOKE ALL ON FUNCTION __API_SCHEMA_INTERNAL_V2__.backend_telemetry_capabilities() FROM PUBLIC;
REVOKE ALL ON FUNCTION __API_SCHEMA_INTERNAL_V2__.backend_telemetry_read(uuid, bigint, integer) FROM PUBLIC;
