// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// externalConnectionKafkaSink is a wrapper sink that asserts the underlying
// resource it is Dial()ing is a kafka sink. This is used to test that External
// Connections route to the correct sink.
type externalConnectionKafkaSink struct {
	sink Sink
}

func (e *externalConnectionKafkaSink) getConcreteType() sinkType {
	return sinkTypeKafka
}

// Dial implements the Sink interface.
func (e *externalConnectionKafkaSink) Dial() error {
	if _, ok := e.sink.(*kafkaSink); !ok {
		return errors.Newf("unexpected sink type %T; expected a kafka sink", e.sink)
	}
	return nil
}

// Close implements the Sink interface.
func (e *externalConnectionKafkaSink) Close() error {
	return nil
}

// EmitRow implements the Sink interface.
func (e *externalConnectionKafkaSink) EmitRow(
	_ context.Context, _ TopicDescriptor, _, _ []byte, _, _ hlc.Timestamp, _ kvevent.Alloc,
) error {
	return nil
}

// Flush implements the Sink interface.
func (e *externalConnectionKafkaSink) Flush(_ context.Context) error {
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (e *externalConnectionKafkaSink) EmitResolvedTimestamp(
	_ context.Context, _ Encoder, _ hlc.Timestamp,
) error {
	return nil
}

var _ Sink = (*externalConnectionKafkaSink)(nil)

func TestChangefeedExternalConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	knobs := s.TestingKnobs.
		DistSQL.(*execinfra.TestingKnobs).
		Changefeed.(*TestingKnobs)
	knobs.WrapSink = func(s Sink, _ jobspb.JobID) Sink {
		// External Connections recursively invokes `getSink` for the underlying
		// resource. We want to prevent double wrapping the sink since we assert on
		// the underlying Sink type in `Dial`.
		if _, ok := s.(*externalConnectionKafkaSink); ok {
			return s
		}
		return &externalConnectionKafkaSink{sink: s}
	}

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()
	unknownParams := func(sink string, params ...string) string {
		return fmt.Sprintf(`unknown %s sink query parameters: [%s]`, sink, strings.Join(params, ", "))
	}

	for _, tc := range []struct {
		name          string
		uri           string
		expectedError string
	}{
		{
			// kafka_topic_prefix was referenced by an old version of the RFC, it's
			// "topic_prefix" now.
			name:          "kafka_topic_prefix",
			uri:           "kafka://nope/?kafka_topic_prefix=foo",
			expectedError: unknownParams(`kafka`, `kafka_topic_prefix`),
		},
		{
			// schema_topic will be implemented but isn't yet.
			name:          "schema_topic is not yet supported",
			uri:           "kafka://nope/?schema_topic=foo",
			expectedError: "schema_topic is not yet supported",
		},
		// Sanity check kafka tls parameters.
		{
			name:          "param tls_enabled must be a bool",
			uri:           "kafka://nope/?tls_enabled=foo",
			expectedError: "param tls_enabled must be a bool",
		},
		{
			name:          "param insecure_tls_skip_verify must be a bool",
			uri:           "kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=foo",
			expectedError: "param insecure_tls_skip_verify must be a bool",
		},
		{
			name:          "param ca_cert must be base 64 encoded",
			uri:           "kafka://nope/?ca_cert=!",
			expectedError: "param ca_cert must be base 64 encoded",
		},
		{
			name:          "ca_cert requires tls_enabled=true",
			uri:           "kafka://nope/?&ca_cert=Zm9v",
			expectedError: "ca_cert requires tls_enabled=true",
		},
		{
			name:          "param client_cert must be base 64 encoded",
			uri:           "kafka://nope/?client_cert=!",
			expectedError: "param client_cert must be base 64 encoded",
		},
		{
			name:          "param client_key must be base 64 encoded",
			uri:           "kafka://nope/?client_key=!",
			expectedError: "param client_key must be base 64 encoded",
		},
		{
			name:          "client_cert requires tls_enabled=true",
			uri:           "kafka://nope/?client_cert=Zm9v",
			expectedError: "client_cert requires tls_enabled=true",
		},
		{
			name:          "client_cert requires client_key to be set",
			uri:           "kafka://nope/?tls_enabled=true&client_cert=Zm9v",
			expectedError: "client_cert requires client_key to be set",
		},
		{
			name:          "client_key requires client_cert to be set",
			uri:           "kafka://nope/?tls_enabled=true&client_key=Zm9v",
			expectedError: "client_key requires client_cert to be set",
		},
		{
			name:          "invalid client certificate",
			uri:           "kafka://nope/?tls_enabled=true&client_cert=Zm9v&client_key=Zm9v",
			expectedError: "invalid client certificate",
		},
		// Sanity check kafka sasl parameters.
		{
			name:          "param sasl_enabled must be a bool",
			uri:           "kafka://nope/?sasl_enabled=maybe",
			expectedError: "param sasl_enabled must be a bool",
		},
		{
			name:          "param sasl_handshake must be a bool",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_handshake=maybe",
			expectedError: "param sasl_handshake must be a bool",
		},
		{
			name:          "sasl_enabled must be enabled to configure SASL handshake behavior",
			uri:           "kafka://nope/?sasl_handshake=false",
			expectedError: "sasl_enabled must be enabled to configure SASL handshake behavior",
		},
		{
			name:          "sasl_user must be provided when SASL is enabled",
			uri:           "kafka://nope/?sasl_enabled=true",
			expectedError: "sasl_user must be provided when SASL is enabled",
		},
		{
			name:          "sasl_password must be provided when SASL is enabled",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_user=a",
			expectedError: "sasl_password must be provided when SASL is enabled",
		},
		{
			name:          "sasl_enabled must be enabled if sasl_user is provided",
			uri:           "kafka://nope/?sasl_user=a",
			expectedError: "sasl_enabled must be enabled if sasl_user is provided",
		},
		{
			name:          "sasl_enabled must be enabled if sasl_password is provided",
			uri:           "kafka://nope/?sasl_password=a",
			expectedError: "sasl_enabled must be enabled if sasl_password is provided",
		},
		{
			name:          "sasl_enabled must be enabled to configure SASL mechanism",
			uri:           "kafka://nope/?sasl_mechanism=SCRAM-SHA-256",
			expectedError: "sasl_enabled must be enabled to configure SASL mechanism",
		},
		{
			name:          "param sasl_mechanism must be one of SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, or PLAIN",
			uri:           "kafka://nope/?sasl_enabled=true&sasl_mechanism=unsuppported",
			expectedError: "param sasl_mechanism must be one of SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, or PLAIN",
		},
		// confluent-cloud scheme tests
		{
			name:          "requires parameter api_key",
			uri:           "confluent-cloud://nope/",
			expectedError: "requires parameter api_key",
		},
		{
			name:          "requires parameter api_secret",
			uri:           "confluent-cloud://nope?api_key=fee",
			expectedError: "requires parameter api_secret",
		},
		{
			name:          "requires sasl_enabled=true",
			uri:           "confluent-cloud://nope?api_key=fee&api_secret=bar&sasl_enabled=false",
			expectedError: "unsupported value false for parameter sasl_enabled, please use true",
		},
		{
			name:          "requires parameter sasl_mechanism=PLAIN",
			uri:           "confluent-cloud://nope?api_key=fee&api_secret=bar&sasl_mechanism=OAUTHBEARER",
			expectedError: "unsupported value OAUTHBEARER for parameter sasl_mechanism, please use PLAIN",
		},
		{
			name:          "requires parameter sasl_handshake=true",
			uri:           "confluent-cloud://nope?api_key=fee&api_secret=bar&sasl_handshake=false",
			expectedError: "unsupported value false for parameter sasl_handshake, please use true",
		},
		{
			name:          "requires parameter tls_enabled=true",
			uri:           "confluent-cloud://nope?api_key=fee&api_secret=bar&tls_enabled=false",
			expectedError: "unsupported value false for parameter tls_enabled, please use true",
		},
		{
			name:          "invalid query parameters",
			uri:           "confluent-cloud://nope?api_key=fee&api_secret=bar&ca_cert=abcd",
			expectedError: "invalid query parameters",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?",
			expectedError: "requires parameter SharedAccessKeyName",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc",
			expectedError: "requires parameter SharedAccessKey",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&sasl_enabled=false",
			expectedError: "unsupported value false for parameter sasl_enabled, please use true",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&sasl_mechanism=OAUTHBEARER",
			expectedError: "unsupported value OAUTHBEARER for parameter sasl_mechanism, please use PLAIN",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&sasl_handshake=false",
			expectedError: "unsupported value false for parameter sasl_handshake, please use true",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&tls_enabled=false",
			expectedError: "unsupported value false for parameter tls_enabled, please use true",
		},
		{
			name:          "test",
			uri:           "azure-event-hub://nope?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&ca_cert=abcd",
			expectedError: "invalid query parameters",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, tc.expectedError,
				fmt.Sprintf(`CREATE EXTERNAL CONNECTION '%s' AS '%s'`, tc.name, tc.uri),
			)
		})
	}

	// We wrap the changefeed Sink with `externalConnectionKafkaSink` that asserts
	// the underlying Sink is a kafka sink.
	t.Run("changefeed-with-well-formed-uri", func(t *testing.T) {
		// kafka scheme external connections
		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION nope AS 'kafka://nope'`)
		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION "nope-with-params" AS 'kafka://nope/?tls_enabled=true&insecure_tls_skip_verify=true&topic_name=foo'`)
		// confluent-cloud external connections
		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION confluent1 AS 'confluent-cloud://nope?api_key=fee&api_secret=bar'`)
		sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION confluent2 AS 'confluent-cloud://nope?api_key=fee&api_secret=bar&`+
			`sasl_mechanism=PLAIN&tls_enabled=true&topic_prefix=foo&sasl_enabled=true&sasl_handshake=true&`+
			`insecure_tls_skip_verify=true'`)

		sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO 'external://nope'`)
		sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO 'external://nope-with-params'`)
		sqlDB.Exec(
			t, `CREATE CHANGEFEED FOR foo INTO 'external://nope/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
		)
		sqlDB.ExpectErr(
			t, `this sink is incompatible with option webhook_client_timeout`,
			`CREATE CHANGEFEED FOR foo INTO 'external://nope/' WITH webhook_client_timeout='1s'`,
		)
		sqlDB.Exec(
			t, `CREATE CHANGEFEED FOR foo INTO 'external://confluent1/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
		)
		sqlDB.Exec(
			t, `CREATE CHANGEFEED FOR foo INTO 'external://confluent2/' WITH kafka_sink_config='{"Flush": {"Messages": 100, "Frequency": "1s"}}'`,
		)
	})
}

func TestBuildAzureKafkaConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name   string
		oldUri string
		newUri string
	}{
		{
			name:   "test",
			oldUri: "kafka://artemeventhubs.servicebus.windows.net:9093?tls_enabled=true&sasl_enabled=true&sasl_user=$ConnectionString&sasl_password=Endpoint%3Dsb%3A%2F%2Fartemeventhubs.servicebus.windows.net%2F%3BSharedAccessKeyName%3Dsaspolicytpcc%3BSharedAccessKey%3D123&sasl_mechanism=PLAIN",
			newUri: "azure-event-hub://artemeventhubs.servicebus.windows.net:9093?SharedAccessKeyName=saspolicytpcc&SharedAccessKey=123&sasl_mechanism=PLAIN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldU, err := url.Parse(tc.oldUri)
			require.NoError(t, err)
			newU, err := url.Parse(tc.newUri)
			require.NoError(t, err)
			expectedConfig, expectedError := buildDialConfig(sinkURL{URL: oldU})
			actualConfig, actualError := buildDialConfig(sinkURL{URL: newU})
			require.Equal(t, expectedConfig, actualConfig)
			require.Equal(t, expectedError, actualError)
		},
		)
	}
}
