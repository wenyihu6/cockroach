// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func createTargetTableStmt(
	newTableName string, targetTableName string,
) (createStmt string, dropStmt string) {
	// BETTER HERE For each tpcc target table, create two tables with the same
	// schemas. For example, create two tables stock_1, stock_2 for tpcc.stock
	// to compare fingerprints.
	tpccTableName := fmt.Sprintf("tpcc.%s", targetTableName)
	createStmt = fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)",
		newTableName, tpccTableName)
	// Return delete stmts for cleanup.
	dropStmt = fmt.Sprintf("DROP TABLE %s", newTableName)
	return createStmt, dropStmt
}

func parseTableNameFromFileName(fileName string) (string, error) {
	// For example, given the filename
	// /2023-11-07/202311071946288411402400000000000-c1a4f08eaf3f6ecd-1-5-000000b7-stock-7.parquet,
	// split the string and return target table stock.
	splittedString := strings.Split(fileName, ".")
	if len(splittedString) <= 1 {
		return "", errors.New("BETTER MSG")
	}
	// Split the first part of filename by -.
	parts := strings.Split(splittedString[0], "-")
	if len(parts) <= 2 {
		return "", errors.New("BETTER MSG")
	}
	return parts[len(parts)-2], nil
}

func upsertStmtForTable(tableName string, args []string) string {
	// returns "UPSERT INTO tableName VALUES (args)"
	b := strings.Builder{}
	b.WriteString("UPSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" VALUES (")
	for i, arg := range args {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(arg)
	}
	b.WriteByte(')')
	return b.String()
}

func getRandomIndex(sizeOfSlice int) int {
	return rand.Intn(sizeOfSlice)
}

func fetcnFilesOfTargetTable(
	selectedTargetTable string,
	firstCloudStorage cloud.ExternalStorage,
	secCloudStorage cloud.ExternalStorage,
) (firstCloudStorageFileNames []string, secCloudStorageFileNames []string, _ error) {
	err := firstCloudStorage.List(context.Background(), "", "", func(str string) error {
		targetTableName, err := parseTableNameFromFileName(str)
		if err != nil {
			return err
		}
		if targetTableName != selectedTargetTable {
			return nil
		}
		firstCloudStorageFileNames = append(firstCloudStorageFileNames, str)
		return nil
	})
	if err != nil {
		return []string{}, []string{}, err
	}

	err = secCloudStorage.List(context.Background(), "", "", func(str string) error {
		targetTableName, err := parseTableNameFromFileName(str)
		if err != nil {
			return err
		}
		if targetTableName != selectedTargetTable {
			return nil
		}
		secCloudStorageFileNames = append(secCloudStorageFileNames, str)
		return nil
	})
	if err != nil {
		return []string{}, []string{}, err
	}
	return
}

func downloadFileFromCloudStorage(
	ctx context.Context, es cloud.ExternalStorage, fileName string,
) (downloadedFileName string, _ error) {
	reader, _, err := es.ReadFile(context.Background(), fileName, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return "", err
	}
	defer func() {
		err = reader.Close(ctx)
	}()

	f, err := os.CreateTemp(os.TempDir(), "")
	if err != nil {
		return "", err
	}

	bytes, err := ioctx.ReadAll(ctx, reader)
	if err != nil {
		return "", err
	}

	_, err = f.Write(bytes)
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func cleanUpDownloadedFiles(fileNames []string) error {
	for _, fileName := range fileNames {
		if err := os.Remove(fileName); err != nil {
			return err
		}
	}
	return nil
}

func processTable(
	t test.Test, sqlRunner *sqlutils.SQLRunner, targetTable string, fileName string,
) error {
	meta, filesInDatums, err := parquet.ReadFile(fileName)
	if err != nil {
		return err
	}
	eventTypeColIdx, err := changefeedccl.GetEventTypeColIdx(meta)
	if err != nil {
		return err
	}

	for _, rowInDatums := range filesInDatums {
		var argsInDatumString []string
		for i, argInDatum := range rowInDatums {
			if i == eventTypeColIdx {
				// skip the event type column
				if argInDatum.String() != "c" {
					return errors.New("BETTER MSG")
				} else {
					continue
				}
			}
			argsInDatumString = append(argsInDatumString, argInDatum.String())
		}
		sqlRunner.Exec(t, upsertStmtForTable(targetTable, argsInDatumString))
	}
	return nil
}

func processTables(
	t test.Test, sqlRunner *sqlutils.SQLRunner, targetTable string, fileNames []string,
) error {
	for _, fn := range fileNames {
		if err := processTable(t, sqlRunner, targetTable, fn); err != nil {
			return err
		}
	}
	return nil
}

func downloadFiles(
	ctx context.Context, es cloud.ExternalStorage, fileNames []string,
) (downloadedFileNames []string, _ error) {
	for _, fn := range fileNames {
		downloadedFn, err := downloadFileFromCloudStorage(ctx, es, fn)
		if err != nil {
			return []string{}, err
		}
		downloadedFileNames = append(downloadedFileNames, downloadedFn)
	}
	return
}

func checkTwoChangeFeedExportContent(
	ctx context.Context,
	t test.Test,
	sqlRunner *sqlutils.SQLRunner,
	firstSinkURI string,
	secSinkURI string,
) {
	// TODO(wenyihu6): Is it faster if I create one table and do all ops in the table at once
	// A handler to the cloud storage.
	firstCloudStorage, err := cloud.ExternalStorageFromURI(ctx, "gs://cockroach-tmp/roachtest/20231114031405?AUTH=implicit",
		base.ExternalIODirConfig{},
		cluster.MakeTestingClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	secCloudStorage, err := cloud.ExternalStorageFromURI(ctx, "gs://cockroach-tmp/roachtest/20231114031405?AUTH=implicit",
		base.ExternalIODirConfig{},
		cluster.MakeTestingClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	// Randomly select one table and skip all others.
	randomlySelectedIndex := getRandomIndex(len(allTrimmedTpccTargets))
	selectedTargetTable := allTrimmedTpccTargets[randomlySelectedIndex]
	firstTableName := selectedTargetTable + "_1"
	secTableName := selectedTargetTable + "_2"

	firstCreateStmt, firstDropStmt := createTargetTableStmt(firstTableName, selectedTargetTable)
	secCreateStmt, secDropStmt := createTargetTableStmt(secTableName, selectedTargetTable)
	sqlRunner.Exec(t, firstCreateStmt)
	sqlRunner.Exec(t, secCreateStmt)
	defer func() {
		sqlRunner.Exec(t, firstDropStmt)
		sqlRunner.Exec(t, secDropStmt)
	}()

	firstCloudStorageFileNames, secCloudStorageFileNames, err := fetcnFilesOfTargetTable(
		selectedTargetTable, firstCloudStorage, secCloudStorage)
	require.NoError(t, err)

	firstDownloadedFileNames, err := downloadFiles(ctx, firstCloudStorage, firstCloudStorageFileNames)
	require.NoError(t, err)
	secDownloadedFileNames, err := downloadFiles(ctx, secCloudStorage, secCloudStorageFileNames)
	require.NoError(t, err)

	// parse downloaded files and execute UPSERT stmts.
	err = processTables(t, sqlRunner, firstTableName, firstDownloadedFileNames)
	require.NoError(t, err)
	err = processTables(t, sqlRunner, secTableName, secDownloadedFileNames)
	require.NoError(t, err)

	// expectedFingerprints := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank")
	//	actualFingerprints := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data2.bank")
	//	require.Equal(t, expectedFingerprints, actualFingerprints)
	// check fingerprints of two tables
	firstFingerPrint := sqlRunner.QueryStr(t, fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s", firstTableName))
	secFingerPrint := sqlRunner.QueryStr(t, fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s", secTableName))
	require.Equal(t, firstFingerPrint, secFingerPrint)
	// clean up
	err = cleanUpDownloadedFiles(firstDownloadedFileNames)
	require.NoError(t, err)
	err = cleanUpDownloadedFiles(secDownloadedFileNames)
	require.NoError(t, err)
}
