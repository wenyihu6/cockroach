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
	"fmt"
	"github.com/cockroachdb/errors"
	"strings"
)

// Helper functions for metamorphic testing stuff. Since we do not have an
// option for correctness testing, we run two tests manually with different options and compare

// map allTpccTargets to another map

func parseTableNameFromFile(fileName string) (string, error) {
	// parts[len(parts)-1]
	// /2023-11-07/202311071946288411402400000000000-c1a4f08eaf3f6ecd-1-5-000000b7-stock-7.parquet
	fileWithoutFormatSuffix := strings.Split(fileName, ".")
	if len(fileWithoutFormatSuffix) <= 1 {
		return "", errors.New("sds")
	}
	parts := strings.Split(fileWithoutFormatSuffix[0], "-")
	// strings.Split("example.txt", ".")
	if len(parts) <= 2 {
		return "", errors.New("sds")
	}
	return parts[len(parts)-2], nil
}

func upsertStmtForTable(tableName string, rows int, cols int) (stmt string) {
	// check my understanding: rows == len(datums) and cols == len(datums[0]) -> looks right
	if rows <= 0 {
		return
	}
	// UPSERT every row: `UPSERT INTO foo (a, b) VALUES (1, $1)`, counter
	stmt = fmt.Sprintf("UPSERT INTO %s VALUES (", tableName)
	for j := 0; j < cols; j++ {
		stmt += fmt.Sprintf(", $%v", j+1)
	}
	stmt += ")"
	return
}

// meta parquet.ReadDatumsMetadata
// cols := meta.NumCols
//	rows := meta.NumRows

// create duplicated tables for each tpcctarget
func allTpccTargetsStmt() (stmts []string) {
	for _, targetTable := range allTpccTargets {
		// CREATE TABLE table2 (LIKE table1 INCLUDING ALL EXCLUDING CONSTRAINTS, c INT, INDEX(b,c));
		// tpcc.warehouse_1, tpcc.warehouse_1
		// CREATE TABLE tpcc.warehouse_1
		tableName1 := targetTable + "_1"
		tableName2 := targetTable + "_2"
		stmts = append(stmts, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", tableName1, targetTable))
		stmts = append(stmts, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", tableName2, targetTable))
	}
	return
}

func checkAllTables(checker func(t1 string, t2 string)) {
	for _, targetTable := range allTpccTargets {
		checker(targetTable+"_1", targetTable+"_2")
		fmt.Println("PASSED")
	}
	fmt.Println("PASSED")
}
