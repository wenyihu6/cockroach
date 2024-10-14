// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Code generated by "stringer"; DO NOT EDIT.

package pgwirebase

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ServerMsgAuth-82]
	_ = x[ServerMsgBackendKeyData-75]
	_ = x[ServerMsgBindComplete-50]
	_ = x[ServerMsgCommandComplete-67]
	_ = x[ServerMsgCloseComplete-51]
	_ = x[ServerMsgCopyInResponse-71]
	_ = x[ServerMsgCopyOutResponse-72]
	_ = x[ServerMsgCopyDataCommand-100]
	_ = x[ServerMsgCopyDoneCommand-99]
	_ = x[ServerMsgDataRow-68]
	_ = x[ServerMsgEmptyQuery-73]
	_ = x[ServerMsgErrorResponse-69]
	_ = x[ServerMsgNoticeResponse-78]
	_ = x[ServerMsgNoData-110]
	_ = x[ServerMsgParameterDescription-116]
	_ = x[ServerMsgParameterStatus-83]
	_ = x[ServerMsgParseComplete-49]
	_ = x[ServerMsgPortalSuspended-115]
	_ = x[ServerMsgReady-90]
	_ = x[ServerMsgRowDescription-84]
}

func (i ServerMessageType) String() string {
	switch i {
	case ServerMsgAuth:
		return "ServerMsgAuth"
	case ServerMsgBackendKeyData:
		return "ServerMsgBackendKeyData"
	case ServerMsgBindComplete:
		return "ServerMsgBindComplete"
	case ServerMsgCommandComplete:
		return "ServerMsgCommandComplete"
	case ServerMsgCloseComplete:
		return "ServerMsgCloseComplete"
	case ServerMsgCopyInResponse:
		return "ServerMsgCopyInResponse"
	case ServerMsgCopyOutResponse:
		return "ServerMsgCopyOutResponse"
	case ServerMsgCopyDataCommand:
		return "ServerMsgCopyDataCommand"
	case ServerMsgCopyDoneCommand:
		return "ServerMsgCopyDoneCommand"
	case ServerMsgDataRow:
		return "ServerMsgDataRow"
	case ServerMsgEmptyQuery:
		return "ServerMsgEmptyQuery"
	case ServerMsgErrorResponse:
		return "ServerMsgErrorResponse"
	case ServerMsgNoticeResponse:
		return "ServerMsgNoticeResponse"
	case ServerMsgNoData:
		return "ServerMsgNoData"
	case ServerMsgParameterDescription:
		return "ServerMsgParameterDescription"
	case ServerMsgParameterStatus:
		return "ServerMsgParameterStatus"
	case ServerMsgParseComplete:
		return "ServerMsgParseComplete"
	case ServerMsgPortalSuspended:
		return "ServerMsgPortalSuspended"
	case ServerMsgReady:
		return "ServerMsgReady"
	case ServerMsgRowDescription:
		return "ServerMsgRowDescription"
	default:
		return "ServerMessageType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
