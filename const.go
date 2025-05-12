package mysqldriver

import "runtime"

const (
	maxPacketSize = 1<<24 - 1 // 16,777,215

	defaultMaxAllowedPacket = 64 << 20 // 64 MiB. See https://github.com/go-sql-driver/mysql/issues/1355

	// Connection attributes
	// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-connection-attribute-tables.html#performance-schema-connection-attributes-available
	connAttrClientName      = "_client_name"
	connAttrClientNameValue = "SQLDB-Go-Driver"
	connAttrOS              = "_os"
	connAttrOSValue         = runtime.GOOS
	connAttrPlatform        = "_platform"
	connAttrPlatformValue   = runtime.GOARCH
	connAttrPid             = "_pid"
	connAttrServerHost      = "_server_host"
)
const (
	iOK           byte = 0x00
	iAuthMoreData byte = 0x01
	iEOF          byte = 0xfe
	iERR          byte = 0xff
)

// https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
type clientFlag uint32

const (
	clientLongPassword clientFlag = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDB
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConn
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenEncClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	clientDeprecateEOF
)

const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

type statusFlag uint16

const (
	statusInTrans statusFlag = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)

const (
	cachingSha2PasswordRequestPublicKey          = 2
	cachingSha2PasswordFastAuthSuccess           = 3
	cachingSha2PasswordPerformFullAuthentication = 4
)

type fieldType byte

type fieldFlag uint16
