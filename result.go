package mysqldriver

type mysqlResult struct {
	affectedRows []int64
	insertIds    []int64
}
