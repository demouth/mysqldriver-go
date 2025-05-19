package mysqldriver

type mysqlResult struct {
	affectedRows []int64
	insertIds    []int64
}

func (res *mysqlResult) LastInsertId() (int64, error) {
	return res.insertIds[len(res.insertIds)-1], nil
}

func (res *mysqlResult) RowsAffected() (int64, error) {
	return res.affectedRows[len(res.affectedRows)-1], nil
}
