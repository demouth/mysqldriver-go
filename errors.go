package mysqldriver

import "fmt"

type MySQLError struct {
	Number   uint16
	SQLState [5]byte
	Message  string
}

func (me *MySQLError) Error() string {
	if me.SQLState != [5]byte{} {
		return fmt.Sprintf("Error %d (%s): %s", me.Number, me.SQLState, me.Message)
	}
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}
