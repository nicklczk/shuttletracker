package model

// User represents a user.
type User struct {
	ID   int
	Name string `db:"rcs_id"`
}
