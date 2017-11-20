package updater

import (
	"testing"
	"time"
)

func TestGenerateTimestamp(t *testing.T) {
	table := [][]interface{}{
		{"91902", "11122017", time.Date(2017, time.November, 12, 9, 19, 2, 0, time.UTC)},
		{"220120", "01012020", time.Date(2020, time.January, 1, 22, 1, 20, 0, time.UTC)},
	}

	for _, testCase := range table {
		timeString := testCase[0].(string)
		dateString := testCase[1].(string)
		timestamp, err := generateTimestamp(timeString, dateString)
		if err != nil {
			t.Errorf("Got error %v, expected none.", err)
		}
		expected := testCase[2].(time.Time)
		if !timestamp.Equal(expected) {
			t.Errorf("Got %v, expected %v.", timestamp, expected)
		}
	}
}
