package v1

import (
	"bytes"
	"encoding/json"
	"testing"
)

// largestWrite records the biggest single write, which is what a streaming encoder holds at once.
type largestWrite struct {
	bytes.Buffer
	largest int
}

func (w *largestWrite) Write(p []byte) (int, error) {
	w.largest = max(w.largest, len(p))
	return w.Buffer.Write(p)
}

func TestWriteJSON_MatchesTheEncoderAndStreams(t *testing.T) {
	for _, tc := range []struct {
		name   string
		report Report
	}{
		{"golden", goldenReport()},
		{"at the caps", atLimitsReport()},
		{"no nodes or pods", Report{SchemaVersion: SchemaVersion, ClusterID: "c"}},
		{"empty nodes and pods", Report{SchemaVersion: SchemaVersion, ClusterID: "c", Nodes: []Node{}, Pods: []Pod{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want bytes.Buffer
			if err := json.NewEncoder(&want).Encode(tc.report); err != nil {
				t.Fatal(err)
			}
			got := &largestWrite{}
			if err := WriteJSON(got, tc.report); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want.Bytes()) {
				t.Fatalf("WriteJSON differs from json.Encoder:\n got %.300s\nwant %.300s", got.Bytes(), want.Bytes())
			}
			if got.largest > 1<<20 {
				t.Fatalf("largest single write = %d bytes of %d; the report must stream item by item", got.largest, got.Len())
			}
		})
	}
}

func TestWriteJSON_ReportsAWriteFailure(t *testing.T) {
	if err := WriteJSON(failingWriter{}, goldenReport()); err == nil {
		t.Fatal("WriteJSON swallowed the writer's error")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, bytes.ErrTooLarge }

// failsAtWrite fails its nth write, so each write WriteJSON makes is shown to report its error.
type failsAtWrite struct {
	n, writes int
}

func (w *failsAtWrite) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.n {
		return 0, bytes.ErrTooLarge
	}
	return len(p), nil
}

func TestWriteJSON_ReportsAFailureAtEveryWrite(t *testing.T) {
	report := goldenReport()
	counter := &failsAtWrite{}
	if err := WriteJSON(counter, report); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= counter.writes; n++ {
		if err := WriteJSON(&failsAtWrite{n: n}, report); err == nil {
			t.Fatalf("a failure at write %d of %d was swallowed", n, counter.writes)
		}
	}
}
