package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// listsTail is how a Report without nodes and pods ends. Nodes and pods are its last fields, so
// WriteJSON can write everything before them in one piece and the lists item by item.
const listsTail = `"nodes":null,"pods":null}`

// WriteJSON writes r as JSON followed by a newline, byte for byte as json.Encoder would, one node or
// pod at a time, so a report at the caps is never held in memory encoded whole.
func WriteJSON(w io.Writer, r Report) error {
	nodes, pods := r.Nodes, r.Pods
	r.Nodes, r.Pods = nil, nil
	head, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if !bytes.HasSuffix(head, []byte(listsTail)) {
		return errors.New("v1.Report no longer ends with its nodes and pods; WriteJSON must change with it")
	}
	if _, err := w.Write(head[:len(head)-len(listsTail)]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, `"nodes":`); err != nil {
		return err
	}
	if err := writeArray(w, nodes); err != nil {
		return err
	}
	if _, err := io.WriteString(w, `,"pods":`); err != nil {
		return err
	}
	if err := writeArray(w, pods); err != nil {
		return err
	}
	_, err = io.WriteString(w, "}\n")
	return err
}

func writeArray[T any](w io.Writer, items []T) error {
	if items == nil {
		_, err := io.WriteString(w, "null")
		return err
	}
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i := range items {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		data, err := json.Marshal(items[i])
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}
