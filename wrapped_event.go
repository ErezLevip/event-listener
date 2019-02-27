package event_listener

import (
	"github.com/apex/log"
	"io"
)

type WrappedEvent struct {
	Value    io.Reader
	Topic    string
	Metadata map[string]string
	Ack      func()
}

func (we *WrappedEvent) PipedRead() (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	go func() {
		_, err := io.Copy(pw, we.Value)
		if err != nil {
			err := pw.CloseWithError(err)
			if err != nil {
				log.Error(err.Error())
			}
			return
		}
	}()
	return pr, nil
}
