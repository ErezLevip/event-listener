package types

import "io"

type Event struct {
	EntityId  string `json:"entity_id"`
	EventType string `json:"event_type"`
	Id        string `json:"id"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

type WrappedEvent struct{
	Ack bool
	Value io.Reader
	Topic string
}

func (we *WrappedEvent) ReadAll() ([]byte,error)  {
	buff := make([]byte, 4)
	total := make([]byte, 0)
	for {
		n, err := we.Value.Read(buff)
		if err != nil {
			if err == io.EOF {
				total = append(total, buff[:n]...)
				break
			}
			return nil, err
		}
		total = append(total, buff[:n]...)
	}
	return total,nil
}