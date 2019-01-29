package types

import "io"

type WrappedEvent struct{
	Value io.Reader
	Topic string
	Metadata map[string]string
	Ack func() error
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