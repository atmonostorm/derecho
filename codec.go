package derecho

import "encoding/json"

type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

var DefaultCodec Codec = jsonCodec{}
