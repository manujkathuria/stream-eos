module stream-eos/ingestor

go 1.27.1

require (
	github.com/gorilla/websocket v1.5.3
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	stream-eos/pkg v0.0.0
	stream-eos/websocket-source v0.0.0
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
)

replace stream-eos/pkg => ../pkg

replace stream-eos/websocket-source => ../websocket-source
