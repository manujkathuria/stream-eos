module stream-eos/process

go 1.27.1

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
)

replace stream-eos/pkg => ../pkg

require (
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	stream-eos/pkg v0.0.0
)
