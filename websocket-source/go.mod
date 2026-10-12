module stream-eos/websocket-source

go 1.27.1

require (
	github.com/gorilla/websocket v1.5.3
	stream-eos/pkg v0.0.0
)

replace stream-eos/pkg => ../pkg
