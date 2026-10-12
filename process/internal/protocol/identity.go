package protocol

import shared "stream-eos/pkg/protocol"

// EventID is shared with ingestion; FIFO recovery metadata remains engine-local.
type EventID = shared.EventID
