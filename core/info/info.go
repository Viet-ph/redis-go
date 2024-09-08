package info

import "github.com/google/uuid"


var (
	Master string = ""
	Role   string = "master"

	MasterHost string = ""
	MasterPort int    = 0

	ReplicationId     uuid.UUID
	ReplicationOffset int
)
