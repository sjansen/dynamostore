package dynamostore_test

import (
	"github.com/alexedwards/scs/v2"

	"github.com/sjansen/dynamostore"
)

var _ scs.Store = dynamostore.New(nil)
