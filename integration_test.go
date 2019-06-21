// +build integration

package dynamostore_test

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"

	"github.com/sjansen/dynamostore"
)

func createClient() *dynamodb.DynamoDB {
	endpoint := os.Getenv("DYNAMOSTORE_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000"
	}

	creds := credentials.NewStaticCredentials("id", "secret", "token")
	sess := session.Must(session.NewSession())
	return dynamodb.New(
		sess,
		aws.NewConfig().
			WithCredentials(creds).
			WithRegion("us-west-2").
			WithEndpoint(endpoint),
	)
}

func randomString() string {
	bytes := make([]byte, 10)
	for i := range bytes {
		bytes[i] = byte(65 + rand.Intn(25))
	}
	return string(bytes)
}

func TestDynamoDBLocal(t *testing.T) {
	require := require.New(t)

	svc := createClient()
	require.NotNil(svc)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := svc.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{})
	require.NoError(err)
}

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	svc := createClient()
	require.NotNil(svc)

	store := dynamostore.New(svc)

	// first time: created
	err := store.CreateTable()
	require.NoError(err)

	// second time: noop
	err = store.CreateTable()
	require.NoError(err)
}

func TestStore(t *testing.T) {
	require := require.New(t)

	svc := createClient()
	require.NotNil(svc)

	store := dynamostore.New(svc)
	require.NotNil(store)

	token := randomString()
	data := []byte(randomString())
	expiry := time.Now().Add(2 * time.Second)

	// given a non-existent session
	// when there is an attempt to delete the session
	err := store.Delete(token)
	// then there shouldn't be an error
	require.NoError(err)

	// given a non-existent session
	// when there is an attempt to read the session
	actual, exists, err := store.Find(token)
	// then there shouldn't be an error
	require.NoError(err)
	// and it should be clear no session exists
	require.Equal(false, exists)
	require.Nil(actual)

	// given a new, unsaved session
	// when there is an attempt to save the session
	err = store.Commit(token, data, expiry)
	// then there shouldn't be an error
	require.NoError(err)
	// and it should be possible to read back the session
	actual, exists, err = store.Find(token)
	require.NoError(err)
	require.Equal(true, exists)
	require.Equal(data, actual)

	// given a previously saved session
	// when enough time has passed for the session to expire
	time.Sleep(3 * time.Second)
	// and there is an attempt to read the session
	actual, exists, err = store.Find(token)
	// then there shouldn't be an error
	require.NoError(err)
	// and it should be clear the session no longer exists
	require.Equal(false, exists)
	require.Nil(actual)
}
