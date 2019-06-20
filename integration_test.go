// +build integration

package dynamostore_test

import (
	"context"
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
