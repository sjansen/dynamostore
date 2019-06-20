package dynamostore

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const DefaultTableName = "scs.session"

var ErrDeleteInProgress = errors.New("table deletion in progress")

type DynamoStore struct {
	svc   *dynamodb.DynamoDB
	table string
}

// New creates a DynamoStore instance using default values.
func New(svc *dynamodb.DynamoDB) *DynamoStore {
	return NewWithTableName(svc, DefaultTableName)
}

// NewWithTableName create a DynamoStore instance, overriding the default
// table name.
func NewWithTableName(svc *dynamodb.DynamoDB, table string) *DynamoStore {
	return &DynamoStore{
		svc:   svc,
		table: table,
	}
}

// Find returns the data for a given session token from the DynamoStore instance.
// If the session token is not found or is expired, the returned exists flag
// will be set to false.
func (s *DynamoStore) Find(token string) (b []byte, exists bool, err error) {
	return nil, false, nil
}

// Commit adds a session token and data to the DynamoStore instance with the
// given expiry time. If the session token already exists then the data and
// expiry time are updated.
func (s *DynamoStore) Commit(token string, b []byte, expiry time.Time) error {
	return nil
}

// Delete removes a session token and corresponding data from the DynamoStore
// instance.
func (s *DynamoStore) Delete(token string) error {
	return nil
}

// CreateTable creates the session store table, if it doesn't already exist.
// This is only intended as a convencience function to make development and
// testing easier. It is not intended for use in production.
func (s *DynamoStore) CreateTable() error {
	if ok, err := s.checkForTable(); err != nil {
		return err
	} else if ok {
		return nil
	}
	if err := s.createTable(); err != nil {
		return err
	}
	return s.waitForTable()
}

func (s *DynamoStore) checkForTable() (bool, error) {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: aws.String(s.table),
	}
	result, err := s.svc.DescribeTable(describeTable)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return false, nil
			}
		}
		return false, err
	}
	switch status := aws.StringValue(result.Table.TableStatus); status {
	case "CREATING":
		return true, s.waitForTable()
	case "DELETING":
		return false, ErrDeleteInProgress
	case "ACTIVE", "UPDATING":
		return true, nil
	default:
		return false, errors.New("unrecognized table status: " + status)
	}
}

func (s *DynamoStore) createTable() error {
	createTable := &dynamodb.CreateTableInput{
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   aws.String(s.table),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("token"),
				KeyType:       aws.String("HASH"),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("token"),
				AttributeType: aws.String("S"),
			},
		},
	}
	_, err := s.svc.CreateTable(createTable)
	return err
}

func (s *DynamoStore) waitForTable() error {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: aws.String(s.table),
	}
	for {
		time.Sleep(1 * time.Second)
		result, err := s.svc.DescribeTable(describeTable)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
					return err
				}
			} else {
				return err
			}
		}
		switch aws.StringValue(result.Table.TableStatus) {
		case "CREATING":
			// continue loop
		case "DELETING":
			return ErrDeleteInProgress
		case "ACTIVE", "UPDATING":
			return nil
		}
	}
}
