package dynamostore

import (
	"errors"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var _ scs.Store = &DynamoStore{}

// DefaultTableName is used when a more specific name isn't provided.
const DefaultTableName = "scs.session"

// ErrDeleteInProgress is returned when table creation fails because
// a table with the same name was recently deleted.
var ErrDeleteInProgress = errors.New("table deletion in progress")

// ErrCreateTimedOut is returned when table creation takes too long.
var ErrCreateTimedOut = errors.New("timed out waiting for table creation")

// DynamoStore represents the session store.
type DynamoStore struct {
	svc   *dynamodb.DynamoDB
	table *string
}

type sessionItem struct {
	Token string `dynamodbav:"token,string"`
	Data  []byte
	TTL   time.Time `dynamodbav:"ttl,unixtime"`
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
		table: aws.String(table),
	}
}

// Find returns the data for a given session token from the DynamoStore instance.
// If the session token is not found or is expired, the returned exists flag
// will be set to false.
func (s *DynamoStore) Find(token string) (b []byte, exists bool, err error) {
	item, err := s.getItem(token)
	switch {
	case err != nil:
		return nil, false, err
	case item.Token == "":
		return nil, false, nil
	case item.TTL.Before(time.Now()):
		return nil, false, nil
	}
	return item.Data, true, nil
}

// Commit adds a session token and data to the DynamoStore instance with the
// given expiry time. If the session token already exists then the data and
// expiry time are updated.
func (s *DynamoStore) Commit(token string, data []byte, expiry time.Time) error {
	return s.setItem(token, data, expiry)
}

// Delete removes a session token and corresponding data from the DynamoStore
// instance.
func (s *DynamoStore) Delete(token string) error {
	return s.deleteItem(token)
}

// CreateTable creates the session store table, if it doesn't already exist.
// This is only intended as a convenience function to make development and
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
	if err := s.waitForTable(); err != nil {
		return err
	}
	return s.updateTTL()
}

func (s *DynamoStore) checkForTable() (bool, error) {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: s.table,
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
		TableName:   s.table,
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

func (s *DynamoStore) deleteItem(token string) error {
	_, err := s.svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: s.table,
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
	})
	return err
}

func (s *DynamoStore) getItem(token string) (*sessionItem, error) {
	result, err := s.svc.GetItem(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      s.table,
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	item := &sessionItem{}
	err = dynamodbattribute.UnmarshalMap(result.Item, item)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *DynamoStore) setItem(token string, data []byte, expiry time.Time) error {
	av, err := dynamodbattribute.MarshalMap(&sessionItem{
		Token: token,
		Data:  data,
		TTL:   expiry,
	})
	if err != nil {
		return err
	}

	_, err = s.svc.PutItem(&dynamodb.PutItemInput{
		Item:      av,
		TableName: s.table,
	})
	return err
}

func (s *DynamoStore) updateTTL() error {
	updateTTL := &dynamodb.UpdateTimeToLiveInput{
		TableName: s.table,
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	}
	_, err := s.svc.UpdateTimeToLive(updateTTL)
	return err
}

func (s *DynamoStore) waitForTable() error {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: s.table,
	}
	for i := 0; i < 60; i++ {
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
	return ErrCreateTimedOut
}
