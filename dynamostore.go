package dynamostore

import (
	"context"
	"errors"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	svc   *dynamodb.Client
	table *string
}

type sessionItem struct {
	Token string `dynamodbav:"token,string"`
	Data  []byte
	TTL   time.Time `dynamodbav:"ttl,unixtime"`
}

// New creates a DynamoStore instance using default values.
func New(svc *dynamodb.Client) *DynamoStore {
	return NewWithTableName(svc, DefaultTableName)
}

// NewWithTableName create a DynamoStore instance, overriding the default
// table name.
func NewWithTableName(svc *dynamodb.Client, table string) *DynamoStore {
	return &DynamoStore{
		svc:   svc,
		table: aws.String(table),
	}
}

// Find returns the data for a given session token from the DynamoStore instance.
// If the session token is not found or is expired, the returned exists flag
// will be set to false.
func (s *DynamoStore) Find(token string) (b []byte, exists bool, err error) {
	ctx := context.Background()
	item, err := s.getItem(ctx, token)
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
	ctx := context.Background()
	return s.setItem(ctx, token, data, expiry)
}

// Delete removes a session token and corresponding data from the DynamoStore
// instance.
func (s *DynamoStore) Delete(token string) error {
	ctx := context.Background()
	if token == "" {
		return nil
	}
	return s.deleteItem(ctx, token)
}

// CreateTable creates the session store table, if it doesn't already exist.
// This is only intended as a convenience function to make development and
// testing easier. It is not intended for use in production.
func (s *DynamoStore) CreateTable() error {
	ctx := context.Background()
	if ok, err := s.checkForTable(ctx); err != nil {
		return err
	} else if ok {
		return nil
	}
	if err := s.createTable(ctx); err != nil {
		return err
	}
	if err := s.waitForTable(ctx); err != nil {
		return err
	}
	return s.updateTTL(ctx)
}

func (s *DynamoStore) checkForTable(ctx context.Context) (bool, error) {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: s.table,
	}
	result, err := s.svc.DescribeTable(ctx, describeTable)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return false, nil
		}
		return false, err
	}
	switch status := result.Table.TableStatus; status {
	case types.TableStatusCreating:
		return true, s.waitForTable(ctx)
	case types.TableStatusDeleting:
		return false, ErrDeleteInProgress
	case types.TableStatusActive, types.TableStatusUpdating:
		return true, nil
	default:
		return false, errors.New("unrecognized table status: " + string(status))
	}
}

func (s *DynamoStore) createTable(ctx context.Context) error {
	createTable := &dynamodb.CreateTableInput{
		BillingMode: types.BillingModePayPerRequest,
		TableName:   s.table,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("token"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("token"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	}
	_, err := s.svc.CreateTable(ctx, createTable)
	return err
}

func (s *DynamoStore) deleteItem(ctx context.Context, token string) error {
	_, err := s.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: s.table,
		Key: map[string]types.AttributeValue{
			"token": &types.AttributeValueMemberS{
				Value: token,
			},
		},
	})
	return err
}

func (s *DynamoStore) getItem(ctx context.Context, token string) (*sessionItem, error) {
	result, err := s.svc.GetItem(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		TableName:      s.table,
		Key: map[string]types.AttributeValue{
			"token": &types.AttributeValueMemberS{
				Value: token,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	item := &sessionItem{}
	err = attributevalue.UnmarshalMap(result.Item, item)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *DynamoStore) setItem(ctx context.Context, token string, data []byte, expiry time.Time) error {
	av, err := attributevalue.MarshalMap(&sessionItem{
		Token: token,
		Data:  data,
		TTL:   expiry,
	})
	if err != nil {
		return err
	}

	_, err = s.svc.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      av,
		TableName: s.table,
	})
	return err
}

func (s *DynamoStore) updateTTL(ctx context.Context) error {
	updateTTL := &dynamodb.UpdateTimeToLiveInput{
		TableName: s.table,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	}
	_, err := s.svc.UpdateTimeToLive(ctx, updateTTL)
	return err
}

func (s *DynamoStore) waitForTable(ctx context.Context) error {
	describeTable := &dynamodb.DescribeTableInput{
		TableName: s.table,
	}
	for i := 0; i < 60; i++ {
		time.Sleep(1 * time.Second)
		result, err := s.svc.DescribeTable(ctx, describeTable)
		if err != nil {
			var notFoundErr *types.ResourceNotFoundException
			if errors.As(err, &notFoundErr) {
				return nil
			}
			return err
		}
		switch result.Table.TableStatus {
		case types.TableStatusCreating:
			// continue loop
		case types.TableStatusDeleting:
			return ErrDeleteInProgress
		case types.TableStatusActive, types.TableStatusUpdating:
			return nil
		}
	}
	return ErrCreateTimedOut
}
