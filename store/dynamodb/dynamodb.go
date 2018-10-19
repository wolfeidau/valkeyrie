package dynamodb

import (
	"encoding/base64"
	"errors"
	"strconv"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	// DefaultReadCapacityUnits default read capacity used to create table
	DefaultReadCapacityUnits = 2
	// DefaultWriteCapacityUnits default write capacity used to create table
	DefaultWriteCapacityUnits = 2
	// TableCreateTimeoutSeconds the maximum time we wait for the AWS DynamoDB table to be created
	TableCreateTimeoutSeconds = 30
	// DeleteTreeTimeoutSeconds the maximum time we retry a write batch
	DeleteTreeTimeoutSeconds = 30
)

var (
	// ErrBucketOptionMissing is returned when bucket config option is missing
	ErrBucketOptionMissing = errors.New("missing dynamodb bucket/table name")
	// ErrMultipleEndpointsUnsupported is returned when more than one endpoint is provided
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb only supports one endpoint")
	// ErrTableCreateTimeout table creation timed out
	ErrTableCreateTimeout = errors.New("dynamodb table creation timed out")
	// ErrDeleteTreeTimeout delete batch timed out
	ErrDeleteTreeTimeout = errors.New("delete batch timed out")
)

// Register register a store provider in valkeyrie for AWS DynamoDB
func Register() {
	valkeyrie.AddStore(store.DYNAMODB, New)
}

// New opens a creates a new table
func New(endpoints []string, options *store.Config) (store.Store, error) {

	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	if (options == nil) || (len(options.Bucket) == 0) {
		return nil, ErrBucketOptionMissing
	}
	var config *aws.Config
	if len(endpoints) == 1 {
		config = &aws.Config{
			Endpoint: aws.String(endpoints[0]),
		}
	}

	ddb := &DynamoDB{
		dynamoSvc: dynamodb.New(session.Must(session.NewSession(config))),
		tableName: options.Bucket,
	}

	return ddb, nil
}

// DynamoDB store used to interact with AWS DynamoDB
type DynamoDB struct {
	dynamoSvc dynamodbiface.DynamoDBAPI
	tableName string
}

// Put a value at the specified key
func (ddb *DynamoDB) Put(key string, value []byte, options *store.WriteOptions) error {

	keyAttr := make(map[string]*dynamodb.AttributeValue)
	keyAttr["id"] = &dynamodb.AttributeValue{S: aws.String(key)}

	exAttr := make(map[string]*dynamodb.AttributeValue)

	exAttr[":incr"] = &dynamodb.AttributeValue{N: aws.String("1")}

	req := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       keyAttr,
		ExpressionAttributeValues: exAttr,
	}

	if len(value) > 0 {
		encodedValue := base64.StdEncoding.EncodeToString(value)
		exAttr[":encv"] = &dynamodb.AttributeValue{S: aws.String(encodedValue)}
		req.UpdateExpression = aws.String("ADD version :incr SET encoded_value = :encv")
	} else {
		req.UpdateExpression = aws.String("ADD version :incr")
	}

	_, err := ddb.dynamoSvc.UpdateItem(req)
	if err != nil {
		return err
	}

	return nil
}

// Get a value given its key
func (ddb *DynamoDB) Get(key string, options *store.ReadOptions) (*store.KVPair, error) {
	res, err := ddb.getKey(key)
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, store.ErrKeyNotFound
	}

	return decodeItem(res.Item)
}

func (ddb *DynamoDB) getKey(key string) (*dynamodb.GetItemOutput, error) {
	return ddb.dynamoSvc.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(key),
			},
		},
	})
}

// Delete the value at the specified key
func (ddb *DynamoDB) Delete(key string) error {
	_, err := ddb.dynamoSvc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(key),
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *DynamoDB) Exists(key string, options *store.ReadOptions) (bool, error) {

	res, err := ddb.dynamoSvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(key),
			},
		},
	})

	if err != nil {
		return false, err
	}

	if res.Item == nil {
		return false, nil
	}

	return true, nil
}

// List the content of a given prefix
func (ddb *DynamoDB) List(directory string, options *store.ReadOptions) ([]*store.KVPair, error) {

	expAttr := make(map[string]*dynamodb.AttributeValue)

	expAttr[":namePrefix"] = &dynamodb.AttributeValue{S: aws.String(directory)}

	res, err := ddb.dynamoSvc.Scan(&dynamodb.ScanInput{
		TableName:                 aws.String(ddb.tableName),
		FilterExpression:          aws.String("begins_with(id, :namePrefix)"),
		ExpressionAttributeValues: expAttr,
	})
	if err != nil {
		return nil, err
	}

	if len(res.Items) == 0 {
		return nil, store.ErrKeyNotFound
	}

	kvArray := []*store.KVPair{}
	val := new(store.KVPair)

	for _, item := range res.Items {
		val, err = decodeItem(item)
		if err != nil {
			return nil, err
		}
		// skip the records which match the prefix
		if val.Key == directory {
			continue
		}
		kvArray = append(kvArray, val)
	}

	return kvArray, nil
}

// DeleteTree deletes a range of keys under a given directory
func (ddb *DynamoDB) DeleteTree(keyPrefix string) error {
	expAttr := make(map[string]*dynamodb.AttributeValue)

	expAttr[":namePrefix"] = &dynamodb.AttributeValue{S: aws.String(keyPrefix)}

	res, err := ddb.dynamoSvc.Scan(&dynamodb.ScanInput{
		TableName:                 aws.String(ddb.tableName),
		FilterExpression:          aws.String("begins_with(id, :namePrefix)"),
		ExpressionAttributeValues: expAttr,
	})
	if err != nil {
		return err
	}

	if len(res.Items) == 0 {
		return nil
	}

	items := make(map[string][]*dynamodb.WriteRequest)

	items[ddb.tableName] = make([]*dynamodb.WriteRequest, len(res.Items))

	for n, item := range res.Items {
		items[ddb.tableName][n] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"id": item["id"],
				},
			},
		}
	}

	return ddb.retryDeleteTree(items)
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {

	getRes, err := ddb.getKey(key)
	if err != nil {
		return false, nil, err
	}

	// AtomicPut is equivalent to Put if previous is nil and the Key
	// doesn't exist in the DB.
	if previous == nil && getRes.Item != nil {
		return false, nil, store.ErrKeyExists
	}

	keyAttr := make(map[string]*dynamodb.AttributeValue)
	keyAttr["id"] = &dynamodb.AttributeValue{S: aws.String(key)}

	exAttr := make(map[string]*dynamodb.AttributeValue)
	exAttr[":incr"] = &dynamodb.AttributeValue{N: aws.String("1")}

	req := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       keyAttr,
		ExpressionAttributeValues: exAttr,
	}

	if len(value) > 0 {
		encodedValue := base64.StdEncoding.EncodeToString(value)
		exAttr[":encv"] = &dynamodb.AttributeValue{S: aws.String(encodedValue)}
		req.UpdateExpression = aws.String("ADD version :incr SET encoded_value = :encv")
	} else {
		req.UpdateExpression = aws.String("ADD version :incr")
	}

	if previous != nil {
		exAttr[":lastVersion"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(previous.LastIndex, 10))}
		req.ConditionExpression = aws.String("version = :lastVersion")
	}

	_, err = ddb.dynamoSvc.UpdateItem(req)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, nil, store.ErrKeyModified
			}
		}
		return false, nil, err
	}

	return true, nil, nil
}

// AtomicDelete delete of a single value
func (ddb *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {

	getRes, err := ddb.getKey(key)
	if err != nil {
		return false, err
	}

	if previous == nil && getRes.Item != nil {
		return false, store.ErrKeyExists
	}

	expAttr := make(map[string]*dynamodb.AttributeValue)
	expAttr[":lastVersion"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(previous.LastIndex, 10))}

	_, err = ddb.dynamoSvc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(key),
			},
		},
		ConditionExpression:       aws.String("version = :lastVersion"),
		ExpressionAttributeValues: expAttr,
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, store.ErrKeyNotFound
			}
		}
		return false, err
	}

	return true, nil
}

// Close nothing to see here
func (ddb *DynamoDB) Close() {}

// NewLock has to implemented at the library level since its not supported by DynamoDB
func (ddb *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// Watch has to implemented at the library level since its not supported by DynamoDB
func (ddb *DynamoDB) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree has to implemented at the library level since its not supported by DynamoDB
func (ddb *DynamoDB) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (ddb *DynamoDB) createTable() error {

	_, err := ddb.dynamoSvc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		// enable encryption of data by default
		SSESpecification: &dynamodb.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: aws.String(dynamodb.SSETypeAes256),
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(DefaultReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(DefaultWriteCapacityUnits),
		},
		TableName: aws.String(ddb.tableName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
				return nil
			}
		}
		return err
	}

	err = ddb.dynamoSvc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(ddb.tableName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DynamoDB) retryDeleteTree(items map[string][]*dynamodb.WriteRequest) error {

	batchResult, err := ddb.dynamoSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})
	if err != nil {
		return err
	}

	if len(batchResult.UnprocessedItems) == 0 {
		return nil
	}

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(DeleteTreeTimeoutSeconds * time.Second)
		timeout <- true
	}()

	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	// poll once a second for table status, until the table is either active
	// or the timeout deadline has been reached
	for {
		select {
		case <-ticker.C:
			batchResult, err = ddb.dynamoSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems: batchResult.UnprocessedItems,
			})
			if err != nil {
				return err
			}

			if len(batchResult.UnprocessedItems) == 0 {
				return nil
			}

		case <-timeout:
			// polling for table status has taken more than the timeout
			return ErrDeleteTreeTimeout
		}
	}

}

func decodeItem(item map[string]*dynamodb.AttributeValue) (*store.KVPair, error) {
	entry := new(ddbEntry)

	err := dynamodbattribute.ConvertFromMap(item, entry)
	if err != nil {
		return nil, err
	}

	rawValue, err := base64.StdEncoding.DecodeString(entry.EncodedValue)
	if err != nil {
		return nil, err
	}

	return &store.KVPair{
		Key:       entry.Name,
		Value:     rawValue,
		LastIndex: uint64(entry.Version),
	}, nil
}

type ddbEntry struct {
	Name         string `json:"id"`
	EncodedValue string `json:"encoded_value"`
	Version      int64  `json:"version"`
}
