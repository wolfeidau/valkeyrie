package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
)

const TestTableName = "test-1-valkeyrie"

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(
		store.DYNAMODB,
		[]string{},
		&store.Config{Bucket: "test-1-valkeyrie"},
	)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if ddb, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing DynamoDB")
		ddb.createTable()
	}

}

func TestSetup(t *testing.T) {
	ddb := newDynamoDBStore(t)
	// ensure this is idempotent
	err := ddb.createTable()
	assert.Nil(t, err)
}

func TestDynamoDBStore(t *testing.T) {
	ddbStore := newDynamoDBStore(t)
	testutils.RunTestCommon(t, ddbStore)
	testutils.RunTestAtomic(t, ddbStore)
}

func TestDynamoDBStoreUnsupported(t *testing.T) {
	ddbStore := newDynamoDBStore(t)
	_, err := ddbStore.NewLock("test", nil)
	assert.Equal(t, store.ErrCallNotSupported, err)

	_, err = ddbStore.WatchTree("test", nil, nil)
	assert.Equal(t, store.ErrCallNotSupported, err)

	_, err = ddbStore.Watch("test", nil, nil)
	assert.Equal(t, store.ErrCallNotSupported, err)
}

func TestBatchWrite(t *testing.T) {

	dynamodbSvc := newDynamoDB()

	mock := &mockedBatchWrite{DynamoDBAPI: dynamodbSvc}
	mock.BatchWriteResp = &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]*dynamodb.WriteRequest{
			"test-1-valkeyrie": []*dynamodb.WriteRequest{
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: map[string]*dynamodb.AttributeValue{
							"id": &dynamodb.AttributeValue{
								S: aws.String("abc123"),
							},
						},
					},
				},
			},
		},
	}
	mock.Count = 1

	kv := &DynamoDB{
		dynamoSvc: mock,
		tableName: "test-1-valkeyrie",
	}

	prefix := "testDeleteTree"

	firstKey := "testDeleteTree/first"
	firstValue := []byte("first")

	secondKey := "testDeleteTree/second"
	secondValue := []byte("second")

	// Put the first key
	err := kv.Put(firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(secondKey, secondValue, nil)
	assert.NoError(t, err)

	err = kv.DeleteTree(prefix)
	assert.NoError(t, err)
}

type mockedBatchWrite struct {
	dynamodbiface.DynamoDBAPI
	BatchWriteResp *dynamodb.BatchWriteItemOutput
	Count          int
}

func (m *mockedBatchWrite) BatchWriteItem(in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if m.Count > 0 {
		m.Count--
		return m.BatchWriteResp, nil
	}

	return &dynamodb.BatchWriteItemOutput{}, nil
}

func newDynamoDB() *dynamodb.DynamoDB {

	creds := credentials.NewStaticCredentials("test", "test", "test")

	config := aws.NewConfig().WithCredentials(creds)
	config.Endpoint = aws.String("http://localhost:8000")
	config.Region = aws.String("us-east-1")

	sess := session.Must(session.NewSession(config))

	return dynamodb.New(sess)
}

func newDynamoDBStore(t *testing.T) *DynamoDB {

	ddb := newDynamoDB()

	ddbStore := &DynamoDB{
		dynamoSvc: ddb,
		tableName: TestTableName,
	}

	err := deleteTable(ddb, TestTableName)
	assert.Nil(t, err)
	err = ddbStore.createTable()
	assert.Nil(t, err)

	return ddbStore
}

func deleteTable(dynamoSvc *dynamodb.DynamoDB, tableName string) error {
	_, err := dynamoSvc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return nil
			}
		}
		return err
	}

	err = dynamoSvc.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	return nil
}
