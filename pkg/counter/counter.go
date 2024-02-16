package counter

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Counter struct {
	name string
	cg   *CounterGroup
}

// NewCounter creates a counter from a CounterGroup
// A Counter within a CounterGroup uses a shared DDB table
func (cg CounterGroup) NewCounter(name string) *Counter {
	return &Counter{
		name: name,
		cg:   &cg,
	}
}

// Name returns the immutable Counter name
func (c Counter) Name() string {
	return c.name
}

func (c Counter) Init(ctx context.Context) error {
	_, err := c.cg.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &c.cg.name,
		Item: map[string]types.AttributeValue{
			CounterGroupNameCol:    &types.AttributeValueMemberS{Value: c.name},
			CounterGroupCounterCol: &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", CounterGroupNameCol)),
	})
	return err
}

// Inc returns an atomic monotonically increasing number
func (c Counter) Inc(ctx context.Context) (int64, error) {
	updateOut, err := c.cg.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &c.cg.name,
		Key: map[string]types.AttributeValue{
			CounterGroupNameCol: &types.AttributeValueMemberS{Value: c.name},
		},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = %s + :incr", CounterGroupCounterCol, CounterGroupCounterCol)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":incr": &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return -1, err
	}
	count := updateOut.Attributes[CounterGroupCounterCol].(*types.AttributeValueMemberN)
	countVal, err := strconv.ParseInt(count.Value, 10, 64)
	if err != nil {
		return -1, err
	}
	return countVal, nil
}
