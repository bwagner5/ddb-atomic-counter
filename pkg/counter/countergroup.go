package counter

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	CounterGroupNameCol    = "CounterName"
	CounterGroupCounterCol = "CounterValue"
)

type CounterGroup struct {
	name string
	ddb  *dynamodb.Client
}

func NewCounterGroup(name string, ddb *dynamodb.Client) *CounterGroup {
	return &CounterGroup{
		name: name,
		ddb:  ddb,
	}
}

// Name returns the immutable CounterGroup name
func (cg CounterGroup) Name() string {
	return cg.name
}

func (cg CounterGroup) CreateInfra(ctx context.Context) (*types.TableDescription, error) {
	// check if the table already exists
	existingOut, err := cg.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &cg.name})
	if err == nil {
		return existingOut.Table, nil
	}
	// if the table doesn't exist create it
	out, err := cg.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   &cg.name,
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(CounterGroupNameCol),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(CounterGroupNameCol),
				KeyType:       types.KeyTypeHash,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create table %s: %w", cg.name, err)
	}
	// wait for the table to be ready for use
	waiter := dynamodb.NewTableExistsWaiter(cg.ddb)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(cg.name)}, 5*time.Minute); err != nil {
		return nil, fmt.Errorf("counter group Table %s never became ready: %w", cg.name, err)
	}
	return out.TableDescription, err
}
