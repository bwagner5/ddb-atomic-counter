package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/bwagner5/ddb-atomic-counter/pkg/counter"
)

type Result struct {
	SeqNum int64
	Error  error
}

func main() {
	// Setup AWS SDK
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("Unable to load AWS Config")
	}
	ddb := dynamodb.NewFromConfig(cfg)

	// Create a CounterGroup which is a dynamodb table
	counterGroup := counter.NewCounterGroup("scg-seq-nums-poc", ddb)
	table, err := counterGroup.CreateInfra(ctx)
	if err != nil {
		log.Fatalf("Error Creating Counter Group Infra: %s", err)
	}
	log.Printf("CounterGroup Infra Table Created: %s\n", *table.TableName)

	// Create a Counter instance from the CounterGroup
	// This maps to one row of the CounterGroup table
	ctr := counterGroup.NewCounter("scg-cp")
	if err := ctr.Init(ctx); err != nil {
		log.Printf("Counter Init: %s\n", err)
	}

	// Test incrementing the Counter from a bunch of go routines
	results := make(chan Result, 1_000)
	var wg sync.WaitGroup
	start := time.Now().UTC()
	for i := 0; i < 1_000; i++ {
		wg.Add(1)
		go func() {
			seqNum, err := ctr.Inc(ctx)
			if err != nil {
				log.Printf("Got Error: %s", err)
			}
			results <- Result{SeqNum: seqNum, Error: err}
			wg.Done()
		}()
	}

	wg.Wait()
	latency := time.Since(start)
	close(results)

	errs := 0
	succeeded := 0
	for r := range results {
		if r.Error != nil {
			errs++
			continue
		}
		succeeded++
		fmt.Println(r.SeqNum)
	}

	fmt.Println("------------------")
	fmt.Printf("Latency: %s\n", latency)
	fmt.Printf("Succeeded: %d\n", succeeded)
	fmt.Printf("Errors: %d\n", errs)
}
