# DDB Atomic Counter

This repo provides a go library to create CounterGroups which are backed by a DDB table. Each row in the table is an instance of an monotonically increasing atomic counter. 

## Usage

The below command will create a CounterGroup (with ddb infra) and run a test with 1,000 go routines which all increment the counter. The result will show the successful increments, any errors, and the latency.

```
AWS_REGION=us-east-2 go run ./...
```

Example Output:
```
10085
10078
10090
------------------
Latency: 1.315962s
Succeeded: 1000
Errors: 0
```
