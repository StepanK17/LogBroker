package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/StepanK17/LogBroker/api/proto/brokerv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type healthResponse struct {
	Status string `json:"status"`
}

type cliConfig struct {
	grpcAddr string
	httpAddr string
	timeout  time.Duration
}

func main() {
	cfg := parseGlobalConfig()
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	var err error

	switch args[0] {
	case "health":
		err = runHealth(cfg)
	case "create-topic":
		err = runCreateTopic(cfg, args[1:])
	case "produce":
		err = runProduce(cfg, args[1:])
	case "consume":
		err = runConsume(cfg, args[1:])
	case "commit-offset":
		err = runCommitOffset(cfg, args[1:])
	case "get-offset":
		err = runGetOffset(cfg, args[1:])
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "command %q failed: %v\n", args[0], err)
		os.Exit(1)
	}
}

func parseGlobalConfig() cliConfig {
	grpcAddr := flag.String("grpc-addr", "localhost:9090", "Broker gRPC address")
	httpAddr := flag.String("http-addr", "http://localhost:8080", "Broker HTTP address")
	timeout := flag.Duration("timeout", 5*time.Second, "Request timeout")
	flag.Parse()

	return cliConfig{
		grpcAddr: *grpcAddr,
		httpAddr: *httpAddr,
		timeout:  *timeout,
	}
}

func runHealth(cfg cliConfig) error {
	client := &http.Client{Timeout: cfg.timeout}

	resp, err := client.Get(strings.TrimRight(cfg.httpAddr, "/") + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload healthResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("broker status: %s\n", payload.Status)
	return nil
}

func runCreateTopic(cfg cliConfig, args []string) error {
	fs := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	name := fs.String("name", "", "Topic name")
	partitions := fs.Int("partitions", 1, "Partitions count")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *name == "" {
		return fmt.Errorf("name is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client, conn, err := newBrokerClient(ctx, cfg.grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := client.CreateTopic(ctx, &brokerv1.CreateTopicRequest{
		Name:       *name,
		Partitions: uint32(*partitions),
	}); err != nil {
		return err
	}

	fmt.Printf("topic created: name=%s partitions=%d\n", *name, *partitions)
	return nil
}

func runProduce(cfg cliConfig, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	topic := fs.String("topic", "", "Topic name")
	partition := fs.Int("partition", 0, "Partition number")
	key := fs.String("key", "", "Record key")
	value := fs.String("value", "", "Record value")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *topic == "" {
		return fmt.Errorf("topic is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client, conn, err := newBrokerClient(ctx, cfg.grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := client.Produce(ctx, &brokerv1.ProduceRequest{
		Topic:     *topic,
		Partition: uint32(*partition),
		Key:       []byte(*key),
		Value:     []byte(*value),
	})
	if err != nil {
		return err
	}

	fmt.Printf("produced: offset=%d timestamp_unix_nano=%d\n", resp.Offset, resp.TimestampUnixNano)
	return nil
}

func runConsume(cfg cliConfig, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	topic := fs.String("topic", "", "Topic name")
	partition := fs.Int("partition", 0, "Partition number")
	offset := fs.Uint64("offset", 0, "Read starting offset")
	maxRecords := fs.Uint("max-records", 100, "Maximum records to fetch")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *topic == "" {
		return fmt.Errorf("topic is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client, conn, err := newBrokerClient(ctx, cfg.grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := client.Fetch(ctx, &brokerv1.FetchRequest{
		Topic:      *topic,
		Partition:  uint32(*partition),
		Offset:     *offset,
		MaxRecords: uint32(*maxRecords),
	})
	if err != nil {
		return err
	}

	if len(resp.Records) == 0 {
		fmt.Printf("no records, next_offset=%d\n", resp.NextOffset)
		return nil
	}

	for _, record := range resp.Records {
		fmt.Printf(
			"offset=%d timestamp_unix_nano=%d key=%q value=%q\n",
			record.Offset,
			record.TimestampUnixNano,
			record.Key,
			record.Value,
		)
	}

	fmt.Printf("next_offset=%d\n", resp.NextOffset)

	return nil
}

func runCommitOffset(cfg cliConfig, args []string) error {
	fs := flag.NewFlagSet("commit-offset", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	group := fs.String("group", "", "Consumer group")
	topic := fs.String("topic", "", "Topic name")
	partition := fs.Int("partition", 0, "Partition number")
	offset := fs.Uint64("offset", 0, "Committed offset")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *group == "" {
		return fmt.Errorf("group is required")
	}
	if *topic == "" {
		return fmt.Errorf("topic is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client, conn, err := newBrokerClient(ctx, cfg.grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := client.CommitOffset(ctx, &brokerv1.CommitOffsetRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: uint32(*partition),
		Offset:    *offset,
	}); err != nil {
		return err
	}

	fmt.Printf("offset committed: group=%s topic=%s partition=%d offset=%d\n", *group, *topic, *partition, *offset)
	return nil
}

func runGetOffset(cfg cliConfig, args []string) error {
	fs := flag.NewFlagSet("get-offset", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	group := fs.String("group", "", "Consumer group")
	topic := fs.String("topic", "", "Topic name")
	partition := fs.Int("partition", 0, "Partition number")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *group == "" {
		return fmt.Errorf("group is required")
	}
	if *topic == "" {
		return fmt.Errorf("topic is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client, conn, err := newBrokerClient(ctx, cfg.grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := client.GetCommittedOffset(ctx, &brokerv1.GetCommittedOffsetRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: uint32(*partition),
	})
	if err != nil {
		return err
	}

	if !resp.Found {
		fmt.Printf("offset not found: group=%s topic=%s partition=%d\n", *group, *topic, *partition)
		return nil
	}

	fmt.Printf("committed offset: group=%s topic=%s partition=%d offset=%d\n", *group, *topic, *partition, resp.Offset)
	return nil
}

func newBrokerClient(ctx context.Context, addr string) (brokerv1.BrokerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}

	return brokerv1.NewBrokerServiceClient(conn), conn, nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  kafka-clone-cli [global flags] health")
	fmt.Println("  kafka-clone-cli [global flags] create-topic -name orders -partitions 3")
	fmt.Println("  kafka-clone-cli [global flags] produce -topic orders -partition 0 -key id-1 -value hello")
	fmt.Println("  kafka-clone-cli [global flags] consume -topic orders -partition 0 -offset 0 -max-records 100")
	fmt.Println("  kafka-clone-cli [global flags] commit-offset -group demo -topic orders -partition 0 -offset 5")
	fmt.Println("  kafka-clone-cli [global flags] get-offset -group demo -topic orders -partition 0")
	fmt.Println()
	fmt.Println("Global flags:")
	fmt.Println("  -grpc-addr   default localhost:9090")
	fmt.Println("  -http-addr   default http://localhost:8080")
	fmt.Println("  -timeout     default 5s")
}
