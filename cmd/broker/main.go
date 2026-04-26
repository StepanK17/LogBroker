package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/StepanK17/LogBroker/internal/app"
	"github.com/StepanK17/LogBroker/internal/config"
	"github.com/StepanK17/LogBroker/internal/logging"
)

func main() {
	configPath := flag.String("config", "", "Path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger := logging.New()

	application, err := app.New(cfg, logger)
	if err != nil {
		logger.Fatalf("init app: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		logger.Fatalf("run app: %v", err)
	}
}
