package app

import (
	"context"
	"errors"
	"log"
	"net"
	nethttp "net/http"
	"time"

	"github.com/StepanK17/LogBroker/api/proto/brokerv1"
	grpcapi "github.com/StepanK17/LogBroker/internal/api/grpc"
	"github.com/StepanK17/LogBroker/internal/api/http"
	"github.com/StepanK17/LogBroker/internal/broker"
	"github.com/StepanK17/LogBroker/internal/config"
	"google.golang.org/grpc"
)

type App struct {
	logger     *log.Logger
	svc        *broker.Service
	grpcServer *grpc.Server
	listener   net.Listener
	httpServer *nethttp.Server
}

func New(cfg config.Config, logger *log.Logger) (*App, error) {
	svc, err := broker.OpenServiceWithOptions(cfg.Storage.DataDir, cfg.Storage.SegmentMaxBytes)
	if err != nil {
		return nil, err
	}
	apiServer := grpcapi.NewServer(svc)
	httpHandler := http.NewServer(logger, svc)
	httpServer := &nethttp.Server{
		Addr:    cfg.HTTP.Address,
		Handler: httpHandler,
	}
	grpcServer := grpc.NewServer()
	brokerv1.RegisterBrokerServiceServer(grpcServer, apiServer)
	lis, err := net.Listen("tcp", cfg.GRPC.Address)
	if err != nil {
		svc.Close()
		return nil, err
	}
	return &App{
		logger:     logger,
		svc:        svc,
		grpcServer: grpcServer,
		listener:   lis,
		httpServer: httpServer,
	}, nil

}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		a.logger.Printf("gRPC server listening on %s", a.listener.Addr().String())
		errCh <- a.grpcServer.Serve(a.listener)
	}()

	go func() {
		a.logger.Printf("http server listening on %s", a.httpServer.Addr)

		err := a.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			errCh <- err
			return
		}
	}()

	select {
	case <-ctx.Done():
		return a.shutdown(context.Background())
	case err := <-errCh:
		return errors.Join(err, a.shutdown(context.Background()))
	}
}

func (a *App) shutdown(ctx context.Context) error {
	a.grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	httpErr := a.httpServer.Shutdown(shutdownCtx)
	svcErr := a.svc.Close()

	return errors.Join(httpErr, svcErr)
}
