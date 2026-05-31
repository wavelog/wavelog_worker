package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wavelog/wavelog_worker/internal/api"
	"github.com/wavelog/wavelog_worker/internal/auth"
	"github.com/wavelog/wavelog_worker/internal/cluster"
	"github.com/wavelog/wavelog_worker/internal/config"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
	"github.com/wavelog/wavelog_worker/internal/ws"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if len(cfg.WorkerSecret) < 32 {
		log.Fatalf("config: worker_secret must be at least 32 characters")
	}

	subMgr := sub.NewManager()

	var reg registry.Registry
	var pub cluster.Publisher
	var rp *cluster.RedisPublisher
	if cfg.RedisURL != "" {
		var err error
		rp, err = cluster.NewRedisPublisher(cfg.RedisURL, subMgr)
		if err != nil {
			log.Printf("cluster: redis unavailable, falling back to single-instance: %v", err)
			reg = registry.New()
			pub = cluster.NewNoopPublisher(subMgr)
		} else {
			log.Printf("cluster: redis pub/sub active (%s)", cfg.RedisURL)
			reg = cluster.NewRedisRegistry(rp.Client(), rp.Context())
			pub = rp
		}
	} else {
		reg = registry.New()
		pub = cluster.NewNoopPublisher(subMgr)
	}

	authBr := auth.NewBridge(reg, cfg.WorkerSecret)
	wsHdlr := ws.NewHandler(authBr, subMgr, reg)

	apiSvr := api.NewServer(subMgr, pub, reg, cfg.WorkerSecret, version)

	wsMux := http.NewServeMux()
	wsMux.Handle("/ws", wsHdlr)

	wsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.WSPort),
		Handler: wsMux,
	}
	internalServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.InternalPort),
		Handler: apiSvr.Handler(),
	}

	log.Printf("welcome to the wavelog worker (version %s)", version)

	go func() {
		log.Printf("ws server listening on :%d", cfg.WSPort)
		if err := wsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ws server: %v", err)
		}
	}()
	go func() {
		log.Printf("internal server listening on :%d", cfg.InternalPort)
		if err := internalServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("internal server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	ctx10, cancel10 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel10()
	wsServer.Shutdown(ctx10)

	ctx5, cancel5 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel5()
	internalServer.Shutdown(ctx5)

	if rp != nil {
		rp.Close()
	}

	log.Println("stopped")
}
