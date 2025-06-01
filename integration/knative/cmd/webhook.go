package main

import (
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/davidandw190/data-locality-scheduler/integration/knative/pkg/webhook"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func main() {
	var logLevel string
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.Parse()

	klog.InitFlags(nil)
	if err := flag.Set("v", getVerbosityLevel(logLevel)); err != nil {
		klog.Fatalf("Failed to set log level: %v", err)
	}

	klog.Info("Loading in-cluster configuration")
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to create in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	klog.Info("Creating webhook server")
	webhookServer, err := webhook.NewWebhookServer(clientset, tlsConfig)
	if err != nil {
		klog.Fatalf("Failed to create webhook server: %v", err)
	}

	go func() {
		if err := webhookServer.Start(); err != nil {
			klog.Fatalf("Failed to start webhook server: %v", err)
		}
	}()

	klog.Info("Webhook server started")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	klog.Info("Received shutdown signal, exiting gracefully")
}

func getVerbosityLevel(level string) string {
	switch level {
	case "debug":
		return "4"
	case "info":
		return "2"
	case "warn":
		return "1"
	case "error":
		return "0"
	default:
		return "2" // info - default
	}
}
