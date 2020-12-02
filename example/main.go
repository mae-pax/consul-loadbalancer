package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mae-pax/consul-loadbalancer/balancer"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

type MyLogger struct {
	log *logrus.Logger
}

func (l *MyLogger) Debug(args string) {
	l.log.Debug(args)
}

func (l *MyLogger) Debugf(format string, v ...interface{}) {
	l.log.Debugf(format, v...)
}

func (l *MyLogger) Info(args string) {
	l.log.Info(args)
}

func (l *MyLogger) Infof(format string, v ...interface{}) {
	l.log.Infof(format, v...)
}

func (l *MyLogger) Warnf(format string, v ...interface{}) {
	l.log.Warnf(format, v...)
}

type MyZapLogger struct {
	log *zap.Logger
}

func (l *MyZapLogger) Debug(args string) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Debug(args)
}

func (l *MyZapLogger) Debugf(format string, v ...interface{}) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Debugf(format, v...)
}

func (l *MyZapLogger) Info(args string) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Info(args)
}

func (l *MyZapLogger) Infof(format string, v ...interface{}) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Infof(format, v...)
}

func (l *MyZapLogger) Warn(args string) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Warn(args)
}

func (l *MyZapLogger) Warnf(format string, v ...interface{}) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Warnf(format, v...)
}

func (l *MyZapLogger) Error(args string) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Error(args)
}

func (l *MyZapLogger) Errorf(format string, v ...interface{}) {
	l.log.Sugar()
	defer l.log.Sync()
	sugar := l.log.Sugar()
	sugar.Errorf(format, v...)
}

func main() {
	// logger := &MyLogger{log: logrus.New()}
	devEnvLog, _ := zap.NewDevelopment(zap.Development())
	logger := &MyZapLogger{log: devEnvLog}
	r, err := balancer.NewConsulResolver(
		"aliyun",
		"127.0.0.1:8500",
		"hb-aerospike",
		"clb/hb-aerospike/cpu_threshold.json",
		"clb/hb-aerospike/zone_cpu.json",
		"clb/hb-aerospike/instance_factor.json",
		"clb/hb-aerospike/onlinelab_factor.json",
		1000*time.Millisecond,
		200*time.Millisecond)
	if err != nil {
		panic(err)
	}

	r.SetLogger(logger)
	r.SetZone("us-east-1a")

	err = r.Start()
	if err != nil {
		panic(err)
	}

	c, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		for {
			ts := time.After(100 * time.Millisecond)
			select {
			case <-ts:
				r.SelectNode()
			}
		}
	}(c)

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP, os.Interrupt)

	<-signChan
	r.Stop()
}
