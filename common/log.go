package common

import (
	"fmt"
	"go.uber.org/zap"
	"sync"
)

var once sync.Once
var logger *zap.Logger = nil

type ZkLoggerAdapter struct{}

func (_ *ZkLoggerAdapter) Printf(fmt string, args ...interface{}) {
	SugaredLog().Infof("[ZooKeeper] "+fmt, args...)
}

func Log() *zap.Logger {
	once.Do(func() {
		if logger == nil {
			l, err := zap.NewDevelopment()
			if err != nil {
				panic(fmt.Sprintf("Failed to initialize logger: %v\n", err))
			}
			logger = l
		}
	})
	return logger
}

func SugaredLog() *zap.SugaredLogger {
	return Log().Sugar()
}
