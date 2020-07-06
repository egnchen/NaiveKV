package common

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync"
	"time"
)

var once sync.Once
var logger *zap.Logger = nil

type ZkLoggerAdapter struct{}

func (_ *ZkLoggerAdapter) Printf(fmt string, args ...interface{}) {
	SugaredLog().Infof("[ZooKeeper] "+fmt, args...)
}

func EmptyTimeEncoder(_ time.Time, _ zapcore.PrimitiveArrayEncoder) {
	// do nothing
}

func Log() *zap.Logger {
	once.Do(func() {
		loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.EncoderConfig.EncodeTime = EmptyTimeEncoder
		loggerConfig.EncoderConfig.EncodeCaller = nil
		loggerConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		l, err := loggerConfig.Build()
		if err != nil {
			panic(err)
		}
		logger = l
	})
	return logger
}

func SugaredLog() *zap.SugaredLogger {
	return Log().Sugar()
}
