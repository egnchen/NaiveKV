package common

import (
	"fmt"
	"go.uber.org/zap"
)

var logger *zap.Logger = nil

func Log() *zap.Logger {
	if logger == nil {
		l, err := zap.NewDevelopment()
		if err != nil {
			fmt.Printf("Failed to initialize logger: %v\n", err)
			return nil
		}
		logger = l
	}
	return logger
}

func SugaredLog() *zap.SugaredLogger {
	return Log().Sugar()
}
