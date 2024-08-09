package utils

import (
	"io"
	"log"
	"os"
	"path"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func GetLogger(dir string) *zap.Logger {

	// 判断目录
	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm) // 创建目录
	}

	// 打开目录下文件 dir + log.log (不存在，创建的新的，存在直接打开 ，追加写)
	file, err := os.OpenFile(path.Join(dir, "log.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("打开日志文件失败", err)
	}

	// 格式配置
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	// encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.ConsoleSeparator = " | "

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	writer := zapcore.AddSync(io.MultiWriter(file, os.Stdout)) // io.MultiWriter(file, os.Stdout) 表示会输出到文件 + 控制台

	// core ： 格式 + 输出句柄
	core := zapcore.NewCore(encoder, writer, zapcore.DebugLevel)

	// 日志对象
	return zap.New(core, zap.AddCaller())
}
