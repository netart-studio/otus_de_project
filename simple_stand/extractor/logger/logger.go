package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	LogDir      = "logs"
	MaxLogSize  = 10 * 1024 * 1024 // 10MB
	MaxLogFiles = 5
)

var (
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
)

func Init() error {
	// Создаем директорию для логов, если она не существует
	if err := os.MkdirAll(LogDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Проверяем права на запись
	if err := checkWritePermissions(LogDir); err != nil {
		return fmt.Errorf("write permission check failed: %w", err)
	}

	// Очищаем старые логи
	if err := cleanOldLogs(); err != nil {
		return fmt.Errorf("failed to clean old logs: %w", err)
	}

	// Создаем новый лог-файл с текущей датой
	logFile := filepath.Join(LogDir, fmt.Sprintf("app_%s.log", time.Now().Format("2006-01-02")))
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Создаем мультиплексор для записи в файл и stdout
	multiWriter := io.MultiWriter(os.Stdout, file)

	// Инициализируем логгеры
	InfoLogger = log.New(multiWriter, "INFO: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	ErrorLogger = log.New(multiWriter, "ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

	// Запускаем горутину для проверки размера лога
	go checkLogSize(logFile, file)

	return nil
}

func checkWritePermissions(dir string) error {
	testFile := filepath.Join(dir, ".write_test")
	file, err := os.OpenFile(testFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("no write permission in log directory: %w", err)
	}
	file.Close()
	os.Remove(testFile)
	return nil
}

func cleanOldLogs() error {
	files, err := os.ReadDir(LogDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	// Сортируем файлы по времени модификации
	var logFiles []os.FileInfo
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			info, err := file.Info()
			if err != nil {
				continue
			}
			logFiles = append(logFiles, info)
		}
	}

	// Удаляем старые файлы, если их больше MaxLogFiles
	if len(logFiles) > MaxLogFiles {
		for i := 0; i < len(logFiles)-MaxLogFiles; i++ {
			oldFile := filepath.Join(LogDir, logFiles[i].Name())
			if err := os.Remove(oldFile); err != nil {
				ErrorLogger.Printf("Failed to remove old log file %s: %v", oldFile, err)
			}
		}
	}

	return nil
}

func checkLogSize(logFile string, file *os.File) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		info, err := file.Stat()
		if err != nil {
			ErrorLogger.Printf("Failed to get log file info: %v", err)
			continue
		}

		if info.Size() > MaxLogSize {
			// Закрываем текущий файл
			file.Close()

			// Создаем новый файл с временной меткой
			newLogFile := filepath.Join(LogDir, fmt.Sprintf("app_%s.log", time.Now().Format("2006-01-02_15-04-05")))
			newFile, err := os.OpenFile(newLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				ErrorLogger.Printf("Failed to create new log file: %v", err)
				continue
			}

			// Обновляем мультиплексор и логгеры
			multiWriter := io.MultiWriter(os.Stdout, newFile)
			InfoLogger = log.New(multiWriter, "INFO: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
			ErrorLogger = log.New(multiWriter, "ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

			file = newFile
		}
	}
}

func Info(format string, v ...interface{}) {
	InfoLogger.Printf(format, v...)
}

func Error(format string, v ...interface{}) {
	ErrorLogger.Printf(format, v...)
}
