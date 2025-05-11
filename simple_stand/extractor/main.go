package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"dashboard/logger"
)

type Trade struct {
	Symbol    string    `json:"s" ch:"symbol"`
	Price     string    `json:"p" ch:"price"`
	Quantity  string    `json:"q" ch:"quantity"`
	TradeTime time.Time `json:"T" ch:"trade_time"`
}

// UnmarshalJSON реализует кастомное разбирание JSON для Trade
func (t *Trade) UnmarshalJSON(data []byte) error {
	type Alias Trade
	aux := &struct {
		TradeTime int64  `json:"T"`
		Price     string `json:"p"`
		Quantity  string `json:"q"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("error unmarshaling trade data: %w", err)
	}

	// Convert milliseconds to time.Time
	t.TradeTime = time.Unix(0, aux.TradeTime*int64(time.Millisecond))
	t.Price = aux.Price
	t.Quantity = aux.Quantity

	return nil
}

type Config struct {
	BinanceWSURL    string
	ClickHouseHost  string
	ClickHousePort  uint32
	ClickHouseDB    string
	ClickHouseUser  string
	ClickHousePass  string
	ClickHouseTable string
	Symbols         []string
	LogLevel        string
	BatchSize       int
}

func loadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("error loading .env file: %w", err)
	}

	// Получаем размер батча из конфигурации или используем значение по умолчанию
	batchSize := 100 // значение по умолчанию
	if batchSizeStr := os.Getenv("BATCH_SIZE"); batchSizeStr != "" {
		if size, err := strconv.Atoi(batchSizeStr); err == nil && size > 0 {
			batchSize = size
		}
	}

	return &Config{
		BinanceWSURL:    os.Getenv("BINANCE_WS_URL"),
		ClickHouseHost:  os.Getenv("CLICKHOUSE_HOST"),
		ClickHousePort:  9000, // Default port
		ClickHouseDB:    os.Getenv("CLICKHOUSE_DATABASE"),
		ClickHouseUser:  os.Getenv("CLICKHOUSE_USERNAME"),
		ClickHousePass:  os.Getenv("CLICKHOUSE_PASSWORD"),
		ClickHouseTable: os.Getenv("CLICKHOUSE_TABLE"),
		Symbols:         strings.Split(os.Getenv("SYMBOLS"), ","),
		LogLevel:        os.Getenv("LOG_LEVEL"),
		BatchSize:       batchSize,
	}, nil
}

func connectClickHouse(cfg *Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePass,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Debug: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("error pinging ClickHouse: %w", err)
	}

	// Проверяем существование базы данных
	if err := conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.ClickHouseDB)); err != nil {
		return nil, fmt.Errorf("error creating database: %w", err)
	}

	return conn, nil
}

func connectWebSocket(cfg *Config, logger *zap.Logger) (*websocket.Conn, error) {
	// Create WebSocket connection with timeout
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   1024 * 1024, // 1MB buffer
		WriteBufferSize:  1024 * 1024, // 1MB buffer
	}

	conn, _, err := dialer.Dial(cfg.BinanceWSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error connecting to WebSocket: %w", err)
	}

	// Set read deadline
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error setting read deadline: %w", err)
	}

	// Subscribe to trade streams
	streams := make([]string, len(cfg.Symbols))
	for i, symbol := range cfg.Symbols {
		streams[i] = fmt.Sprintf("%s@trade", strings.ToLower(symbol))
	}

	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     1,
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error subscribing to streams: %w", err)
	}

	// Read subscription response with timeout
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var response map[string]interface{}
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error reading subscription response: %w", err)
	}

	// Reset read deadline
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error resetting read deadline: %w", err)
	}

	logger.Info("Successfully subscribed to trade streams", zap.Any("response", response))
	return conn, nil
}

// Функция для создания нового батча
func createNewBatch(ctx context.Context, conn driver.Conn, tableName string) (driver.Batch, error) {
	return conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
}

func processTrades(ctx context.Context, conn driver.Conn, cfg *Config, logger *zap.Logger) error {
	logger.Info("Starting trade processing",
		zap.String("ws_url", cfg.BinanceWSURL),
		zap.Strings("symbols", cfg.Symbols),
		zap.Int("batch_size", cfg.BatchSize),
		zap.String("clickhouse_table", cfg.ClickHouseTable))

	// Проверяем существование таблицы
	var count uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", cfg.ClickHouseTable)).Scan(&count); err != nil {
		logger.Error("Error checking table existence", zap.Error(err))
	} else {
		logger.Info("Current table row count", zap.Uint64("count", count))
	}

	// Создаем тестовую запись для проверки
	testBatch, err := createNewBatch(ctx, conn, cfg.ClickHouseTable)
	if err != nil {
		logger.Error("Failed to create test batch", zap.Error(err))
	} else {
		if err := testBatch.Append(
			"TEST",
			"1234.56780000",
			"0.12340000",
			time.Now(),
		); err != nil {
			logger.Error("Failed to append test record", zap.Error(err))
		} else {
			if err := testBatch.Send(); err != nil {
				logger.Error("Failed to send test batch", zap.Error(err))
			} else {
				logger.Info("Test record inserted successfully")
			}
		}
	}

	var ws *websocket.Conn
	var reconnectDelay time.Duration = time.Second

	// Функция для переподключения к WebSocket
	reconnect := func() error {
		if ws != nil {
			ws.Close()
		}
		var err error
		ws, err = connectWebSocket(cfg, logger)
		if err != nil {
			return fmt.Errorf("failed to reconnect to WebSocket: %w", err)
		}
		reconnectDelay = time.Second // Reset delay on successful connection
		return nil
	}

	// Initial connection
	if err := reconnect(); err != nil {
		return err
	}

	// Создаем новый батч
	batch, err := createNewBatch(ctx, conn, cfg.ClickHouseTable)
	if err != nil {
		return fmt.Errorf("error preparing initial batch: %w", err)
	}

	// Таймер для принудительной отправки батча
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Канал для обработки ошибок
	errChan := make(chan error, 1)

	// Мьютекс для синхронизации доступа к батчу
	var batchMutex sync.Mutex

	// Счетчики для метрик
	var (
		totalTrades      int64
		successfulTrades int64
		failedTrades     int64
		lastLogTime      = time.Now()
		currentRows      = 0
	)

	// Функция для отправки батча
	sendBatch := func(b driver.Batch) error {
		if b == nil {
			return nil
		}
		if b.Rows() > 0 {
			logger.Info("Sending batch",
				zap.Int("rows", b.Rows()),
				zap.String("table", cfg.ClickHouseTable))

			if err := b.Send(); err != nil {
				logger.Error("Failed to send batch",
					zap.Error(err),
					zap.Int("rows", b.Rows()),
					zap.String("table", cfg.ClickHouseTable),
					zap.Int64("total_trades", totalTrades),
					zap.Int64("successful_trades", successfulTrades),
					zap.Int64("failed_trades", failedTrades))
				return fmt.Errorf("error sending batch: %w", err)
			}
			logger.Info("Batch sent successfully",
				zap.Int("rows", b.Rows()),
				zap.String("table", cfg.ClickHouseTable),
				zap.Int64("total_trades", totalTrades))
		}
		return nil
	}

	// Горутина для обработки сообщений WebSocket
	go func() {
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				errChan <- fmt.Errorf("error reading from WebSocket: %w", err)
				return
			}

			// Выводим сырые данные
			logger.Info("Raw WebSocket message",
				zap.String("message", string(message)),
				zap.Time("received_at", time.Now()))

			var trade Trade
			if err := json.Unmarshal(message, &trade); err != nil {
				logger.Error("Failed to unmarshal trade data",
					zap.Error(err),
					zap.String("message", string(message)))
				continue
			}

			batchMutex.Lock()
			if err := batch.Append(
				trade.Symbol,
				trade.Price,
				trade.Quantity,
				trade.TradeTime,
			); err != nil {
				failedTrades++
				logger.Error("Error appending trade to batch",
					zap.Error(err),
					zap.String("symbol", trade.Symbol),
					zap.String("price", trade.Price),
					zap.String("quantity", trade.Quantity),
					zap.Time("trade_time", trade.TradeTime),
					zap.Int64("total_trades", totalTrades),
					zap.Int64("failed_trades", failedTrades))

				// Если ошибка связана с тем, что батч уже отправлен, создаем новый
				if strings.Contains(err.Error(), "batch has already been sent") {
					var newBatchErr error
					batch, newBatchErr = createNewBatch(ctx, conn, cfg.ClickHouseTable)
					if newBatchErr != nil {
						logger.Error("Failed to create new batch after send",
							zap.Error(newBatchErr))
						batchMutex.Unlock()
						continue
					}
					// Повторяем попытку добавления записи в новый батч
					if err := batch.Append(
						trade.Symbol,
						trade.Price,
						trade.Quantity,
						trade.TradeTime,
					); err != nil {
						logger.Error("Failed to append trade to new batch",
							zap.Error(err))
						batchMutex.Unlock()
						continue
					}
				} else {
					batchMutex.Unlock()
					continue
				}
			}
			currentRows++
			totalTrades++
			successfulTrades++

			// Если батч заполнен, отправляем его
			if currentRows >= cfg.BatchSize {
				if err := sendBatch(batch); err != nil {
					logger.Error("Failed to send full batch", zap.Error(err))
				}
				var newBatchErr error
				batch, newBatchErr = createNewBatch(ctx, conn, cfg.ClickHouseTable)
				if newBatchErr != nil {
					logger.Error("Failed to create new batch after full batch",
						zap.Error(newBatchErr))
				}
				currentRows = 0
			}
			batchMutex.Unlock()

			// Логируем метрики каждые 5 секунд
			if time.Since(lastLogTime) > 5*time.Second {
				logger.Info("Processing metrics",
					zap.Int64("total_trades", totalTrades),
					zap.Int64("successful_trades", successfulTrades),
					zap.Int64("failed_trades", failedTrades))
				lastLogTime = time.Now()
			}
		}
	}()

	// Основной цикл обработки
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			logger.Error("WebSocket error", zap.Error(err))
			time.Sleep(reconnectDelay)
			reconnectDelay *= 2 // exponential backoff
			if reconnectDelay > 1*time.Minute {
				reconnectDelay = 1 * time.Minute
			}
			if err := reconnect(); err != nil {
				return err
			}
		case <-ticker.C:
			batchMutex.Lock()
			if currentRows > 0 {
				if err := sendBatch(batch); err != nil {
					logger.Error("Failed to send partial batch", zap.Error(err))
				}
				batch, err = createNewBatch(ctx, conn, cfg.ClickHouseTable)
				if err != nil {
					logger.Error("Failed to create new batch after partial send",
						zap.Error(err))
				}
				currentRows = 0
			}
			batchMutex.Unlock()
		}
	}
}

func createTableIfNotExists(ctx context.Context, conn driver.Conn, cfg *Config) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			symbol LowCardinality(String),
			price String,
			quantity String,
			trade_time DateTime64(3)
		) ENGINE = MergeTree()
		ORDER BY (trade_time, symbol)
		PARTITION BY toYYYYMM(trade_time)
	`, cfg.ClickHouseTable)

	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	// Проверяем, что таблица создана
	var count uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", cfg.ClickHouseTable)).Scan(&count); err != nil {
		return fmt.Errorf("error checking table existence: %w", err)
	}

	return nil
}

func dropTableIfExists(ctx context.Context, conn driver.Conn, cfg *Config) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", cfg.ClickHouseTable)
	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("error dropping table: %w", err)
	}
	return nil
}

func main() {
	// Инициализация логгера
	if err := logger.Init(); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Создание логгера zap
	zapConfig := zap.NewProductionConfig()
	if cfg.LogLevel != "" {
		level := zap.NewAtomicLevel()
		if err := level.UnmarshalText([]byte(cfg.LogLevel)); err == nil {
			zapConfig.Level = level
		}
	}
	logger, err := zapConfig.Build()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Connect to ClickHouse
	conn, err := connectClickHouse(cfg)
	if err != nil {
		logger.Fatal("Failed to connect to ClickHouse", zap.Error(err))
	}
	defer conn.Close()

	// Drop existing table
	if err := dropTableIfExists(context.Background(), conn, cfg); err != nil {
		logger.Fatal("Failed to drop table", zap.Error(err))
	}

	// Create table if not exists
	if err := createTableIfNotExists(context.Background(), conn, cfg); err != nil {
		logger.Fatal("Failed to create table", zap.Error(err))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processing trades
	if err := processTrades(ctx, conn, cfg, logger); err != nil {
		logger.Fatal("Error processing trades", zap.Error(err))
	}
}
