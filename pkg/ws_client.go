package pkg

import (
	"context"
	"encoding/json"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/gorilla/websocket"
	"net/url"
	"time"
)

type LogOutput struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

type wsClient struct {
	// construct field
	Schema       string
	Host         string
	Path         string
	PingInterval int
	MaxLen       int

	stats outputs.Observer
	conn  *websocket.Conn
}

func (w *wsClient) String() string {
	return "websocket"
}

func (w *wsClient) Connect() error {
	u := url.URL{Scheme: w.Schema, Host: w.Host, Path: w.Path}
	dial, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err == nil {
		w.conn = dial
		ticker := time.NewTicker(time.Duration(w.PingInterval) * time.Second)
		go func() {
			for range ticker.C {
				w.conn.WriteMessage(websocket.PingMessage, nil)
			}
		}()
	} else {
		time.Sleep(10 * time.Second)
	}
	return err
}

func (w *wsClient) Close() error {
	return w.conn.Close()
}

func (w *wsClient) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	// 记录这批日志
	w.stats.NewBatch(len(events))
	failEvents, err := w.PublishEvents(events)
	if err != nil {
		// 如果发送正常，则ACK
		batch.ACK()
	} else {
		// 发送失败，则重试。受RetryLimit的限制
		batch.RetryEvents(failEvents)
	}
	return err
}

func (w *wsClient) PublishEvents(events []publisher.Event) ([]publisher.Event, error) {
	for i, event := range events {
		err := w.publishEvent(&event)
		if err != nil {
			// 如果单条消息发送失败，则将剩余的消息直接重试
			return events[i:], err
		}
	}
	return nil, nil
}

func (w *wsClient) publishEvent(event *publisher.Event) error {
	bytes, err := w.encode(&event.Content)
	if err != nil {
		// 如果编码失败，就不重试了，重试也不会成功
		// encode error, don't retry.
		// consider being success
		return nil
	}
	err = w.conn.WriteMessage(websocket.TextMessage, bytes)
	if err != nil {
		// 写入WebSocket Server失败
		return err
	}
	return nil
}

func (w *wsClient) encode(event *beat.Event) ([]byte, error) {
	logOutput := &LogOutput{}
	value, err := event.Fields.GetValue("message")
	if err != nil {
		return nil, err
	}
	s := value.(string)
	if len(s) >= w.MaxLen {
		return nil, err
	}
	logOutput.Timestamp = event.Timestamp
	logOutput.Message = s
	return json.Marshal(logOutput)
}
