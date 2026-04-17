package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/udhos/pulsix/inject"
)

func TestHandleMessagesDurableAck(t *testing.T) {
	t.Parallel()

	tracker := newReceiptTracker()
	stats := &ingressStats{}
	injectCh := make(chan inject.InjectMessage, 1)
	app := newIngressHTTP(context.Background(), injectCh, tracker, stats)

	done := make(chan struct{})
	go func() {
		msg := <-injectCh
		if string(msg.Data) != "hello" {
			t.Errorf("unexpected payload: %q", msg.Data)
		}
		stats.Acked.Add(1)
		tracker.ack(msg.Receipt)
		close(done)
	}()

	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader("hello"))
	rec := httptest.NewRecorder()

	app.handleMessages(rec, req)
	<-done

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	var body responseBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if !body.Durable {
		t.Fatalf("expected durable response: %+v", body)
	}
	if body.Bytes != 5 {
		t.Fatalf("expected bytes=5 got=%d", body.Bytes)
	}
	if stats.Requests.Load() != 1 || stats.Accepted.Load() != 1 || stats.Acked.Load() != 1 {
		t.Fatalf("unexpected stats: requests=%d accepted=%d acked=%d",
			stats.Requests.Load(),
			stats.Accepted.Load(),
			stats.Acked.Load(),
		)
	}
}

func TestHandleHealthUnhealthy(t *testing.T) {
	t.Parallel()

	tracker := newReceiptTracker()
	tracker.fail(context.DeadlineExceeded)
	stats := &ingressStats{}
	app := newIngressHTTP(context.Background(), make(chan inject.InjectMessage), tracker, stats)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	app.handleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected health body to include error: %s", rec.Body.String())
	}
}
