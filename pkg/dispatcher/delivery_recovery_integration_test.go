/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dispatcher

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Run against a disposable queue on a broker whose dead-letter retention policy
// covers t.statsapi-live-backend.*. No existing queue is read or modified.
func TestQuorumDeadLetterRetentionAndRecovery(t *testing.T) {
	uri := os.Getenv("DELIVERY_TEST_AMQP_URL")
	if uri == "" {
		t.Skip("set DELIVERY_TEST_AMQP_URL to test quorum retention")
	}
	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatal("cannot connect to test broker")
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	name := fmt.Sprintf("t.statsapi-live-backend.delivery-recovery-test-%d", time.Now().UnixNano())
	_, err = ch.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(name, false, false, false)
	if err = ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer ch.ExchangeDelete(name, false, false)
	if err = ch.QueueBind(name, "", name, false, nil); err != nil {
		t.Fatal(err)
	}
	if err = ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	msg := recoveryMessage(nil)
	confirmation, err := ch.PublishWithDeferredConfirm("", name, true, false, amqp.Publishing{ContentType: msg.ContentType, Body: msg.Body, DeliveryMode: amqp.Persistent})
	if err != nil || !confirmation.Wait() {
		t.Fatalf("publish: %v", err)
	}
	if err = ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}
	deliveries, err := ch.Consume(name, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	next := func() amqp.Delivery {
		t.Helper()
		select {
		case m := <-deliveries:
			return m
		case <-time.After(10 * time.Second):
			t.Fatal("message disappeared before acceptance")
			return amqp.Delivery{}
		}
	}
	d := recoveryDispatcher(t)
	d.DLX = true
	for i := 0; i < 25; i++ {
		m := next()
		if err = d.dispatchDLQ(context.Background(), m, recoveryClient{result: recoveryResult(503)}); err == nil {
			t.Fatal("expected sink failure")
		}
	}
	m := next()
	t.Logf("event retained after 25 failed deliveries; RabbitMQ delivery count: %v", m.Headers["x-delivery-count"])
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Ce-Id") != "retained-event" {
			t.Errorf("unexpected event ID %q", r.Header.Get("Ce-Id"))
		}
		if attempts.Add(1) <= 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	d.SubscriberURL = server.URL
	client, err := cloudevents.NewClientHTTP(cehttp.WithIsRetriableFunc(func(status int) bool { return d.isRetriableStatus(context.Background(), status) }))
	if err != nil {
		t.Fatal(err)
	}
	if err = d.dispatchDLQ(context.Background(), m, client); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("HTTP attempts=%d, want 3", attempts.Load())
	}
	// A synchronous request on the same channel orders after the delivery ack.
	q, err := ch.QueueInspect(name)
	if err != nil || q.Messages != 0 {
		t.Fatalf("queue after recovery: %+v, %v", q, err)
	}
	t.Log("sink recovered after two HTTP 503s; event acknowledged and queue drained")
	// The normal dispatcher must confirm publication before acknowledging its input.
	a := &recoveryAcknowledger{}
	d.DLXName = name
	if err = d.dispatch(context.Background(), recoveryMessage(a), recoveryClient{result: recoveryResult(503)}, ch); err == nil {
		t.Fatal("expected subscriber failure")
	}
	if a.acks != 1 || a.nacks != 0 {
		t.Fatalf("ack=%d nack=%d", a.acks, a.nacks)
	}
	forwarded := next()
	if forwarded.MessageId != "retained-event" || fmt.Sprint(forwarded.Headers["knativeerrorcode"]) != "503" {
		t.Fatalf("unexpected forwarded event ID/status: %s/%v", forwarded.MessageId, forwarded.Headers["knativeerrorcode"])
	}
	if err = forwarded.Ack(false); err != nil {
		t.Fatal(err)
	}
	t.Log("normal dispatcher confirmed dead-letter publication before acknowledging")
}

func TestDeadLetterHTTPRetriesAreBounded(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); w.WriteHeader(503) }))
	defer server.Close()
	d := recoveryDispatcher(t)
	d.DLX = true
	d.SubscriberURL = server.URL
	client, err := cloudevents.NewClientHTTP(cehttp.WithIsRetriableFunc(func(status int) bool { return d.isRetriableStatus(context.Background(), status) }))
	if err != nil {
		t.Fatal(err)
	}
	a := &recoveryAcknowledger{}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	err = d.dispatchDLQ(ctx, recoveryMessage(a), client)
	if err == nil || attempts.Load() != 4 || a.acks != 0 || a.nacks != 1 {
		t.Fatalf("err=%v attempts=%d ack=%d nack=%d", err, attempts.Load(), a.acks, a.nacks)
	}
}
