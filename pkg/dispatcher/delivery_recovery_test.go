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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"knative.dev/eventing-rabbitmq/pkg/rabbit"
)

type recoveryAcknowledger struct {
	acks, nacks int
	nackErr     error
	formatted   bool
}

func (a *recoveryAcknowledger) Ack(_ uint64, multiple bool) error {
	if multiple {
		panic("multiple ack")
	}
	a.acks++
	return nil
}
func (a *recoveryAcknowledger) Nack(_ uint64, multiple, requeue bool) error {
	if multiple || !requeue {
		panic("discarded delivery")
	}
	a.nacks++
	return a.nackErr
}
func (a *recoveryAcknowledger) Reject(uint64, bool) error { panic("unexpected reject") }

// Formatting a delivery traverses its acknowledger, including a live AMQP channel.
func (a *recoveryAcknowledger) Format(fmt.State, rune) { a.formatted = true }

type recoveryClient struct {
	cloudevents.Client
	response           *cloudevents.Event
	result, sendResult protocol.Result
}

func (c recoveryClient) Request(context.Context, cloudevents.Event) (*cloudevents.Event, protocol.Result) {
	return c.response, c.result
}
func (c recoveryClient) Send(context.Context, cloudevents.Event) protocol.Result { return c.sendResult }
func recoveryResult(status int) protocol.Result {
	return cehttp.NewRetriesResult(cehttp.NewResult(status, ""), 0, time.Now(), nil)
}
func recoveryDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	h, err := noop.NewMeterProvider().Meter("test").Float64Histogram("dispatch")
	if err != nil {
		t.Fatal(err)
	}
	return &Dispatcher{SubscriberURL: "http://sink", BrokerIngressURL: "http://broker", DispatchDuration: h, Tracer: tracenoop.NewTracerProvider().Tracer("test")}
}
func recoveryMessage(a amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{Acknowledger: a, DeliveryTag: 42, ContentType: "application/cloudevents+json", Body: []byte(`{"specversion":"1.0","source":"delivery-test","id":"retained-event","type":"test.delivery"}`)}
}
func TestDeadLetterRetainsUnacceptedEvents(t *testing.T) {
	for _, status := range []int{200, 204, 302, 400, 401, 404, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			d := recoveryDispatcher(t)
			a := &recoveryAcknowledger{}
			err := d.dispatchDLQ(context.Background(), recoveryMessage(a), recoveryClient{result: recoveryResult(status)})
			success := status >= 200 && status < 300
			if success && (err != nil || a.acks != 1 || a.nacks != 0) {
				t.Fatalf("accepted: err=%v ack=%d nack=%d", err, a.acks, a.nacks)
			}
			if !success && (err == nil || a.acks != 0 || a.nacks != 1) {
				t.Fatalf("failed: err=%v ack=%d nack=%d", err, a.acks, a.nacks)
			}
			if a.formatted {
				t.Fatal("failure logger formatted the live delivery")
			}
		})
	}
}
func TestDeadLetterRetainsTransportFailureAndResponseFailure(t *testing.T) {
	response := cloudevents.NewEvent()
	response.SetID("response")
	response.SetSource("test")
	response.SetType("test")
	for name, c := range map[string]recoveryClient{
		"connection reset":     {result: errors.New("connection reset")},
		"invalid result":       {result: nil},
		"response rejected":    {result: recoveryResult(200), response: &response, sendResult: recoveryResult(400)},
		"response unavailable": {result: recoveryResult(200), response: &response, sendResult: recoveryResult(503)},
	} {
		t.Run(name, func(t *testing.T) {
			a := &recoveryAcknowledger{}
			err := recoveryDispatcher(t).dispatchDLQ(context.Background(), recoveryMessage(a), c)
			if err == nil || a.acks != 0 || a.nacks != 1 {
				t.Fatalf("err=%v ack=%d nack=%d", err, a.acks, a.nacks)
			}
		})
	}
}
func TestDeadLetterRecoveryAcknowledgesOnlyAfterAcceptance(t *testing.T) {
	d := recoveryDispatcher(t)
	a := &recoveryAcknowledger{}
	msg := recoveryMessage(a)
	for range 25 {
		if err := d.dispatchDLQ(context.Background(), msg, recoveryClient{result: recoveryResult(503)}); err == nil {
			t.Fatal("expected failure")
		}
	}
	if a.acks != 0 || a.nacks != 25 {
		t.Fatalf("ack=%d nack=%d", a.acks, a.nacks)
	}
	if err := d.dispatchDLQ(context.Background(), msg, recoveryClient{result: recoveryResult(204)}); err != nil {
		t.Fatal(err)
	}
	if a.acks != 1 || a.nacks != 25 {
		t.Fatalf("ack=%d nack=%d", a.acks, a.nacks)
	}
}
func TestDeadLetterReportsNackFailure(t *testing.T) {
	a := &recoveryAcknowledger{nackErr: errors.New("channel closed")}
	err := recoveryDispatcher(t).dispatchDLQ(context.Background(), recoveryMessage(a), recoveryClient{result: recoveryResult(503)})
	if err == nil || !strings.Contains(err.Error(), "channel closed") || a.acks != 0 {
		t.Fatalf("err=%v ack=%d", err, a.acks)
	}
}

type failedPublishChannel struct {
	rabbit.RabbitMQChannelMock
	ack         *recoveryAcknowledger
	published   bool
	destination string
	status      interface{}
}

func (c *failedPublishChannel) PublishWithDeferredConfirm(_ string, _ string, _, _ bool, p amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if c.ack.acks != 0 {
		panic("acknowledged before publication")
	}
	c.published = true
	c.destination, _ = p.Headers["knativeerrordest"].(string)
	c.status = p.Headers["knativeerrorcode"]
	return nil, errors.New("broker unavailable")
}
func TestFailedDeadLetterPublicationRequeuesOriginal(t *testing.T) {
	for _, responseFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(responseFailure), func(t *testing.T) {
			a := &recoveryAcknowledger{}
			channel := &failedPublishChannel{ack: a}
			d := recoveryDispatcher(t)
			d.DLXName = "dead-letter"
			c := recoveryClient{result: recoveryResult(503)}
			if responseFailure {
				e := cloudevents.NewEvent()
				c = recoveryClient{result: recoveryResult(200), response: &e, sendResult: recoveryResult(503)}
			}
			err := d.dispatch(context.Background(), recoveryMessage(a), c, channel)
			wantDestination := d.SubscriberURL
			if responseFailure {
				wantDestination = d.BrokerIngressURL
			}
			if channel.destination != wantDestination || fmt.Sprint(channel.status) != "503" {
				t.Fatalf("failure attribution: destination=%s status=%v", channel.destination, channel.status)
			}
			if err == nil || !channel.published || a.acks != 0 || a.nacks != 1 {
				t.Fatalf("err=%v published=%v ack=%d nack=%d", err, channel.published, a.acks, a.nacks)
			}
		})
	}
}
