package amqp_consumer

import (
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/testutil"

	_ "net/http/pprof"
)

func TestAutoEncoding(t *testing.T) {
	enc, err := internal.NewGzipEncoder()
	require.NoError(t, err)
	payload, err := enc.Encode([]byte(`measurementName fieldKey="gzip" 1556813561098000000`))
	require.NoError(t, err)

	var a AMQPConsumer
	parser := &influx.Parser{}
	require.NoError(t, parser.Init())
	a.deliveries = make(map[telegraf.TrackingID]amqp091.Delivery)
	a.parser = parser
	a.decoder, err = internal.NewContentDecoder("auto")
	require.NoError(t, err)

	acc := &testutil.Accumulator{}

	d := amqp091.Delivery{
		ContentEncoding: "gzip",
		Body:            payload,
	}
	err = a.onMessage(acc, d)
	require.NoError(t, err)
	acc.AssertContainsFields(t, "measurementName", map[string]interface{}{"fieldKey": "gzip"})

	encIdentity, err := internal.NewIdentityEncoder()
	require.NoError(t, err)
	payload, err = encIdentity.Encode([]byte(`measurementName2 fieldKey="identity" 1556813561098000000`))
	require.NoError(t, err)

	d = amqp091.Delivery{
		ContentEncoding: "not_gzip",
		Body:            payload,
	}

	err = a.onMessage(acc, d)
	require.NoError(t, err)
	acc.AssertContainsFields(t, "measurementName2", map[string]interface{}{"fieldKey": "identity"})
}

func TestQueueConsume(t *testing.T) {
	// pprof
	go http.ListenAndServe(":80", nil)

	rmq := AMQPConsumer{
		Brokers: []string{
			"amqp://username:password@127.0.0.1:5672",
		},
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		Queue:        "test_queue",
		//QueuePassive:              true,
		QueueConsumeCheck:         true,
		QueueConsumeCheckInterval: time.Second * 10,
		BindingKey:                "#",
		Log: testutil.Logger{
			Name: "amqp_consumer",
		},
	}

	if err := rmq.Start(new(ac)); err != nil {
		t.Fatal(err)
	}

	t.Log("waiting...")

	select {}
	// delete the "test_queue" queue to test checkQueueConsume.
	// test result:
	// 		Errorf Error inspecting queue: Exception (404) Reason: "NOT_FOUND - no queue 'test_queue' in vhost '/'"
	// 		Errorf Error inspect queue test_queue: no consumers
	// 		panic: QueueConsumeCheckFailCallback test_queue
}

type ac struct{}

func (a ac) AddFields(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (a ac) AddGauge(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (a ac) AddCounter(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (a ac) AddSummary(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (a ac) AddHistogram(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (a ac) AddMetric(_ telegraf.Metric) {}

func (a ac) SetPrecision(_ time.Duration) {}

func (a ac) AddError(_ error) {}

func (a ac) WithTracking(_ int) telegraf.TrackingAccumulator {
	return tkAc{}
}

type tkAc struct{}

func (t2 tkAc) AddFields(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (t2 tkAc) AddGauge(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (t2 tkAc) AddCounter(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (t2 tkAc) AddSummary(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {}

func (t2 tkAc) AddHistogram(_ string, _ map[string]interface{}, _ map[string]string, _ ...time.Time) {
}

func (t2 tkAc) AddMetric(_ telegraf.Metric) {}

func (t2 tkAc) SetPrecision(_ time.Duration) {}

func (t2 tkAc) AddError(_ error) {}

func (t2 tkAc) WithTracking(_ int) telegraf.TrackingAccumulator {
	return nil
}

func (t2 tkAc) AddTrackingMetric(_ telegraf.Metric) telegraf.TrackingID {
	return 0
}

func (t2 tkAc) AddTrackingMetricGroup(_ []telegraf.Metric) telegraf.TrackingID {
	return 0
}

func (t2 tkAc) Delivered() <-chan telegraf.DeliveryInfo {
	return nil
}
