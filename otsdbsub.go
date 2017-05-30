package openTSDBSubmitter

import (
	"bosun.org/cmd/scollector/collectors"
	"bosun.org/collect"
	"bosun.org/opentsdb"
	"bosun.org/slog"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type Client struct {
	opentsdbAddr string
	updateChan   chan *opentsdb.DataPoint
}

type Metric struct {
	collectors.MetricMeta
	client *Client
}

func (c *Client) NewMetric(m *collectors.MetricMeta) *Metric {
	r := &Metric{
		MetricMeta: *m,
		client:     c,
	}
	for k, v := range r.TagSet {
		r.TagSet[k] = opentsdb.MustReplace(v, "_")
	}
	r.Metric = opentsdb.MustReplace(r.Metric, "_")
	return r
}

func (m *Metric) Submit(value interface{}, timestamp time.Time) {
	if m.client.opentsdbAddr == "" {
		return
	}
	var rv interface{}
	switch v := value.(type) {
	case bool:
		if v {
			rv = int(1)
		} else {
			rv = int(0)
		}
	case time.Duration:
		rv = v.Seconds() * 1e3
	}

	var md opentsdb.MultiDataPoint
	if timestamp.IsZero() {
		collectors.Add(&md, m.Metric, rv, m.TagSet, m.RateType, m.Unit, m.Desc)
	} else {
		ts := timestamp.UnixNano() / time.Millisecond.Nanoseconds()
		collectors.AddTS(&md, m.Metric, ts, rv, m.TagSet, m.RateType, m.Unit, m.Desc)
	}
	for _, d := range md {
		go func(d *opentsdb.DataPoint) {
			m.client.updateChan <- d
		}(d)
	}
}

func NewClient(host string) *Client {
	b := &Client{
		opentsdbAddr: host,
		updateChan:   make(chan *opentsdb.DataPoint),
	}
	if b.opentsdbAddr == "" {
		return b
	}
	go func() {
		t := time.NewTicker(3 * time.Second)
		dps := []*opentsdb.DataPoint{}
		for {
			select {
			case dp := <-b.updateChan:
				dps = append(dps, dp)
				if len(dps) < 256 {
					continue
				}
			case <-t.C:
				if len(dps) <= 0 {
					continue
				}
			}
			err := sendDataPoints(dps, b.opentsdbAddr)
			if err != nil {
				slog.Errorf("Error sending data to opentsdb: %v", err)
				continue
			}
			dps = []*opentsdb.DataPoint{}
		}
	}()
	return b
}

func sendDataPoints(dps []*opentsdb.DataPoint, addr string) error {
	resp, err := collect.SendDataPoints(dps, addr)
	if err == nil {
		defer resp.Body.Close()
		defer io.Copy(ioutil.Discard, resp.Body)
	}
	if err != nil {
		return err
	}
	// Some problem with connecting to the server; retry later.
	if resp.StatusCode != http.StatusNoContent {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		if resp.StatusCode == 400 && len(dps) > 1 { // bad datapoint, try each independently
			for _, d := range dps {
				err = sendDataPoints([]*opentsdb.DataPoint{d}, addr)
				if err != nil {
					slog.Errorf("bad opentsdb datapoint: %v", d)
				}
			}
			return nil
		}
		return fmt.Errorf("bad status from opentsdb")
	}
	return nil
}
