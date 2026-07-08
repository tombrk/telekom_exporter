package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const defaultTelekomURL = "https://pass.telekom.de/api/service/generic/v1/status"

type TelekomStatus struct {
	InitialVolume    float64  `json:"initialVolume"`
	InitialVolumeStr string   `json:"initialVolumeStr"`
	NextUpdate       float64  `json:"nextUpdate"`
	PassName         string   `json:"passName"`
	PassStage        float64  `json:"passStage"`
	PassType         float64  `json:"passType"`
	RemainingSeconds float64  `json:"remainingSeconds"`
	RemainingTimeStr string   `json:"remainingTimeStr"`
	SessionState     float64  `json:"sessionState"`
	Subscriptions    []string `json:"subscriptions"`
	Title            string   `json:"title"`
	UsedAt           float64  `json:"usedAt"`
	UsedPercentage   float64  `json:"usedPercentage"`
	UsedVolume       float64  `json:"usedVolume"`
	UsedVolumeStr    string   `json:"usedVolumeStr"`
	ValidityPeriod   float64  `json:"validityPeriod"`
}

type TelekomCollector struct {
	client *http.Client
	url    string

	up                   *prometheus.Desc
	scrapeDuration       *prometheus.Desc
	initialBytes         *prometheus.Desc
	usedBytes            *prometheus.Desc
	remainingBytes       *prometheus.Desc
	usedPercent          *prometheus.Desc
	remainingSeconds     *prometheus.Desc
	nextUpdateSeconds    *prometheus.Desc
	usedAtTimestamp      *prometheus.Desc
	passStage            *prometheus.Desc
	passType             *prometheus.Desc
	sessionState         *prometheus.Desc
	validityPeriod       *prometheus.Desc
	info                 *prometheus.Desc
}

func NewTelekomCollector(url string) *TelekomCollector {
	labels := []string{"pass_name", "pass_type", "validity_period"}

	return &TelekomCollector{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		url: url,

		up: prometheus.NewDesc(
			"telekom_exporter_up",
			"Whether the Telekom status endpoint scrape succeeded.",
			nil,
			nil,
		),
		scrapeDuration: prometheus.NewDesc(
			"telekom_exporter_scrape_duration_seconds",
			"Duration of the Telekom status endpoint scrape.",
			nil,
			nil,
		),

		initialBytes: prometheus.NewDesc(
			"telekom_data_initial_bytes",
			"Initial included data volume in bytes.",
			labels,
			nil,
		),
		usedBytes: prometheus.NewDesc(
			"telekom_data_used_bytes",
			"Used data volume in bytes.",
			labels,
			nil,
		),
		remainingBytes: prometheus.NewDesc(
			"telekom_data_remaining_bytes",
			"Remaining data volume in bytes.",
			labels,
			nil,
		),
		usedPercent: prometheus.NewDesc(
			"telekom_data_used_percent",
			"Used data volume percentage.",
			labels,
			nil,
		),
		remainingSeconds: prometheus.NewDesc(
			"telekom_pass_remaining_seconds",
			"Remaining seconds until the current pass or billing period expires.",
			labels,
			nil,
		),
		nextUpdateSeconds: prometheus.NewDesc(
			"telekom_pass_next_update_seconds",
			"Seconds until Telekom says the status should be updated again.",
			labels,
			nil,
		),
		usedAtTimestamp: prometheus.NewDesc(
			"telekom_status_used_at_timestamp_seconds",
			"Unix timestamp of the Telekom usage data.",
			labels,
			nil,
		),
		passStage: prometheus.NewDesc(
			"telekom_pass_stage",
			"Telekom pass stage code.",
			labels,
			nil,
		),
		passType: prometheus.NewDesc(
			"telekom_pass_type",
			"Telekom pass type code.",
			labels,
			nil,
		),
		sessionState: prometheus.NewDesc(
			"telekom_session_state",
			"Telekom session state code.",
			labels,
			nil,
		),
		validityPeriod: prometheus.NewDesc(
			"telekom_validity_period",
			"Telekom validity period code.",
			labels,
			nil,
		),
		info: prometheus.NewDesc(
			"telekom_info",
			"Telekom pass metadata.",
			[]string{
				"pass_name",
				"pass_type",
				"validity_period",
				"initial_volume",
				"used_volume",
				"subscriptions",
				"title",
			},
			nil,
		),
	}
}

func (c *TelekomCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.scrapeDuration
	ch <- c.initialBytes
	ch <- c.usedBytes
	ch <- c.remainingBytes
	ch <- c.usedPercent
	ch <- c.remainingSeconds
	ch <- c.nextUpdateSeconds
	ch <- c.usedAtTimestamp
	ch <- c.passStage
	ch <- c.passType
	ch <- c.sessionState
	ch <- c.validityPeriod
	ch <- c.info
}

func (c *TelekomCollector) Collect(ch chan<- prometheus.Metric) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status, err := c.fetch(ctx)

	duration := time.Since(start).Seconds()
	ch <- prometheus.MustNewConstMetric(c.scrapeDuration, prometheus.GaugeValue, duration)

	if err != nil {
		log.Printf("telekom scrape failed: %v", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)

	remaining := status.InitialVolume - status.UsedVolume
	if remaining < 0 {
		remaining = 0
	}

	labelValues := []string{
		status.PassName,
		fmt.Sprintf("%.0f", status.PassType),
		fmt.Sprintf("%.0f", status.ValidityPeriod),
	}

	usedAtSeconds := status.UsedAt / 1000.0

	ch <- prometheus.MustNewConstMetric(c.initialBytes, prometheus.GaugeValue, status.InitialVolume, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.usedBytes, prometheus.GaugeValue, status.UsedVolume, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.remainingBytes, prometheus.GaugeValue, remaining, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.usedPercent, prometheus.GaugeValue, status.UsedPercentage, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.remainingSeconds, prometheus.GaugeValue, status.RemainingSeconds, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.nextUpdateSeconds, prometheus.GaugeValue, status.NextUpdate, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.usedAtTimestamp, prometheus.GaugeValue, usedAtSeconds, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.passStage, prometheus.GaugeValue, status.PassStage, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.passType, prometheus.GaugeValue, status.PassType, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.sessionState, prometheus.GaugeValue, status.SessionState, labelValues...)
	ch <- prometheus.MustNewConstMetric(c.validityPeriod, prometheus.GaugeValue, status.ValidityPeriod, labelValues...)

	ch <- prometheus.MustNewConstMetric(
		c.info,
		prometheus.GaugeValue,
		1,
		status.PassName,
		fmt.Sprintf("%.0f", status.PassType),
		fmt.Sprintf("%.0f", status.ValidityPeriod),
		status.InitialVolumeStr,
		status.UsedVolumeStr,
		strings.Join(status.Subscriptions, ","),
		status.Title,
	)
}

func (c *TelekomCollector) fetch(ctx context.Context) (*TelekomStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "telekom-exporter/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	var status TelekomStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

func main() {
	listenAddr := getenv("LISTEN_ADDR", ":9108")
	telekomURL := getenv("TELEKOM_STATUS_URL", defaultTelekomURL)

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewTelekomCollector(telekomURL))

	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("telekom_exporter\n\nmetrics: /metrics\nhealth:  /healthz\n"))
	})

	log.Printf("listening on %s", listenAddr)
	log.Printf("telekom status url: %s", telekomURL)

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}

func getenv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
