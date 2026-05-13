package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static/*
var staticFS embed.FS

const (
	defaultConfigFilePath = "config.json"
	defaultAlertsFilePath = "alerts.json"
	defaultAddr           = ":8080"
	defaultPollSeconds    = 5
	defaultBinanceBase    = "https://fapi.binance.com/fapi/v1"
)

// binanceRESTBase is Binance REST API root including version path; set once in main.
var binanceRESTBase string

// fileConfig mirrors config.json keys; empty fields leave prior values untouched when merging.
type fileConfig struct {
	TelegramBotToken string `json:"telegram_bot_token"`
	TelegramChatID   string `json:"telegram_chat_id"`
	ListenAddr       string `json:"listen_addr"`
	PollSeconds      *int   `json:"poll_seconds"`
	BinanceBase      string `json:"binance_base"`
	AlertsFile       string `json:"alerts_file"`
}

// AlertStep is one sequential condition in a chain; all steps must be satisfied in order before notify.
type AlertStep struct {
	Type                string  `json:"type,omitempty"` // "price" or "line"
	Target              float64 `json:"target,omitempty"`
	Direction           string  `json:"direction,omitempty"` // "above" or "below"
	LineStart           float64 `json:"line_start,omitempty"`
	LineEnd             float64 `json:"line_end,omitempty"`
	LineDurationSeconds int64   `json:"line_duration_seconds,omitempty"`
	// Diagonal line with absolute date coordinates
	LineDate1  int64   `json:"line_date1,omitempty"`  // Unix timestamp of point 1
	LinePrice1 float64 `json:"line_price1,omitempty"` // Price at point 1
	LineDate2  int64   `json:"line_date2,omitempty"`  // Unix timestamp of point 2
	LinePrice2 float64 `json:"line_price2,omitempty"` // Price at point 2
}

type Alert struct {
	ID            string      `json:"id"`
	Symbol        string      `json:"symbol"`
	Steps         []AlertStep `json:"steps"`
	StepIndex     int         `json:"step_index"` // index of step we are waiting for (0-based)
	CreatedAt     int64       `json:"created_at,omitempty"`
	LastTriggered int64       `json:"last_triggered,omitempty"` // Unix timestamp of last trigger
}

type alertStore struct {
	mu       sync.Mutex
	nextID   int
	items    []Alert
	filePath string // empty disables disk persistence
}

func newAlertStore(filePath string) (*alertStore, error) {
	trimmed := strings.TrimSpace(filePath)
	if trimmed == "" {
		return &alertStore{}, nil
	}
	s := &alertStore{filePath: filepath.Clean(trimmed)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *alertStore) persistLocked() {
	if s.filePath == "" {
		return
	}
	tmp := s.filePath + ".tmp"
	data, err := json.MarshalIndent(s.items, "", "  ")
	if err != nil {
		log.Printf("persist alerts: json: %v", err)
		return
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("persist alerts: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		log.Printf("persist alerts: rename to %s: %v", s.filePath, err)
		return
	}
}

func (s *alertStore) load() error {
	if s.filePath == "" {
		return nil
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Printf("alerts storage: no %s yet (will create on first change)", s.filePath)
			return nil
		}
		return fmt.Errorf("read alerts file %s: %w", s.filePath, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var items []Alert
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("parse alerts file %s: %w", s.filePath, err)
	}

	type cand struct {
		a   Alert
		raw string // trimmed alert id before fixes
	}

	var pending []cand
outer:
	for _, a := range items {
		if len(a.Steps) == 0 {
			log.Printf("alerts storage: skipping alert with no steps (id=%q)", a.ID)
			continue
		}
		for _, step := range a.Steps {
			if step.Target <= 0 || (step.Direction != "above" && step.Direction != "below") {
				log.Printf("alerts storage: skipping malformed chain (id=%q)", a.ID)
				continue outer
			}
		}
		if a.StepIndex < 0 {
			a.StepIndex = 0
		}
		if a.StepIndex > len(a.Steps) {
			log.Printf("alerts storage: skipping corrupted alert id=%q (step_index=%d)", a.ID, a.StepIndex)
			continue
		}
		if a.CreatedAt == 0 {
			a.CreatedAt = time.Now().Unix()
		}
		// StepIndex == len(steps) means chain is done but Telegram notify may still be pending.
		id := strings.TrimSpace(a.ID)
		if id != "" {
			if _, err := strconv.Atoi(id); err != nil {
				id = ""
			}
		}
		pending = append(pending, cand{a: a, raw: id})
	}

	maxNum := 0
	for _, c := range pending {
		if c.raw == "" {
			continue
		}
		if n, err := strconv.Atoi(c.raw); err == nil && n > maxNum {
			maxNum = n
		}
	}

	seen := map[string]struct{}{}
	valid := make([]Alert, 0, len(pending))
	for _, c := range pending {
		a := c.a
		id := c.raw
		if id != "" {
			if _, dup := seen[id]; dup {
				id = ""
			}
		}
		if id == "" {
			maxNum++
			a.ID = strconv.Itoa(maxNum)
		} else {
			a.ID = id
		}
		seen[a.ID] = struct{}{}
		valid = append(valid, a)
	}

	s.mu.Lock()
	s.items = valid
	s.nextID = maxNum
	s.mu.Unlock()
	log.Printf("alerts storage: restored %d alert(s) from %s", len(valid), s.filePath)
	return nil
}

func (s *alertStore) add(a Alert) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	a.ID = strconv.Itoa(s.nextID)
	if a.CreatedAt == 0 {
		a.CreatedAt = time.Now().Unix()
	}
	s.items = append(s.items, a)
	s.persistLocked()
	return a.ID
}

func (s *alertStore) remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		if s.items[i].ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			s.persistLocked()
			return true
		}
	}
	return false
}

func (s *alertStore) snapshot() []Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Alert, len(s.items))
	copy(out, s.items)
	return out
}

func main() {
	cfgPath := strings.TrimSpace(os.Getenv("CONFIG_FILE"))
	explicitCfg := cfgPath != ""
	if !explicitCfg {
		cfgPath = defaultConfigFilePath
	}

	botToken := ""
	chatID := ""
	addr := defaultAddr
	pollSecs := defaultPollSeconds
	binanceRESTBase = defaultBinanceBase

	alertsPath := defaultAlertsFilePath
	if err := applyConfigFile(cfgPath, explicitCfg, &botToken, &chatID, &addr, &pollSecs, &binanceRESTBase, &alertsPath); err != nil {
		log.Fatal(err)
	}

	// Environment overrides file (non-empty env wins).
	if v := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")); v != "" {
		botToken = v
	}
	if v := strings.TrimSpace(os.Getenv("TELEGRAM_CHAT_ID")); v != "" {
		chatID = v
	}
	if v := strings.TrimSpace(os.Getenv("LISTEN_ADDR")); v != "" {
		addr = v
	}
	if v := strings.TrimSpace(os.Getenv("POLL_SECONDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pollSecs = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("BINANCE_BASE")); v != "" {
		binanceRESTBase = strings.TrimRight(v, "/")
	}
	binanceRESTBase = strings.TrimRight(binanceRESTBase, "/")

	if v := strings.TrimSpace(os.Getenv("ALERTS_FILE")); v != "" {
		alertsPath = v
	}

	store, err := newAlertStore(alertsPath)
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	if pollSecs < 2 {
		pollSecs = 2
	}
	pollDur := time.Duration(pollSecs) * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher(ctx, client, botToken, chatID, store, pollDur)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", serveIndex)
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"telegram_configured": botToken != "" && chatID != "",
		})
	})
	mux.HandleFunc("GET /api/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"alerts": store.snapshot()})
	})
	mux.HandleFunc("GET /api/price", func(w http.ResponseWriter, r *http.Request) {
		sym := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
		if sym == "" {
			http.Error(w, "missing symbol", http.StatusBadRequest)
			return
		}
		price, err := fetchBinancePrice(r.Context(), client, sym)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"symbol": sym, "price": price})
	})
	mux.HandleFunc("POST /api/alerts", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Symbol string `json:"symbol"`
			Steps  []struct {
				Type        string  `json:"type,omitempty"`
				Target      float64 `json:"target,omitempty"`
				Direction   string  `json:"direction,omitempty"`
				LineStart   float64 `json:"line_start,omitempty"`
				LineEnd     float64 `json:"line_end,omitempty"`
				LineMinutes int     `json:"line_minutes,omitempty"`
				LineDate1   int64   `json:"line_date1,omitempty"`
				LinePrice1  float64 `json:"line_price1,omitempty"`
				LineDate2   int64   `json:"line_date2,omitempty"`
				LinePrice2  float64 `json:"line_price2,omitempty"`
			} `json:"steps"`
			Type        string  `json:"type,omitempty"`
			Target      float64 `json:"target,omitempty"`
			Direction   string  `json:"direction,omitempty"`
			LineStart   float64 `json:"line_start,omitempty"`
			LineEnd     float64 `json:"line_end,omitempty"`
			LineMinutes int     `json:"line_minutes,omitempty"`
			LineDate1   int64   `json:"line_date1,omitempty"`
			LinePrice1  float64 `json:"line_price1,omitempty"`
			LineDate2   int64   `json:"line_date2,omitempty"`
			LinePrice2  float64 `json:"line_price2,omitempty"`
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, 1<<18))
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		sym := strings.ToUpper(strings.TrimSpace(body.Symbol))
		if sym == "" {
			http.Error(w, "symbol required", http.StatusBadRequest)
			return
		}
		var steps []AlertStep
		for _, s := range body.Steps {
			st, err := normalizeStep(struct {
				Type        string  `json:"type,omitempty"`
				Target      float64 `json:"target,omitempty"`
				Direction   string  `json:"direction,omitempty"`
				LineStart   float64 `json:"line_start,omitempty"`
				LineEnd     float64 `json:"line_end,omitempty"`
				LineMinutes int     `json:"line_minutes,omitempty"`
				LineDate1   int64   `json:"line_date1,omitempty"`
				LinePrice1  float64 `json:"line_price1,omitempty"`
				LineDate2   int64   `json:"line_date2,omitempty"`
				LinePrice2  float64 `json:"line_price2,omitempty"`
			}{
				Type:        s.Type,
				Target:      s.Target,
				Direction:   s.Direction,
				LineStart:   s.LineStart,
				LineEnd:     s.LineEnd,
				LineMinutes: s.LineMinutes,
				LineDate1:   s.LineDate1,
				LinePrice1:  s.LinePrice1,
				LineDate2:   s.LineDate2,
				LinePrice2:  s.LinePrice2,
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			steps = append(steps, st)
		}
		if len(steps) == 0 {
			st, err := normalizeStep(struct {
				Type        string  `json:"type,omitempty"`
				Target      float64 `json:"target,omitempty"`
				Direction   string  `json:"direction,omitempty"`
				LineStart   float64 `json:"line_start,omitempty"`
				LineEnd     float64 `json:"line_end,omitempty"`
				LineMinutes int     `json:"line_minutes,omitempty"`
				LineDate1   int64   `json:"line_date1,omitempty"`
				LinePrice1  float64 `json:"line_price1,omitempty"`
				LineDate2   int64   `json:"line_date2,omitempty"`
				LinePrice2  float64 `json:"line_price2,omitempty"`
			}{
				Type:        body.Type,
				Target:      body.Target,
				Direction:   body.Direction,
				LineStart:   body.LineStart,
				LineEnd:     body.LineEnd,
				LineMinutes: body.LineMinutes,
				LineDate1:   body.LineDate1,
				LinePrice1:  body.LinePrice1,
				LineDate2:   body.LineDate2,
				LinePrice2:  body.LinePrice2,
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			steps = append(steps, st)
		}
		if len(steps) == 0 {
			http.Error(w, "at least one step required", http.StatusBadRequest)
			return
		}
		id := store.add(Alert{Symbol: sym, Steps: steps, StepIndex: 0})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	})
	mux.HandleFunc("DELETE /api/alerts", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		if !store.remove(id) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	log.Printf("listening on %s (poll every %ds)", addr, pollSecs)
	if botToken == "" || chatID == "" {
		log.Println("warning: telegram_bot_token / telegram_chat_id not set (config file or env) — alerts only go to stdout")
	}
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b)
}

func normalizeStep(body struct {
	Type        string  `json:"type,omitempty"`
	Target      float64 `json:"target,omitempty"`
	Direction   string  `json:"direction,omitempty"`
	LineStart   float64 `json:"line_start,omitempty"`
	LineEnd     float64 `json:"line_end,omitempty"`
	LineMinutes int     `json:"line_minutes,omitempty"`
	LineDate1   int64   `json:"line_date1,omitempty"`
	LinePrice1  float64 `json:"line_price1,omitempty"`
	LineDate2   int64   `json:"line_date2,omitempty"`
	LinePrice2  float64 `json:"line_price2,omitempty"`
}) (AlertStep, error) {
	dir := strings.ToLower(strings.TrimSpace(body.Direction))
	if dir != "above" && dir != "below" {
		return AlertStep{}, errors.New("each step direction must be above or below")
	}
	typ := strings.ToLower(strings.TrimSpace(body.Type))
	if typ == "" {
		typ = "price"
	}
	switch typ {
	case "price":
		if body.Target <= 0 {
			return AlertStep{}, errors.New("each step needs a positive target")
		}
		return AlertStep{Type: "price", Target: body.Target, Direction: dir}, nil
	case "line":
		// Support diagonal line with absolute date coordinates
		if body.LineDate1 > 0 && body.LineDate2 > 0 && body.LinePrice1 > 0 && body.LinePrice2 > 0 {
			if body.LineDate1 >= body.LineDate2 {
				return AlertStep{}, errors.New("line_date1 must be before line_date2")
			}
			return AlertStep{
				Type:       "line",
				Direction:  dir,
				LineDate1:  body.LineDate1,
				LinePrice1: body.LinePrice1,
				LineDate2:  body.LineDate2,
				LinePrice2: body.LinePrice2,
			}, nil
		}
		// Fallback to duration-based line
		if body.LineStart <= 0 || body.LineEnd <= 0 {
			return AlertStep{}, errors.New("line steps need a positive start and end price")
		}
		if body.LineMinutes <= 0 {
			return AlertStep{}, errors.New("line steps need a positive duration in minutes")
		}
		return AlertStep{
			Type:                "line",
			Direction:           dir,
			LineStart:           body.LineStart,
			LineEnd:             body.LineEnd,
			LineDurationSeconds: int64(body.LineMinutes) * 60,
		}, nil
	default:
		return AlertStep{}, fmt.Errorf("unknown step type %q", body.Type)
	}
}

func (s AlertStep) isMet(price float64, now time.Time, createdAt time.Time) bool {
	dir := strings.ToLower(strings.TrimSpace(s.Direction))
	if dir != "above" && dir != "below" {
		return false
	}
	typ := strings.ToLower(strings.TrimSpace(s.Type))
	if typ == "" {
		typ = "price"
	}
	switch typ {
	case "line":
		// Support diagonal line with absolute date coordinates
		if s.LineDate1 > 0 && s.LineDate2 > 0 && s.LinePrice1 > 0 && s.LinePrice2 > 0 {
			nowUnix := now.Unix()
			// Calculate expected price at current time based on two endpoints
			elapsed := float64(nowUnix - s.LineDate1)
			duration := float64(s.LineDate2 - s.LineDate1)
			if duration <= 0 {
				return false
			}
			progress := elapsed / duration
			// Clamp progress to [0, 1] if checking line only within date range
			// Or allow extrapolation if checking beyond the range
			expected := s.LinePrice1 + (s.LinePrice2-s.LinePrice1)*progress
			if dir == "above" {
				return price >= expected
			}
			return price <= expected
		}
		// Fallback to duration-based line (backward compatibility)
		if s.LineStart <= 0 || s.LineEnd <= 0 || s.LineDurationSeconds <= 0 {
			return false
		}
		if createdAt.IsZero() {
			createdAt = now
		}
		elapsed := now.Sub(createdAt).Seconds()
		expected := s.LineStart + (s.LineEnd-s.LineStart)*elapsed/float64(s.LineDurationSeconds)
		if dir == "above" {
			return price >= expected
		}
		return price <= expected
	default:
		if s.Target <= 0 {
			return false
		}
		if dir == "above" {
			return price >= s.Target
		}
		return price <= s.Target
	}
}

func (s AlertStep) summary() string {
	typ := strings.ToLower(strings.TrimSpace(s.Type))
	if typ == "line" {
		// Diagonal line with absolute dates
		if s.LineDate1 > 0 && s.LineDate2 > 0 {
			d1 := time.Unix(s.LineDate1, 0).Format("02.01.06 15:04")
			d2 := time.Unix(s.LineDate2, 0).Format("02.01.06 15:04")
			return fmt.Sprintf("diagonal %.2f@%s → %.2f@%s", s.LinePrice1, d1, s.LinePrice2, d2)
		}
		// Duration-based line (backward compat)
		dur := s.LineDurationSeconds / 60
		return fmt.Sprintf("line %.2f→%.2f over %dm", s.LineStart, s.LineEnd, dur)
	}
	return fmt.Sprintf("%.2f", s.Target)
}

func applyConfigFile(path string, mustExist bool, botToken, chatID, addr *string, pollSecs *int, binanceBase *string, alertsFile *string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if mustExist {
			return fmt.Errorf("config file %s: %w", path, err)
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("config file %s: %w", path, err)
	}
	var fc fileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	if s := strings.TrimSpace(fc.TelegramBotToken); s != "" {
		*botToken = s
	}
	if s := strings.TrimSpace(fc.TelegramChatID); s != "" {
		*chatID = s
	}
	if s := strings.TrimSpace(fc.ListenAddr); s != "" {
		*addr = s
	}
	if fc.PollSeconds != nil && *fc.PollSeconds > 0 {
		*pollSecs = *fc.PollSeconds
	}
	if s := strings.TrimSpace(fc.BinanceBase); s != "" {
		*binanceBase = strings.TrimRight(strings.TrimSpace(s), "/")
	}
	if s := strings.TrimSpace(fc.AlertsFile); s != "" {
		*alertsFile = s
	}
	log.Printf("loaded config file %s", path)
	return nil
}

func watcher(ctx context.Context, client *http.Client, botToken, chatID string, store *alertStore, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkAlerts(ctx, client, botToken, chatID, store)
		}
	}
}

func checkAlerts(ctx context.Context, client *http.Client, botToken, chatID string, store *alertStore) {
	alerts := store.snapshot()
	if len(alerts) == 0 {
		return
	}
	symbols := map[string]struct{}{}
	for _, a := range alerts {
		symbols[a.Symbol] = struct{}{}
	}
	results := make(map[string]float64, len(symbols))
	for sym := range symbols {
		price, err := fetchBinancePrice(ctx, client, sym)
		if err != nil {
			log.Printf("binance %s: %v", sym, err)
			continue
		}
		results[sym] = price
	}

	now := time.Now()
	nowUnix := now.Unix()
	cooldownSeconds := int64(60) // Minimum seconds between consecutive triggers for same alert

	var toUpdate []Alert
	for _, a := range alerts {
		p, ok := results[a.Symbol]
		if !ok || p <= 0 {
			toUpdate = append(toUpdate, a)
			continue
		}
		if len(a.Steps) == 0 {
			continue
		}
		if a.StepIndex >= len(a.Steps) {
			// All steps met - check if we should trigger
			// Only trigger if enough time has passed since last trigger (cooldown)
			if a.LastTriggered == 0 || (nowUnix-a.LastTriggered) >= cooldownSeconds {
				// Trigger the alert
				msg := formatTelegramMsg(a, p)
				err := sendTelegram(ctx, client, botToken, chatID, msg)
				if err != nil {
					log.Printf("telegram notify failed for alert %s: %v — will retry", a.ID, err)
				} else {
					log.Printf("fired chained alert %s %s steps=%d price=%v", a.ID, a.Symbol, len(a.Steps), p)
					a.LastTriggered = nowUnix // Update last triggered time
				}
			}
			// Keep alert in store (don't remove it)
			toUpdate = append(toUpdate, a)
			continue
		}
		cur := a.Steps[a.StepIndex]
		met := cur.isMet(p, now, time.Unix(a.CreatedAt, 0))
		if met {
			a.StepIndex++
		}
		// Keep alert in store regardless
		toUpdate = append(toUpdate, a)
	}

	store.replaceAll(toUpdate)
}

func (s *alertStore) replaceAll(next []Alert) {
	nextCopy := append([]Alert(nil), next...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if reflect.DeepEqual(s.items, nextCopy) {
		return
	}
	s.items = nextCopy
	s.persistLocked()
}

func formatTelegramMsg(a Alert, current float64) string {
	var b strings.Builder
	b.WriteString("[notifier] Binance chained alert: ")
	b.WriteString(a.Symbol)
	b.WriteString(" — all steps matched, last price ")
	b.WriteString(fmt.Sprint(current))
	b.WriteString(" USDT.\nChain: ")
	for i, s := range a.Steps {
		if i > 0 {
			b.WriteString(" → ")
		}
		op := "≥"
		if s.Direction == "below" {
			op = "≤"
		}
		b.WriteString(op)
		if strings.ToLower(strings.TrimSpace(s.Type)) == "line" {
			if s.LineDurationSeconds > 0 {
				b.WriteString(fmt.Sprintf("%.2f → %.2f over %dm", s.LineStart, s.LineEnd, s.LineDurationSeconds/60))
			} else {
				b.WriteString(fmt.Sprintf("%.2f → %.2f", s.LineStart, s.LineEnd))
			}
		} else {
			b.WriteString(fmt.Sprint(s.Target))
		}
	}
	return b.String()
}

func fetchBinancePrice(ctx context.Context, client *http.Client, symbol string) (float64, error) {
	u := binanceRESTBase + "/ticker/price?symbol=" + url.QueryEscape(symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(out.Price, 64)
}

func sendTelegram(ctx context.Context, client *http.Client, botToken, chatID, text string) error {
	if botToken == "" || chatID == "" {
		log.Println("alert (Telegram not configured):", text)
		return nil
	}
	u := "https://api.telegram.org/bot" + botToken + "/sendMessage"
	payload := map[string]string{
		"chat_id": chatID,
		"text":    text,
	}
	js, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(js)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram %d: %s", resp.StatusCode, string(body))
	}
	var tg struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(body, &tg); err != nil {
		return err
	}
	if !tg.OK {
		return errors.New("telegram returned ok:false")
	}
	return nil
}
