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
	"sort"
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
	Target    float64 `json:"target"`
	Direction string  `json:"direction"` // "above" or "below"
}

type Alert struct {
	ID        string      `json:"id"`
	Symbol    string      `json:"symbol"`
	Steps     []AlertStep `json:"steps"`
	StepIndex int         `json:"step_index"` // index of step we are waiting for (0-based)
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
				Target    float64 `json:"target"`
				Direction string  `json:"direction"`
			} `json:"steps"`
			Target    float64 `json:"target"`
			Direction string  `json:"direction"`
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
		if len(body.Steps) > 0 {
			for _, s := range body.Steps {
				st, err := normalizeStep(s.Target, s.Direction)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				steps = append(steps, st)
			}
		} else {
			st, err := normalizeStep(body.Target, body.Direction)
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

func normalizeStep(target float64, direction string) (AlertStep, error) {
	if target <= 0 {
		return AlertStep{}, errors.New("each step needs a positive target")
	}
	dir := strings.ToLower(strings.TrimSpace(direction))
	if dir != "above" && dir != "below" {
		return AlertStep{}, errors.New("each step direction must be above or below")
	}
	return AlertStep{Target: target, Direction: dir}, nil
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

	var triggered []Alert
	var keep []Alert
	for _, a := range alerts {
		p, ok := results[a.Symbol]
		if !ok || p <= 0 {
			keep = append(keep, a)
			continue
		}
		if len(a.Steps) == 0 {
			continue
		}
		if a.StepIndex >= len(a.Steps) {
			triggered = append(triggered, a)
			continue
		}
		cur := a.Steps[a.StepIndex]
		met := (cur.Direction == "above" && p >= cur.Target) || (cur.Direction == "below" && p <= cur.Target)
		if met {
			a.StepIndex++
			if a.StepIndex >= len(a.Steps) {
				triggered = append(triggered, a)
			} else {
				keep = append(keep, a)
			}
		} else {
			keep = append(keep, a)
		}
	}

	if len(triggered) == 0 {
		store.replaceAll(keep)
		return
	}

	sort.Slice(triggered, func(i, j int) bool { return triggered[i].ID < triggered[j].ID })
	for _, a := range triggered {
		price := results[a.Symbol]
		msg := formatTelegramMsg(a, price)
		err := sendTelegram(ctx, client, botToken, chatID, msg)
		if err != nil {
			log.Printf("telegram notify failed for alert %s: %v — re-queued", a.ID, err)
			keep = append(keep, a)
			continue
		}
		log.Printf("fired chained alert %s %s steps=%d price=%v", a.ID, a.Symbol, len(a.Steps), price)
	}
	store.replaceAll(keep)
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
		b.WriteString(fmt.Sprint(s.Target))
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
