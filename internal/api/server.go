package api

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
)

//go:embed static/*
var staticFiles embed.FS

type NodeAPI interface {
	DeployWorkflow(data []byte) error
	TriggerWorkflow(name string) error
	ListWorkflows() []string
	ListRuns(workflowName string) []string
	GetRun(runID string) interface{}
	GetWorkflowDef(name string) interface{}
	MeshMembers() []string
	SubscribeEvents() (chan []byte, func(), error)
}

type Server struct {
	port int
	node NodeAPI
	srv  *http.Server
}

func NewServer(port int, node NodeAPI) *Server {
	return &Server{
		port: port,
		node: node,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	staticFS, _ := fs.Sub(staticFiles, "static")
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	mux.HandleFunc("/deploy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.node.DeployWorkflow(body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deployed"})
	})

	mux.HandleFunc("/workflows", func(w http.ResponseWriter, r *http.Request) {
		workflows := s.node.ListWorkflows()
		writeJSON(w, http.StatusOK, map[string]interface{}{"workflows": workflows})
	})

	mux.HandleFunc("/workflows/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/workflows/")
		if name == "" {
			http.Error(w, "workflow name required", http.StatusBadRequest)
			return
		}
		wf := s.node.GetWorkflowDef(name)
		if wf == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "workflow not found"})
			return
		}
		writeJSON(w, http.StatusOK, wf)
	})

	mux.HandleFunc("/trigger/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/trigger/")
		if name == "" {
			http.Error(w, "workflow name required", http.StatusBadRequest)
			return
		}
		if err := s.node.TriggerWorkflow(name); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "triggered"})
	})

	mux.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		workflow := r.URL.Query().Get("workflow")
		runIDs := s.node.ListRuns(workflow)

		type runSummary struct {
			ID       string `json:"id"`
			Workflow string `json:"workflow"`
			Status   string `json:"status"`
		}
		summaries := make([]runSummary, 0, len(runIDs))
		for _, id := range runIDs {
			runRaw := s.node.GetRun(id)
			if runRaw == nil {
				continue
			}
			var summary runSummary
			summary.ID = id
			data, _ := json.Marshal(runRaw)
			var m map[string]interface{}
			json.Unmarshal(data, &m)
			if wf, ok := m["workflow"].(string); ok {
				summary.Workflow = wf
			}
			if st, ok := m["status"].(string); ok {
				summary.Status = st
			}
			summaries = append(summaries, summary)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"runs": summaries})
	})

	mux.HandleFunc("/runs/", func(w http.ResponseWriter, r *http.Request) {
		runID := strings.TrimPrefix(r.URL.Path, "/runs/")
		if runID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "run id required"})
			return
		}
		run := s.node.GetRun(runID)
		if run == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "run not found"})
			return
		}
		writeJSON(w, http.StatusOK, run)
	})

	mux.HandleFunc("/mesh", func(w http.ResponseWriter, r *http.Request) {
		members := s.node.MeshMembers()
		writeJSON(w, http.StatusOK, map[string]interface{}{"members": members})
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch, cancel, err := s.node.SubscribeEvents()
		if err != nil {
			slog.Error("failed to subscribe events", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer cancel()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-ch:
				if !ok {
					return
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	})

	s.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	slog.Info("api server starting", "port", s.port)
	go func() {
		if err := s.srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("api server error", "error", err)
		}
	}()
	return nil
}

func (s *Server) Shutdown() {
	if s.srv != nil {
		s.srv.Close()
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
