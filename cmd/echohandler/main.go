package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type ExecuteRequest struct {
	WorkflowID string                 `json:"workflow_id"`
	TaskID     string                 `json:"task_id"`
	Input      map[string]interface{} `json:"input"`
}

type ExecuteResult struct {
	Success bool   `json:"success"`
	Output  string `json:"output"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("task=%s workflow=%s handler=%s", req.TaskID, req.WorkflowID, r.URL.Path)

		time.Sleep(200 * time.Millisecond)

		result := ExecuteResult{
			Success: true,
			Output:  fmt.Sprintf("Task %s completed successfully at %s", req.TaskID, time.Now().Format(time.RFC3339)),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	port := "9999"
	log.Printf("echo handler listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
