package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/pranesh/meshflow/internal/api"
	"github.com/pranesh/meshflow/internal/config"
	"github.com/pranesh/meshflow/internal/node"
)

var (
	Version   = "0.1.0"
	BuildTime = "unknown"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "node":
		if len(os.Args) < 3 || os.Args[2] != "start" {
			fmt.Fprintln(os.Stderr, "Usage: meshflow node start")
			os.Exit(1)
		}
		startNode()
	case "workflow":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: meshflow workflow <deploy|list|trigger|status> [args...]")
			os.Exit(1)
		}
		handleWorkflow(os.Args[2:])
	case "mesh":
		handleMesh(os.Args[2:])
	case "events":
		handleEvents(os.Args[2:])
	case "version":
		fmt.Printf("meshflow %s (built %s)\n", Version, BuildTime)
	default:
		printUsage()
		os.Exit(1)
	}
}

func apiURL() string {
	host := os.Getenv("MESHFLOW_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MESHFLOW_API_PORT")
	if port == "" {
		port = "8080"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

func startNode() {
	cfg := config.FromEnv()
	slog.Info("meshflow", "version", Version, "name", cfg.NodeName)

	n := node.New(cfg)
	if err := n.Start(); err != nil {
		slog.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	apiSrv := api.NewServer(cfg.APIPort, n)
	if err := apiSrv.Start(); err != nil {
		slog.Error("api server failed", "error", err)
	}

	slog.Info("api server listening", "port", cfg.APIPort)
	n.Wait()
	apiSrv.Shutdown()
}

func handleWorkflow(args []string) {
	switch args[0] {
	case "deploy":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: meshflow workflow deploy <file>")
			os.Exit(1)
		}
		data, err := os.ReadFile(args[1])
		if err != nil {
			slog.Error("failed to read file", "file", args[1], "error", err)
			os.Exit(1)
		}

		resp, err := http.Post(apiURL()+"/deploy", "application/octet-stream", bytes.NewReader(data))
		if err != nil {
			slog.Error("failed to connect to node api", "url", apiURL(), "error", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var result map[string]string
		json.Unmarshal(body, &result)

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Error: %s\n", result["error"])
			os.Exit(1)
		}
		fmt.Printf("Deployed: %s\n", args[1])

	case "list":
		resp, err := http.Get(apiURL() + "/workflows")
		if err != nil {
			slog.Error("failed to connect", "error", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		json.Unmarshal(body, &result)

		workflows, ok := result["workflows"].([]interface{})
		if !ok || len(workflows) == 0 {
			fmt.Println("No workflows deployed.")
			return
		}
		for _, w := range workflows {
			fmt.Println(w)
		}

	case "trigger":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: meshflow workflow trigger <name>")
			os.Exit(1)
		}
		resp, err := http.Post(apiURL()+"/trigger/"+args[1], "application/json", nil)
		if err != nil {
			slog.Error("failed to connect", "error", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var result map[string]string
		json.Unmarshal(body, &result)

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Error: %s\n", result["error"])
			os.Exit(1)
		}
		fmt.Printf("Triggered: %s\n", args[1])

	case "status":
		runID := ""
		if len(args) > 1 {
			runID = args[1]
		}

		if runID == "" {
			resp, err := http.Get(apiURL() + "/runs")
			if err != nil {
				slog.Error("failed to connect", "error", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			var result map[string]interface{}
			json.Unmarshal(body, &result)

			runs, ok := result["runs"].([]interface{})
			if !ok || len(runs) == 0 {
				fmt.Println("No runs found.")
				return
			}
			fmt.Println(padRight("RUN ID", 38) + padRight("WORKFLOW", 16) + "STATUS")
			fmt.Println(strings.Repeat("-", 70))
			for _, r := range runs {
				rm := r.(map[string]interface{})
				fmt.Printf("%s%s%s\n",
					padRight(fmt.Sprint(rm["id"]), 38),
					padRight(fmt.Sprint(rm["workflow"]), 16),
					rm["status"],
				)
			}
			return
		}

		resp, err := http.Get(apiURL() + "/runs/" + runID)
		if err != nil {
			slog.Error("failed to connect", "error", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Run not found: %s\n", runID)
			os.Exit(1)
		}

		var run map[string]interface{}
		json.Unmarshal(body, &run)

		fmt.Printf("Run ID:    %s\n", run["id"])
		fmt.Printf("Workflow:  %s\n", run["workflow"])
		fmt.Printf("Status:    %s\n", run["status"])
		fmt.Printf("Node:      %s\n", run["node"])
		fmt.Printf("Started:   %s\n", run["started_at"])

		tasks, ok := run["tasks"].(map[string]interface{})
		if ok {
			fmt.Println("\nTasks:")
			for id, v := range tasks {
				t := v.(map[string]interface{})
				fmt.Printf("  %-15s  %-10s  attempt=%v", id, t["status"], t["attempt"])
				if e, ok := t["error"].(string); ok && e != "" {
					fmt.Printf("  error=%s", e)
				}
				if o, ok := t["output"].(string); ok && o != "" {
					if len(o) > 60 {
						fmt.Printf("  output=%s...", o[:60])
					} else {
						fmt.Printf("  output=%s", o)
					}
				}
				fmt.Println()
			}
		}

	default:
		fmt.Fprintln(os.Stderr, "Usage: meshflow workflow <deploy|list|trigger|status> [args...]")
		os.Exit(1)
	}
}

func handleMesh(args []string) {
	if len(args) < 1 || args[0] != "status" {
		fmt.Fprintln(os.Stderr, "Usage: meshflow mesh status")
		os.Exit(1)
	}

	resp, err := http.Get(apiURL() + "/mesh")
	if err != nil {
		slog.Error("failed to connect", "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	members, ok := result["members"].([]interface{})
	if !ok || len(members) == 0 {
		fmt.Println("No mesh members.")
		return
	}
	fmt.Println("Mesh members:")
	for _, m := range members {
		fmt.Printf("  %s\n", m)
	}
}

func handleEvents(args []string) {
	if len(args) < 1 || args[0] != "tail" {
		fmt.Fprintln(os.Stderr, "Usage: meshflow events tail")
		os.Exit(1)
	}

	resp, err := http.Get(apiURL() + "/events")
	if err != nil {
		slog.Error("failed to connect", "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", body)
		os.Exit(1)
	}

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			slog.Error("read error", "error", err)
			return
		}
		fmt.Print(string(buf[:n]))
	}
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return s + strings.Repeat(" ", n-len(s))
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `meshflow %s — decentralized event-driven workflow mesh

Usage:
  meshflow node start              Start a mesh node
  meshflow workflow deploy <file>  Deploy a workflow from a YAML file
  meshflow workflow list           List deployed workflows
  meshflow workflow trigger <name> Trigger a workflow run
  meshflow workflow status [run]   Show workflow run status
  meshflow events tail             Stream live mesh events
  meshflow mesh status             Show cluster topology
  meshflow version                 Print version

Environment:
  MESHFLOW_NAME               Node name (default: hostname)
  MESHFLOW_BIND_ADDR          Bind address (default: 0.0.0.0)
  MESHFLOW_PORT               Main port (default: 7946)
  MESHFLOW_GOSSIP_PORT        Gossip port (default: 7947)
  MESHFLOW_PUBSUB_PORT        Pub/sub port (default: 4222)
  MESHFLOW_API_PORT           HTTP API port (default: 8080)
  MESHFLOW_HOST               API host for CLI commands (default: localhost)
  MESHFLOW_SEEDS              Comma-separated seed nodes (host:port)
  MESHFLOW_DATA_DIR           Data directory (default: ./data)
`, Version)
}
