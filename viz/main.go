package main

import (
	"bufio"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// LogEntry represents a parsed log entry
type LogEntry struct {
	Timestamp time.Time
	NodeName  string
	State     string
	Message   string
}

// NodeLogs holds logs for a specific node, indexed by timestamp
type NodeLogs map[time.Time]LogEntry

// LogGrid represents a grid where rows are timestamps and columns are nodes
type LogGrid struct {
	Timestamps []time.Time
	Nodes      []string
	Logs       map[string]NodeLogs
}

var logPattern = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+)\s+Node ID: (\w+), State: (\w+)\s*\|\s*(.*)$`)

func main() {
	logDir := "../debug_logs"
	logEntries, err := readLogs(logDir)
	if err != nil {
		fmt.Printf("Error reading logs: %v\n", err)
		return
	}

	// Create a grid structure based on logs
	grid := buildLogGrid(logEntries)

	// Generate HTML page
	if err := generateHTML(grid, "output.html"); err != nil {
		fmt.Printf("Error generating HTML: %v\n", err)
	}
}

// readLogs reads all logs from the specified directory and returns a slice of LogEntry
func readLogs(logDir string) ([]LogEntry, error) {
	var logs []LogEntry

	err := filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(info.Name(), ".log") {
			return nil
		}
		entries, err := parseLogFile(path)
		if err != nil {
			return err
		}
		logs = append(logs, entries...)
		return nil
	})

	return logs, err
}

// parseLogFile parses a single log file and returns the log entries
func parseLogFile(filePath string) ([]LogEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		entry, err := parseLogLine(line)
		if err == nil {
			entries = append(entries, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// parseLogLine parses a single log line and returns a LogEntry
func parseLogLine(line string) (LogEntry, error) {
	matches := logPattern.FindStringSubmatch(line)
	if len(matches) == 0 {
		return LogEntry{}, fmt.Errorf("log line did not match expected format: %s", line)
	}

	timestamp, err := time.Parse("2006/01/02 15:04:05.000000", matches[1])
	if err != nil {
		return LogEntry{}, err
	}

	return LogEntry{
		Timestamp: timestamp,
		NodeName:  matches[2],
		State:     matches[3],
		Message:   matches[4],
	}, nil
}

// buildLogGrid creates a LogGrid that aligns logs by timestamps for all nodes
func buildLogGrid(logs []LogEntry) LogGrid {
	grid := LogGrid{
		Logs: make(map[string]NodeLogs),
	}

	timestampSet := make(map[time.Time]struct{})

	// Collect logs for each node and record all timestamps
	for _, log := range logs {
		if _, exists := grid.Logs[log.NodeName]; !exists {
			grid.Nodes = append(grid.Nodes, log.NodeName)
			grid.Logs[log.NodeName] = make(NodeLogs)
		}
		grid.Logs[log.NodeName][log.Timestamp] = log
		timestampSet[log.Timestamp] = struct{}{}
	}

	// Collect all unique timestamps and sort them
	for ts := range timestampSet {
		grid.Timestamps = append(grid.Timestamps, ts)
	}
	sort.Slice(grid.Timestamps, func(i, j int) bool {
		return grid.Timestamps[i].Before(grid.Timestamps[j])
	})

	return grid
}

// generateHTML generates an HTML page with the log entries in a grid format
func generateHTML(grid LogGrid, outputPath string) error {
	tmpl := `
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Node Logs Visualization</title>
	<style>
		body { font-family: Arial, sans-serif; }
		table { width: 100%; border-collapse: collapse; margin-top: 20px; }
		th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
		th { background-color: #f2f2f2; }
		.follower { background-color: #e0f7fa; }
		.leader { background-color: #c8e6c9; }
		.candidate { background-color: #fff9c4; }
		.dead { background-color: #ffcdd2; }
		.log-timestamp { font-weight: bold; }
		.log-node { color: #2E86C1; }
		.log-state { font-weight: bold; }
		.empty { background-color: #f9f9f9; }
	</style>
</head>
<body>
	<h1>Node Logs Visualization</h1>
	<table>
		<thead>
			<tr>
				<th>Timestamp</th>
				{{ range .Nodes }}
					<th>{{ . }}</th>
				{{ end }}
			</tr>
		</thead>
		<tbody>
			{{ range $timestamp := .Timestamps }}
			<tr>
				<td class="log-timestamp">{{ $timestamp }}</td>
				{{ range $node := $.Nodes }}
					{{ $log := index $.Logs $node }}
					{{ with $entry := index $log $timestamp }}
						<td class="{{ $entry.State | lower }}">
							<strong>{{ $entry.State }}</strong><br>{{ $entry.Message }}
						</td>
					{{ else }}
						<td class="empty"></td>
					{{ end }}
				{{ end }}
			</tr>
			{{ end }}
		</tbody>
	</table>
</body>
</html>
    `

	t, err := template.New("logs").Funcs(template.FuncMap{
		"lower": strings.ToLower,
	}).Parse(tmpl)
	if err != nil {
		return err
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return t.Execute(f, grid)
}
