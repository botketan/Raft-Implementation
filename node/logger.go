package node

import (
	"fmt"
	"log"
	"os"
)

// Logger struct that holds the node ID and the logger instance
type Logger struct {
	id     string
	logger *log.Logger
	state  *State
}

// NewLogger initializes the logger, creating a log file named after the node ID
func NewLogger(id string, state *State) (Logger, error) {
	// Create log file with the name of the node ID
	fileName := fmt.Sprintf("%s.log", id)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return Logger{}, err
	}

	// Create the logger, writing to the file
	logger := log.New(file, "", log.LstdFlags|log.Lmicroseconds)
	return Logger{
		id:     id,
		state:  state,
		logger: logger,
	}, nil
}

// Log function that logs the message with the node ID and formatted arguments
func (l *Logger) Log(format string, args ...interface{}) {
	l.logger.Printf("Node ID: %s, State: %s | "+format, append([]interface{}{l.id, *(l.state)}, args...)...)
}
