package logging

import (
	"log"
	"os"
)

func New() *log.Logger {
	return log.New(os.Stdout, "[kafka-clone] ", log.LstdFlags|log.Lmicroseconds)
}
