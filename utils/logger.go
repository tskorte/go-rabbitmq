package utils

import (
	"fmt"
	"log"
)

func LogError(text string) {
	fmt.Errorf(" [🛑] %s", text)
}

func LogSuccess(text string) {
	log.Printf(" [✅] %s", text)
}

func LogWorking(text string) {
	log.Printf(" [🔄] %s", text)
}
