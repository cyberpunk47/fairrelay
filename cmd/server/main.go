package main

import (
	"fmt"
	"os"

	"github.com/cyberpunk47/fairrelay/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Println("config error")
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Policy", cfg.Policy, "| fast", cfg.WorkersFast, "| slow", cfg.WorkersSlow)
}
