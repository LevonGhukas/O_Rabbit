package main

import (
	"fmt"
	"os"

	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"
	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli/tui"
)

// Keep the canonical client entrypoint thin; shared CLI logic lives in internal/orabbitcli.
func main() {
	// No arguments in a real terminal opens the interactive shell. Pipes,
	// scripts and CI keep the previous behavior (usage + exit code 2).
	if len(os.Args) == 1 && tui.IsTerminal() {
		if err := tui.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "interactive shell:", err)
			os.Exit(1)
		}
		return
	}
	os.Exit(orabbitcli.Main(os.Args[1:]))
}
