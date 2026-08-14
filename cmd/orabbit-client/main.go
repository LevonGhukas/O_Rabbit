package main

import (
	"os"

	"github.com/LevonGhukas/O_Rabbit/internal/orabbitcli"
)

// Keep the canonical client entrypoint thin; shared CLI logic lives in internal/orabbitcli.
func main() {
	os.Exit(orabbitcli.Main(os.Args[1:]))
}
