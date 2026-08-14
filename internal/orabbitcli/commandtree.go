package orabbitcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

type cliCommand struct {
	Name       string
	Children   []*cliCommand
	Run        func(context.Context, []string) int
	RenderHelp func(io.Writer)
}

func cliRootCommand() *cliCommand {
	stack := &cliCommand{
		Name:       "stack",
		RenderHelp: renderStackHelp,
		Children: []*cliCommand{
			{Name: "start", Run: cmdStart},
			{Name: "stop", Run: cmdStop},
			{Name: "status", Run: cmdStackStatus},
		},
	}

	run := &cliCommand{
		Name:       "run",
		Run:        cmdRun,
		RenderHelp: renderRunGroupHelp,
		Children: []*cliCommand{
			{Name: "interactive", Run: cmdRunInteractive},
			{Name: "submit", Run: cmdRunSubmit},
			{Name: "watch", Run: cmdRunWatch},
			{Name: "cancel", Run: cmdRunCancel},
			{Name: "diagnose", Run: cmdRunDiagnose},
			{Name: "recover", Run: cmdRunRecover},
		},
	}

	return &cliCommand{
		Name:       CLIName,
		RenderHelp: usage,
		Children: []*cliCommand{
			stack,
			run,
			{Name: "help", Run: cmdHelp},
		},
	}
}

func (c *cliCommand) findChild(name string) *cliCommand {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	for _, child := range c.Children {
		if child == nil {
			continue
		}
		if child.Name == name {
			return child
		}
	}
	return nil
}

func (c *cliCommand) invoke(ctx context.Context, args []string) int {
	if len(args) == 0 {
		if c.Run != nil {
			return c.Run(ctx, args)
		}
		c.printHelp(ctx, os.Stdout)
		return exitSuccess
	}

	first := strings.TrimSpace(args[0])
	switch first {
	case "", "help":
		return c.helpTopic(ctx, args[1:])
	case "-h", "--help":
		c.printHelp(ctx, os.Stdout)
		return exitSuccess
	}

	if child := c.findChild(first); child != nil {
		return child.invoke(ctx, args[1:])
	}

	if c.Run != nil {
		return c.Run(ctx, args)
	}

	fmt.Fprintf(os.Stderr, "unknown %s %q\n", c.commandLabel(), first)
	c.printHelp(ctx, os.Stderr)
	return exitUsage
}

func (c *cliCommand) helpTopic(ctx context.Context, args []string) int {
	if len(args) == 0 {
		c.printHelp(ctx, os.Stdout)
		return exitSuccess
	}

	current := c
	for i, arg := range args {
		child := current.findChild(arg)
		if child == nil {
			fmt.Fprintf(os.Stderr, "unknown help topic %q\n", strings.Join(args[:i+1], " "))
			c.printHelp(ctx, os.Stderr)
			return exitUsage
		}
		current = child
	}

	current.printHelp(ctx, os.Stdout)
	return exitSuccess
}

func (c *cliCommand) printHelp(ctx context.Context, w io.Writer) {
	if c.RenderHelp != nil {
		c.RenderHelp(w)
		return
	}
	if c.Run != nil {
		_ = c.Run(ctx, []string{"--help"})
		return
	}
}

func (c *cliCommand) commandLabel() string {
	if c == nil || strings.TrimSpace(c.Name) == "" || c.Name == CLIName {
		return "command"
	}
	return c.Name + " command"
}
