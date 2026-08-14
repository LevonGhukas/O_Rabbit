package docker

import (
	"context"
	"testing"

	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

func TestValidateContainerRef(t *testing.T) {
	valid := []string{"abc123", "orabbit-worker-1", "minio.init", "container_name"}
	for _, item := range valid {
		if err := ValidateContainerRef(item); err != nil {
			t.Fatalf("expected valid ref %q: %v", item, err)
		}
	}
	invalid := []string{"", "../bad", "name with spaces", "semi;colon"}
	for _, item := range invalid {
		if err := ValidateContainerRef(item); err == nil {
			t.Fatalf("expected invalid ref %q", item)
		}
	}
}

func TestListContainersParsesDockerOutput(t *testing.T) {
	svc := NewService(CommandExecutorFunc(func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
		return sshops.CommandResult{
			ExitCode: 0,
			StdoutTail: `{"ID":"abc123","Names":"orabbit-worker-1","Image":"orabbit-worker:latest","State":"running","Status":"Up 1 minute (healthy)","Ports":"9102/tcp","CreatedAt":"2026-05-03 10:00:00 +0000 UTC"}
{"ID":"def456","Names":"minio","Image":"minio/minio:latest","State":"exited","Status":"Exited (0) 2 minutes ago","Ports":"","CreatedAt":"2026-05-03 09:55:00 +0000 UTC"}`,
		}, nil
	}), 0)

	items, err := svc.ListContainers(context.Background(), sshops.SSHTarget{})
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("container count=%d want 2", len(items))
	}
	if items[0].Health != "healthy" {
		t.Fatalf("health=%q want healthy", items[0].Health)
	}
	if items[1].State != "exited" {
		t.Fatalf("state=%q want exited", items[1].State)
	}
}
