package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	opsdocker "github.com/LevonGhukas/O_Rabbit/internal/ops/docker"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

func TestValidateComponentAllowlist(t *testing.T) {
	for _, component := range []string{"master", "worker", "minio"} {
		if err := ValidateComponent(component); err != nil {
			t.Fatalf("expected supported component %q: %v", component, err)
		}
	}
	if err := ValidateComponent("postgres"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected not implemented error for postgres, got %v", err)
	}
	if err := ValidateComponent("bad"); err == nil {
		t.Fatal("expected unsupported component error")
	}
}

func TestValidateScaleWorker(t *testing.T) {
	scale := 3
	if err := ValidateScale("worker", &scale); err != nil {
		t.Fatalf("expected valid worker scale: %v", err)
	}
	tooLarge := 51
	if err := ValidateScale("worker", &tooLarge); err == nil {
		t.Fatal("expected worker scale validation failure")
	}
	if err := ValidateScale("master", &scale); err == nil {
		t.Fatal("expected non-worker scale rejection")
	}
}

func TestPrepareDeploymentBuildsAllowlistedWorkerCommand(t *testing.T) {
	scale := 3
	svc := NewService(
		CommandExecutorFunc(func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
			return sshops.CommandResult{
				ExitCode:   0,
				StdoutTail: "deploy-master.sh=1\ndeploy-worker.sh=1\ndeploy-minio.sh=1\ndocker-compose.master.yml=1\ndocker-compose.worker.yml=1\ndocker-compose.minio.yml=1\nDockerfile.orabbit=1\n",
			}, nil
		}),
		DockerCheckerFunc(func(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error) {
			return opsdocker.DockerStatus{Installed: true, DaemonOK: true, Version: "26.0.0"}, nil
		}),
		SSHTesterFunc(func(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error) {
			return sshops.TestResult{HostKeyFingerprint: "SHA256:abc"}, nil
		}),
		0,
	)

	plan, err := svc.PrepareDeployment(context.Background(), sshops.SSHTarget{Host: "127.0.0.1", User: "deploy"}, "/root/O_Rabbit", "worker", DeploymentParams{Scale: &scale})
	if err != nil {
		t.Fatalf("PrepareDeployment: %v", err)
	}
	if plan.ScriptID != "deploy-worker.sh" {
		t.Fatalf("script_id=%q want deploy-worker.sh", plan.ScriptID)
	}
	if !strings.Contains(plan.Command, "WORKER_SCALE") || !strings.Contains(plan.Command, "3") {
		t.Fatalf("expected scale in command: %s", plan.Command)
	}
}

func TestNormalizeWorkerSettings(t *testing.T) {
	ok := []DeploymentParams{
		{},
		{Name: "etl-a", CPUs: "1.5", Memory: "2g"},
		{Name: " etl-b ", CPUs: "0.1", Memory: "64M"}, // surrounding spaces trimmed, memory unit lowercased
		{CPUs: "64", Memory: "512g"},
	}
	for _, p := range ok {
		if _, err := NormalizeWorkerSettings("worker", p); err != nil {
			t.Errorf("NormalizeWorkerSettings(%+v) unexpected error: %v", p, err)
		}
	}
	got, _ := NormalizeWorkerSettings("worker", DeploymentParams{Memory: " 64M "})
	if got.Memory != "64m" {
		t.Errorf("memory normalized to %q, want 64m", got.Memory)
	}

	bad := []DeploymentParams{
		{Name: "Etl"},         // uppercase
		{Name: "-etl"},        // leading dash
		{Name: "etl_a"},       // underscore
		{Name: "a; rm -rf /"}, // shell metacharacters
		{CPUs: "0.05"},        // below minimum
		{CPUs: "65"},          // above maximum
		{CPUs: "1.555"},       // too many decimals
		{CPUs: "1e1"},         // not a plain number
		{Memory: "32m"},       // below minimum
		{Memory: "513g"},      // above maximum
		{Memory: "2gb"},       // wrong unit
		{Memory: "1.5g"},      // fractional
	}
	for _, p := range bad {
		if _, err := NormalizeWorkerSettings("worker", p); err == nil {
			t.Errorf("NormalizeWorkerSettings(%+v) accepted invalid settings", p)
		}
	}

	if _, err := NormalizeWorkerSettings("master", DeploymentParams{Name: "etl"}); err == nil {
		t.Error("worker settings must be rejected for non-worker components")
	}
}

func TestPrepareDeploymentPassesWorkerSettings(t *testing.T) {
	svc := NewService(
		CommandExecutorFunc(func(ctx context.Context, target sshops.SSHTarget, command string, onStream sshops.StreamCallback) (sshops.CommandResult, error) {
			return sshops.CommandResult{
				ExitCode:   0,
				StdoutTail: "deploy-master.sh=1\ndeploy-worker.sh=1\ndeploy-minio.sh=1\ndocker-compose.master.yml=1\ndocker-compose.worker.yml=1\ndocker-compose.minio.yml=1\nDockerfile.orabbit=1\n",
			}, nil
		}),
		DockerCheckerFunc(func(ctx context.Context, target sshops.SSHTarget) (opsdocker.DockerStatus, error) {
			return opsdocker.DockerStatus{Installed: true, DaemonOK: true, Version: "26.0.0"}, nil
		}),
		SSHTesterFunc(func(ctx context.Context, target sshops.SSHTarget) (sshops.TestResult, error) {
			return sshops.TestResult{HostKeyFingerprint: "SHA256:abc"}, nil
		}),
		0,
	)
	target := sshops.SSHTarget{Host: "127.0.0.1", User: "deploy"}

	plan, err := svc.PrepareDeployment(context.Background(), target, "/root/O_Rabbit", "worker", DeploymentParams{Name: "etl-a", CPUs: "1.5", Memory: "2G"})
	if err != nil {
		t.Fatalf("PrepareDeployment: %v", err)
	}
	for _, want := range []string{"ORABBIT_WORKER_NAME=", "etl-a", "ORABBIT_WORKER_CPUS=", "1.5", "ORABBIT_WORKER_MEMORY=", "2g"} {
		if !strings.Contains(plan.Command, want) {
			t.Errorf("command missing %q: %s", want, plan.Command)
		}
	}

	plain, err := svc.PrepareDeployment(context.Background(), target, "/root/O_Rabbit", "worker", DeploymentParams{})
	if err != nil {
		t.Fatalf("PrepareDeployment without settings: %v", err)
	}
	if strings.Contains(plain.Command, "ORABBIT_WORKER_") {
		t.Errorf("unset settings must not be passed: %s", plain.Command)
	}

	if _, err := svc.PrepareDeployment(context.Background(), target, "/root/O_Rabbit", "worker", DeploymentParams{Memory: "1tb"}); err == nil {
		t.Error("invalid memory must fail before anything runs on the server")
	}
}
