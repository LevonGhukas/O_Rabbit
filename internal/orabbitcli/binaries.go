package orabbitcli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type daemonBinarySpec struct {
	FlagName    string
	DisplayName string
	SiblingName string
	BuildTarget string
}

var (
	masterBinarySpec = daemonBinarySpec{
		FlagName:    "--master-bin",
		DisplayName: defaultMasterHelperBinary + exeSuffix(),
		SiblingName: defaultMasterHelperBinary + exeSuffix(),
		BuildTarget: "./cmd/master",
	}
	workerBinarySpec = daemonBinarySpec{
		FlagName:    "--worker-bin",
		DisplayName: defaultWorkerHelperBinary + exeSuffix(),
		SiblingName: defaultWorkerHelperBinary + exeSuffix(),
		BuildTarget: "./cmd/worker",
	}
)

func resolveDaemonBinary(spec daemonBinarySpec, override string) (string, error) {
	clientExe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve %s location: %w", spec.DisplayName, err)
	}
	return resolveDaemonBinaryFromClientPath(clientExe, spec, override)
}

func resolveDaemonBinaryFromClientPath(clientExe string, spec daemonBinarySpec, override string) (string, error) {
	if v := strings.TrimSpace(override); v != "" {
		path, err := filepath.Abs(v)
		if err != nil {
			return "", fmt.Errorf("resolve %s %q: %w", spec.FlagName, v, err)
		}
		if err := validateDaemonBinaryPath(path); err != nil {
			return "", fmt.Errorf("invalid %s %q: %w", spec.FlagName, path, err)
		}
		return path, nil
	}

	clientExe, err := filepath.Abs(clientExe)
	if err != nil {
		return "", fmt.Errorf("resolve client path %q: %w", clientExe, err)
	}
	candidate := filepath.Join(filepath.Dir(clientExe), spec.SiblingName)
	if err := validateDaemonBinaryPath(candidate); err == nil {
		return candidate, nil
	}

	return "", fmt.Errorf(
		"missing %s: looked for %q next to %q; build it with `go build -o %s %s` or pass %s",
		spec.DisplayName,
		candidate,
		clientExe,
		candidate,
		spec.BuildTarget,
		spec.FlagName,
	)
}

func validateDaemonBinaryPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory")
	}
	return nil
}

func stageManagedBinary(sourcePath, runtimeDir, stagedName string) (string, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	runtimeDir = strings.TrimSpace(runtimeDir)
	if sourcePath == "" {
		return "", fmt.Errorf("missing source binary path")
	}
	if runtimeDir == "" {
		return "", fmt.Errorf("missing managed runtime dir")
	}

	sourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", fmt.Errorf("resolve source binary %q: %w", sourcePath, err)
	}
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return "", fmt.Errorf("stat source binary %q: %w", sourcePath, err)
	}

	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		return "", fmt.Errorf("prepare managed runtime dir %q: %w", runtimeDir, err)
	}

	stagedPath := filepath.Join(runtimeDir, stagedName)
	if sameFilePath(sourcePath, stagedPath) {
		return stagedPath, nil
	}
	if stagedInfo, err := os.Stat(stagedPath); err == nil {
		if stagedInfo.Size() == sourceInfo.Size() && !stagedInfo.ModTime().Before(sourceInfo.ModTime()) {
			return stagedPath, nil
		}
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return "", fmt.Errorf("open source binary %q: %w", sourcePath, err)
	}
	defer src.Close()

	tmp, err := os.CreateTemp(runtimeDir, stagedName+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("stage %q in %q: %w", stagedName, runtimeDir, err)
	}
	tmpPath := tmp.Name()
	cleanupTmp := true
	defer func() {
		_ = tmp.Close()
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(tmp, src); err != nil {
		return "", fmt.Errorf("copy %q into managed runtime dir %q: %w", sourcePath, runtimeDir, err)
	}
	mode := sourceInfo.Mode() & 0o777
	if mode == 0 {
		mode = 0o755
	}
	if err := tmp.Chmod(mode); err != nil {
		return "", fmt.Errorf("chmod staged binary %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close staged binary %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, stagedPath); err != nil {
		return "", fmt.Errorf("finalize staged binary %q: %w", stagedPath, err)
	}
	cleanupTmp = false
	return stagedPath, nil
}

func resolveManagedDaemonBinary(spec daemonBinarySpec, override, runtimeDir string) (string, error) {
	sourcePath, err := resolveDaemonBinary(spec, override)
	if err != nil {
		return "", err
	}
	stagedPath, err := stageManagedBinary(sourcePath, runtimeDir, spec.SiblingName)
	if err != nil {
		return "", fmt.Errorf("prepare managed %s from %q: %w", spec.DisplayName, sourcePath, err)
	}
	return stagedPath, nil
}

func sameFilePath(a, b string) bool {
	aa := filepath.Clean(a)
	bb := filepath.Clean(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(aa, bb)
	}
	return aa == bb
}
