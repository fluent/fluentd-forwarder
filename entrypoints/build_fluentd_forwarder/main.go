package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

func getRevision(importPath string) (string, error) {
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		return "", fmt.Errorf("GOPATH is not set")
	}
	repoPath := filepath.Join(goPath, "src", strings.Replace(importPath, "/", string(filepath.Separator), -1))
	err := os.Chdir(repoPath)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("git", "describe", "--tags", "--abbrev=0")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return (string)(out), nil
}

const ImportPathBase = "github.com/fluent/fluentd-forwarder"
const VersionStringVarName = "main.progVersion"

func bail(message string, status int) {
	fmt.Fprintf(os.Stderr, "build: %s\n", message)
	os.Exit(status)
}

func main() {
	rev, err := getRevision(ImportPathBase)
	if err != nil {
		bail(err.Error(), 1)
	}
	for _, app := range os.Args[1:] {
		cmd := exec.Command("go", "get", "-u", "-ldflags", fmt.Sprintf("-X %s=%s", VersionStringVarName, rev), path.Join(ImportPathBase, "entrypoints", app))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			bail(err.Error(), 2)
		}
	}
	os.Exit(0)
}
