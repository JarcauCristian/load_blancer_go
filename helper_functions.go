package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

func getHealth(alias string) (string, error) {
	cmdArgs := []string{"./mc.exe", "ping", alias, "--json", "--count", "1"}
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(&stdout)

	var status interface{}
	for scanner.Scan() {
		line := scanner.Text()
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return "", err
		}

		status = data["status"]
	}

	return status.(string), nil
}

func searchTags(alias []string, tags map[string]string) (map[string][]string, error) {
	findList := make([]string, len(tags))
	cmdArgs := []string{"./mc.exe", "find", alias[1]}
	index := 0
	for k, v := range tags {
		findList[index] = fmt.Sprintf("--tags=%s=%s", k, v)
		index++
	}

	cmdArgs = append(cmdArgs, findList...)

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	var findings []string
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := scanner.Text()
		findings = append(findings, strings.Join(strings.Split(line, "/")[1:], "/"))
	}

	return map[string][]string{alias[0]: findings}, nil
}
