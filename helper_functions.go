package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
)

func getHealth(alias string) (string, error) {
	cmd := exec.Command("./mc.exe", "ping", alias, "--json", "--count", "1")
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
		fmt.Println(line)

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return "", err
		}

		status = data["status"]
	}

	return status.(string), nil
}
