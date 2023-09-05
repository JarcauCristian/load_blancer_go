package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
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

func getTotalBytes(alias []string, token string, fileSize float64) (float64, error) {

	fullUrl := alias[0] + "/minio/v2/metrics/cluster"

	req, err := http.NewRequest("POST", fullUrl, bytes.NewBuffer([]byte{}))

	if err != nil {
		return 0.0, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Second * 10}

	resp, err := client.Do(req)

	if err != nil {
		return 0.0, err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	stringBody := string(body)
	startIndex := strings.Index(stringBody, "minio_cluster_capacity_raw_free_bytes{server=\"127.0.0.1:9000\"}")

	total, err := strconv.ParseFloat(stringBody[startIndex+63:startIndex+63+16], 64)

	if err != nil {
		return 0.0, err
	}

	return total - fileSize, nil
}
