package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func getHealth(alias string) (string, error) {
	cmdArgs := []string{"./mc", "ping", alias, "--json", "--count", "1"}
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
	cmdArgs := []string{"./mc", "find", alias[1]}
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
		findings = append(findings, line)
	}

	return map[string][]string{alias[0]: findings}, nil
}

func listMinioPath(alias []string, path string) ([]string, error) {
	cmdArgs := []string{"./mc", "ls", alias[1] + "/" + path, "--json"}
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(&stdout)

	var files []string
	for scanner.Scan() {
		line := scanner.Text()
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return nil, err
		}

		if data["status"].(string) == "success" && data["type"].(string) == "file" {
			files = append(files, data["key"].(string))
		}
	}

	return files, nil
}

func searchContentType(alias []string, contentType string) (map[string][]string, error) {
	cmdArgs := []string{"./mc", "find", alias[1], fmt.Sprintf("--metadata=Content-Type=%s", contentType)}

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
		findings = append(findings, line)
	}

	return map[string][]string{alias[0]: findings}, nil
}

func searchExtension(alias []string, extension string) (map[string][]string, error) {
	cmdArgs := []string{"./mc", "find", alias[1], fmt.Sprintf("--name=*.%s", extension)}

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
		findings = append(findings, line)
	}

	return map[string][]string{alias[0]: findings}, nil
}

func getObject(datasetPath string) (string, error) {
	cmdArgs := []string{"./mc", "share", "download", "-E 10m", "--json", datasetPath}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	var mp map[string]interface{}
	err = json.Unmarshal(stdout.Bytes(), &mp)

	if err != nil {
		return "", err
	}

	return mp["share"].(string), nil
}

func search(alias []string, datasetPath string) (string, error) {
	cmdArgs := []string{"./mc", "find", alias[1]}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.Join(strings.Split(scanner.Text(), "/")[1:], "/")

		if datasetPath == line {
			return alias[0], nil
		}
	}

	return "", nil
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
	pattern := "[^0-9+e\\-\\.$]"

	fmt.Printf("Start index: %d\nstringBody: %s\n", startIndex, stringBody[startIndex+63:startIndex+63+16])

	re := regexp.MustCompile(pattern)
	processedInput := re.ReplaceAllString(stringBody[startIndex+63:startIndex+63+16], " ")
	processedInput = strings.Replace(processedInput, " ", "", -1)

	total, err := strconv.ParseFloat(processedInput, 64)

	if err != nil {
		fmt.Println(err)
		return 0.0, err
	}

	return total - fileSize, nil
}
