package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

func (minioInstance *MinIO) addInstance(instance InstanceModel) error {
	var config []Config
	reader, err := os.Open("./configs/config.json")

	if err != nil {
		return err
	}

	data, readErr := io.ReadAll(reader)

	if readErr != nil {
		return readErr
	}

	if err = json.Unmarshal(data, &config); err != nil {
		return err
	}

	splits := strings.Split(instance.Url, ":")

	var secure bool
	if splits[0] == "https" {
		secure = true
	} else {
		secure = false
	}

	endpoint := splits[1][2:] + ":" + splits[2]
	accessKey := instance.AccessKey
	secretKey := instance.SecretKey

	alias := fmt.Sprintf("minio%d", minioInstance.currentIndex)
	minioInstance.aliases[instance.Url] = alias
	minioInstance.currentIndex++
	minioInstance.tokens[instance.Url] = instance.Token

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return err
	}
	minioInstance.clients[instance.Url] = minioClient

	cmd := exec.Command("./mc.exe", "alias", "set", alias, instance.Url, accessKey, secretKey)
	if err = cmd.Run(); err != nil {
		return err
	}

	addConfig := Config{
		Token:     instance.Token,
		Alias:     alias,
		SecretKey: base64.StdEncoding.EncodeToString([]byte(secretKey)),
		AccessKey: base64.StdEncoding.EncodeToString([]byte(accessKey)),
		Site:      instance.Url,
	}

	config = append(config, addConfig)

	file, err := os.OpenFile("./configs/config.json", os.O_CREATE, os.ModePerm)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	if err = encoder.Encode(config); err != nil {
		return err
	}

	return nil
}

func (minioInstance *MinIO) addInstances(servers ServersModel) error {
	var config []Config
	reader, err := os.Open("./configs/config.json")

	if err != nil {
		return err
	}

	data, readErr := io.ReadAll(reader)

	if readErr != nil {
		return readErr
	}

	if err = json.Unmarshal(data, &config); err != nil {
		return err
	}

	for _, server := range servers.Instances {
		splits := strings.Split(server.Url, ":")

		var secure bool
		if splits[0] == "https" {
			secure = true
		} else {
			secure = false
		}

		endpoint := splits[1][2:] + ":" + splits[2]
		accessKey := server.AccessKey
		secretKey := server.SecretKey

		alias := fmt.Sprintf("minio%d", minioInstance.currentIndex)
		minioInstance.aliases[server.Url] = alias
		minioInstance.currentIndex++
		minioInstance.tokens[server.Url] = server.Token

		minioClient, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: secure,
		})
		if err != nil {
			return err
		}
		minioInstance.clients[server.Url] = minioClient

		cmd := exec.Command("./mc.exe", "alias", "set", alias, server.Url, accessKey, secretKey)
		if err = cmd.Run(); err != nil {
			return err
		}

		addConfig := Config{
			Token:     server.Token,
			Alias:     alias,
			SecretKey: base64.StdEncoding.EncodeToString([]byte(secretKey)),
			AccessKey: base64.StdEncoding.EncodeToString([]byte(accessKey)),
			Site:      server.Url,
		}

		config = append(config, addConfig)
	}
	file, err := os.OpenFile("./configs/config.json", os.O_CREATE, os.ModePerm)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	if err = encoder.Encode(config); err != nil {
		return err
	}

	return nil
}

func (minioInstance *MinIO) Healths() (map[string]string, error) {
	var wg sync.WaitGroup

	aliasesLength := len(minioInstance.aliases)

	var health = make(map[string]string, aliasesLength)

	wg.Add(aliasesLength)
	for url, alias := range minioInstance.aliases {

		go func(url, alias string) {
			defer wg.Done()
			status, err := getHealth(alias)
			if err != nil {
				fmt.Println("An error occurred!")
			}
			if status == "success" {
				health[url] = alias
			}
		}(url, alias)
	}

	wg.Wait()

	return health, nil
}

func (minioInstance *MinIO) searchByTags(tags TagsModel) ([]map[string][]string, error) {
	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	healthyInstancesLength := len(healthyInstances)
	var findings = make([]map[string][]string, healthyInstancesLength)
	index := 0

	wg.Add(healthyInstancesLength)
	for k, v := range healthyInstances {
		alias := []string{k, v}
		go func(alias []string, tags TagsModel) {
			defer wg.Done()
			finding, err := searchTags(alias, tags.Tags)
			if err != nil {
				fmt.Println("An error occurred!")
			} else {
				findings[index] = finding
				index++
			}
		}(alias, tags)

	}

	wg.Wait()

	return findings, nil
}

func (minioInstance MinIO) putObject(content []byte, fileName string, tags map[string]interface{}, fileSize float64) error {
	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	healthyInstancesLength := len(healthyInstances)
	var spaces = make([]map[string]float64, healthyInstancesLength)
	index := 0

	wg.Add(healthyInstancesLength)
	for k, v := range healthyInstances {
		alias := []string{k, v}
		token := minioInstance.tokens[k]
		site := k
		go func(alias []string, token string, fileSize float64) {
			defer wg.Done()
			spaceLeft, err := getTotalBytes(alias, token, fileSize)
			if err != nil {
				return
			} else {
				spaces[index] = map[string]float64{site: spaceLeft}
				index++
			}
		}(alias, token, fileSize)
	}

	wg.Wait()

	maxim := 0.0
	var targetSite string
	for _, space := range spaces {
		for k, v := range space {
			if v > maxim {
				targetSite = k
				maxim = v
			}
		}
	}

	var newTags = make(map[string]string, len(tags))
	for k, v := range tags {
		newTags[k] = v.(string)
	}

	object, err := minioInstance.clients[targetSite].PutObject(
		context.Background(),
		"dataspace",
		fileName,
		bytes.NewReader(content),
		-1,
		minio.PutObjectOptions{
			PartSize:    1024 * 1024 * 5,
			UserTags:    newTags,
			ContentType: "application/json",
		},
	)
	if err != nil {
		return err
	}

	fmt.Println(object.Bucket, object.Size, object.Location)

	return nil
}

func (minioInstance MinIO) uploadFile(reader io.Reader, tags map[string]string, fileSize float64, fileName string) error {
	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	healthyInstancesLength := len(healthyInstances)
	var spaces = make([]map[string]float64, healthyInstancesLength)
	index := 0

	wg.Add(healthyInstancesLength)
	for k, v := range healthyInstances {
		alias := []string{k, v}
		token := minioInstance.tokens[k]
		site := k
		go func(alias []string, token string, fileSize float64) {
			defer wg.Done()
			spaceLeft, err := getTotalBytes(alias, token, fileSize)
			if err != nil {
				return
			} else {
				spaces[index] = map[string]float64{site: spaceLeft}
				index++
			}
		}(alias, token, fileSize)
	}

	wg.Wait()
	fmt.Println(spaces)
	maxim := 0.0
	var targetSite string
	for _, space := range spaces {
		for k, v := range space {
			if v > maxim {
				targetSite = k
				maxim = v
			}
		}
	}

	object, err := minioInstance.clients[targetSite].PutObject(
		context.Background(),
		"dataspace",
		fileName,
		reader,
		int64(fileSize),
		minio.PutObjectOptions{
			UserTags:    tags,
			ContentType: "application/json",
		},
	)
	if err != nil {
		return err
	}

	fmt.Println(object.Bucket, object.Size, object.Location)

	return nil
}

type MinIO struct {
	aliases      map[string]string
	clients      map[string]*minio.Client
	tokens       map[string]string
	currentIndex int
}

func NewMinIO() (*MinIO, error) {
	var config []Config
	reader, err := os.Open("./configs/config.json")

	if err != nil {
		fmt.Println("An error occurred when trying to open the config file!")
		return nil, err
	}

	data, readErr := io.ReadAll(reader)

	if readErr != nil {
		fmt.Println("An error occurred when trying to read data from the config file!")
		return nil, err
	}

	jsonErr := json.Unmarshal(data, &config)

	if jsonErr != nil {
		fmt.Println("An error occurred when trying parse data as json!")
		return nil, err
	}

	currentIndex := len(config) + 1
	var aliases = make(map[string]string)
	var clients = make(map[string]*minio.Client)
	var tokens = make(map[string]string)

	for _, line := range config {
		var secure bool
		if strings.Split(line.Site, ":")[0] == "https" {
			secure = true
		} else {
			secure = false
		}
		splits := strings.Split(line.Site, ":")
		endpoint := splits[1][2:] + ":" + splits[2]
		accessKey, base64Err := base64.StdEncoding.DecodeString(line.AccessKey)

		if base64Err != nil {
			return nil, err
		}

		secretKey, base64Err := base64.StdEncoding.DecodeString(line.SecretKey)

		if base64Err != nil {
			return nil, err
		}

		aliases[line.Site] = line.Alias

		minioClient, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(string(accessKey), string(secretKey), ""),
			Secure: secure,
		})
		if err != nil {
			return nil, err
		}
		clients[line.Site] = minioClient
		tokens[line.Site] = line.Token

		cmd := exec.Command("./mc.exe", "alias", "set", line.Alias, line.Site, string(accessKey), string(secretKey))
		if err = cmd.Run(); err != nil {
			return nil, err
		}
	}

	return &MinIO{
		currentIndex: currentIndex,
		aliases:      aliases,
		tokens:       tokens,
		clients:      clients,
	}, nil
}
