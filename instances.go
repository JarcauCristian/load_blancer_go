package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

	cmd := exec.Command("./mc", "alias", "set", alias, instance.Url, accessKey, secretKey)
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

		cmd := exec.Command("./mc", "alias", "set", alias, server.Url, accessKey, secretKey)
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

func (minioInstance *MinIO) listPath(path string) ([]string, error) {
	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return nil, err
	}

	var findPath []string

	var wg sync.WaitGroup

	wg.Add(len(healthyInstances))
	for k, v := range healthyInstances {
		alias := []string{k, v}

		go func(alias []string, datasetPath string) {
			defer wg.Done()

			finding, err := search(alias, datasetPath)

			if err != nil {
				fmt.Println("Is not present here!")
			}

			if finding != "" {
				findPath = []string{alias[0], alias[1]}
			}
		}(alias, path)
	}

	wg.Wait()

	files, err := listMinioPath(findPath, path)

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (minioInstance *MinIO) findObject(datasetPath string) (string, error) {
	healthyInstances, err := minioInstance.Healths()

	if err != nil {
		return "", err
	}

	var findDataset string
	var wg sync.WaitGroup

	wg.Add(len(healthyInstances))
	for k, v := range healthyInstances {
		alias := []string{k, v}

		go func(alias []string, datasetPath string) {
			defer wg.Done()

			finding, err := search(alias, datasetPath)

			if err != nil {
				fmt.Println("Is not present here!")
			}

			if finding != "" {
				findDataset = finding
			}
		}(alias, datasetPath)
	}

	wg.Wait()

	shareUrl, err := minioInstance.getObject(findDataset, datasetPath)

	if err != nil {
		return "", err
	}

	return shareUrl, nil
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

func (minioInstance *MinIO) searchByContentType(contentType string) ([]map[string][]string, error) {
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
		go func(alias []string, contentType string) {
			defer wg.Done()
			finding, err := searchContentType(alias, contentType)
			if err != nil {
				fmt.Println("An error occurred!")
			} else {
				findings[index] = finding
				index++
			}
		}(alias, contentType)

	}

	wg.Wait()

	return findings, nil
}

func (minioInstance *MinIO) searchByExtension(extension string) ([]map[string][]string, error) {
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
		go func(alias []string, extension string) {
			defer wg.Done()
			finding, err := searchExtension(alias, extension)
			if err != nil {
				fmt.Println("An error occurred!")
			} else {
				findings[index] = finding
				index++
			}
		}(alias, extension)

	}

	wg.Wait()

	return findings, nil
}

func (minioInstance *MinIO) getDatasetTags(datasetPath string) (map[string]string, error) {
	var mp map[string]interface{}

	cmdArgs := []string{"./mc", "tag", "list", datasetPath, "--json"}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(stdout.Bytes(), &mp)
	if err != nil {
		return nil, err
	}

	var tags = make(map[string]string)
	if mp["status"].(string) == "success" {
		switch v := mp["tagset"].(type) {
		case map[string]interface{}:
			for tagName, tagValue := range v {
				tags[tagName] = tagValue.(string)
			}
		default:
			return nil, errors.New("not none type")
		}
	} else {
		return nil, errors.New("tags does not exist for this object")
	}

	return tags, nil
}

func (minioInstance *MinIO) getAllObjects(extension string) ([]map[string]map[string]string, error) {
	if extension == "" {
		extension = "csv"
	}

	findings, err := minioInstance.searchByExtension(extension)

	if err != nil {
		return nil, err
	}

	var finalResult []map[string]map[string]string
	for _, value := range findings {
		var wg sync.WaitGroup
		for k, v := range value {
			var objectWithTags []map[string]map[string]string
			wg.Add(len(v))
			for _, datasetPath := range v {
				go func(path string, key string) {
					defer wg.Done()
					result, err := minioInstance.getDatasetTags(path)
					if err == nil && len(result) > 0 && result != nil {
						objectWithTags = append(objectWithTags, map[string]map[string]string{fmt.Sprintf("%s#%s", key, path): result})
					}

				}(datasetPath, k)
			}
			wg.Wait()
			if len(objectWithTags) > 0 && objectWithTags != nil {
				finalResult = append(finalResult, objectWithTags...)
			}
		}
	}
	return finalResult, nil

}

func (minioInstance *MinIO) putObject(content []byte, fileName string, tags map[string]interface{}, fileSize float64) (string, error) {
	if minioInstance.robinIndex == minioInstance.currentIndex-1 {
		minioInstance.robinIndex = 0
	}

	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return "", err
	}

	if strings.Contains(fileName, ".csv") {
		fileName = strings.Replace(fileName, ".csv", "", 1)
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
	if len(spaces) == minioInstance.currentIndex-1 {
		for k, v := range spaces[minioInstance.robinIndex] {
			if v > 0 {
				targetSite = k
				minioInstance.robinIndex++
			} else {
				for i := minioInstance.robinIndex; i < len(spaces); i++ {
					leftSpace := false
					for k, v := range spaces[i] {
						if v > 0 {
							targetSite = k
							minioInstance.robinIndex++
							leftSpace = true
						}
					}
					if leftSpace {
						break
					}
				}
			}
		}
	} else {
		for _, space := range spaces {
			for k, v := range space {
				if v > maxim {
					targetSite = k
					maxim = v
				}
			}
		}
		minioInstance.robinIndex++
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
			PartSize: 1024 * 1024 * 5,
			UserTags: newTags,
		},
	)
	if err != nil {
		return "", err
	}

	fmt.Println(object.Bucket, object.Size, object.Location)

	return targetSite + "=" + object.Bucket + "=" + fileName, nil
}

func (minioInstance *MinIO) uploadFile(reader io.Reader, tags map[string]string, fileSize float64, fileName string, contentType string, temporary bool) (map[string]string, error) {
	if minioInstance.robinIndex == minioInstance.currentIndex-1 {
		minioInstance.robinIndex = 0
	}

	fmt.Println(minioInstance.robinIndex)

	healthyInstances, err := minioInstance.Healths()
	if err != nil {
		return nil, err
	}

	if contentType == "text/csv" {
		contentType = "application/octet-stream"
	}

	if strings.Contains(fileName, ".csv") {
		fileName = strings.Replace(fileName, ".csv", "", 1)
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

	fmt.Println(spaces)
	fmt.Printf("CurrentIndex: %d\n", minioInstance.currentIndex)

	if len(spaces) == minioInstance.currentIndex-1 {
		for k, v := range spaces[minioInstance.robinIndex] {
			// First Case it selects as the target site the site at the current robinIndex if value is greater then 0
			if v > 0 {
				targetSite = k
				minioInstance.robinIndex++
			} else {
				// Second Case goes through all the remaining instances if for the current instance gets a negative value for the size
				for i := minioInstance.robinIndex; i < len(spaces); i++ {
					leftSpace := false
					for k, v := range spaces[i] {
						if v > 0 {
							targetSite = k
							minioInstance.robinIndex++
							leftSpace = true
						}
					}
					if leftSpace {
						break
					}
				}
			}
		}
	} else {
		for _, space := range spaces {
			for k, v := range space {
				if v > maxim {
					targetSite = k
					maxim = v
				}
			}
		}
		minioInstance.robinIndex++
	}

	var bucketName string

	if temporary {
		bucketName = "temp"
	} else {
		bucketName = "dataspace"
	}

	fmt.Printf("Target Site: %s\n", targetSite)

	object, err := minioInstance.clients[targetSite].PutObject(
		context.Background(),
		bucketName,
		fileName,
		reader,
		int64(fileSize),
		minio.PutObjectOptions{
			UserTags:    tags,
			ContentType: contentType,
		},
	)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	result := map[string]string{"location": object.Bucket + "/" + fileName, "size": strconv.Itoa(int(object.Size))}

	return result, nil
}

func (minioInstance *MinIO) getObject(url string, datasetPath string) (string, error) {
	path := fmt.Sprintf("%s/%s", minioInstance.aliases[url], datasetPath)
	cmdArgs := []string{"./mc", "share", "download", "--expire", "10m", "--json", path}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(stdout.Bytes(), &data)

	if err != nil {
		return "", err
	}
	fmt.Println(data)

	if data["status"].(string) != "success" {
		return "", errors.New("could not get download link for the object")
	}

	return data["share"].(string), nil
}

type MinIO struct {
	aliases      map[string]string
	clients      map[string]*minio.Client
	tokens       map[string]string
	currentIndex int
	robinIndex   int
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
	robinIndex := 0
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
		endpoint := splits[1][2:]
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

		cmd := exec.Command("./mc", "alias", "set", line.Alias, line.Site, string(accessKey), string(secretKey))
		if err = cmd.Run(); err != nil {
			return nil, err
		}
	}

	return &MinIO{
		currentIndex: currentIndex,
		aliases:      aliases,
		tokens:       tokens,
		clients:      clients,
		robinIndex:   robinIndex,
	}, nil
}
