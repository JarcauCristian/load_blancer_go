package main

type Instance struct {
	Url       string `json:"url" binding:"required"`
	Token     string `json:"token" binding:"required"`
	AccessKey string `json:"access_key" binding:"required"`
	SecretKey string `json:"secret_key" binding:"required"`
}

type Servers struct {
	Instances []Instance `json:"instances" binding:"required"`
}

type Tags struct {
	Instance map[string]string
}

type Config struct {
	Site      string `json:"site"`
	Token     string `json:"token"`
	Alias     string `json:"alias"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}
