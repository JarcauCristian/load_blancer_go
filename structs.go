package main

type InstanceModel struct {
	Url       string `json:"url" binding:"required"`
	Token     string `json:"token" binding:"required"`
	AccessKey string `json:"access_key" binding:"required"`
	SecretKey string `json:"secret_key" binding:"required"`
}

type ServersModel struct {
	Instances []InstanceModel `json:"instances" binding:"required"`
}

type TagsModel struct {
	Tags map[string]string `json:"tags" binding:"required"`
}

type Config struct {
	Site      string `json:"site"`
	Token     string `json:"token"`
	Alias     string `json:"alias"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}
