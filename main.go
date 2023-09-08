package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"mime/multipart"
)

func main() {
	var minio, err = NewMinIO()

	if err != nil {
		fmt.Println("Something happened when creating the instance!")
		return
	}
	//gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.MaxMultipartMemory = 100 << 20
	r.POST("/add_instance", func(c *gin.Context) {
		var instance InstanceModel
		addInstanceErr := c.BindJSON(&instance)
		if addInstanceErr != nil {
			c.JSON(400, gin.H{
				"error_message": "Body is not correct!",
			})
		} else {
			addInstanceErr = minio.addInstance(instance)
			if addInstanceErr != nil {
				c.JSON(500, gin.H{
					"error_message": fmt.Sprintf("Something happened when trying to add the instance!%s", addInstanceErr.Error()),
				})
			} else {
				c.JSON(200, gin.H{
					"message": "Added instance successfully!",
				})
			}
		}
	})

	r.GET("/get_tags", func(c *gin.Context) {
		path, okPath := c.GetQuery("path")
		if !okPath {
			c.JSON(400, gin.H{
				"message": "Body is not correct!",
			})
		} else {
			data, _ := minio.getDatasetTags(path)
			c.JSON(200, gin.H{
				"message": data,
			})
		}
	})

	r.POST("/add_instances", func(c *gin.Context) {
		var servers ServersModel
		addInstancesErr := c.BindJSON(&servers)
		if addInstancesErr != nil {
			c.JSON(400, gin.H{
				"message": "Body is not correct!",
			})
		} else {
			addInstancesErr = minio.addInstances(servers)
			if addInstancesErr != nil {
				c.JSON(500, gin.H{
					"message": fmt.Sprintf("Something happened when trying to add the instance!%s", addInstancesErr.Error()),
				})
			} else {
				c.JSON(200, gin.H{
					"message": "Added instance successfully!",
				})
			}
		}
	})

	r.POST("/search_by_tags", func(c *gin.Context) {
		var tags TagsModel
		searchByTagsErr := c.BindJSON(&tags)
		if searchByTagsErr != nil {
			c.JSON(400, gin.H{
				"message": "Body is not correct!",
			})
		} else {
			searchByTagsOutput, searchByTagsErr := minio.searchByTags(tags)
			if searchByTagsErr != nil {
				c.JSON(500, gin.H{
					"message": fmt.Sprintf("Something happened when trying to add the instance!%s", searchByTagsErr.Error()),
				})
			} else {
				c.JSON(200, searchByTagsOutput)
			}
		}
	})

	r.POST("/search_by_content_type", func(c *gin.Context) {
		var contentType ContentType
		searchByContentTypeErr := c.BindJSON(&contentType)
		if searchByContentTypeErr != nil {
			c.JSON(400, gin.H{
				"message": "Body is not correct!",
			})
		} else {
			searchByContentTypeOutput, searchByContentTypeErr := minio.searchByContentType(contentType.Content)
			if searchByContentTypeErr != nil {
				c.JSON(500, gin.H{
					"message": fmt.Sprintf("Something happened when trying to add the instance!%s", searchByContentTypeErr.Error()),
				})
			} else {
				c.JSON(200, searchByContentTypeOutput)
			}
		}
	})

	r.POST("/search_by_extension", func(c *gin.Context) {
		var extension Extension
		searchByExtensionErr := c.BindJSON(&extension)
		if searchByExtensionErr != nil {
			c.JSON(400, gin.H{
				"message": "Body is not correct!",
			})
		} else {
			searchByExtensionOutput, searchByExtensionErr := minio.searchByExtension(extension.Extension)
			if searchByExtensionErr != nil {
				c.JSON(500, gin.H{
					"message": fmt.Sprintf("Something happened when trying to add the instance!%s", searchByExtensionErr.Error()),
				})
			} else {
				c.JSON(200, searchByExtensionOutput)
			}
		}
	})

	r.PUT("/put_object", func(c *gin.Context) {

		file, okFile := c.GetPostForm("file")
		fileName, okFileName := c.GetPostForm("file_name")
		tags, okTags := c.GetPostForm("tags")

		var mapTags map[string]interface{}

		if !okFile && !okFileName && !okTags {
			c.JSON(400, gin.H{
				"message": "Format is incorrect!",
			})
		} else {
			err := json.Unmarshal([]byte(tags), &mapTags)
			if err != nil {
				c.JSON(500, gin.H{
					"message": "Something happened when unmarshalling!",
				})
			}
		}
		content := []byte(file)
		contentSize := float64(len(content))
		err := minio.putObject(content, fileName, mapTags, contentSize)

		if err != nil {
			c.JSON(500, gin.H{
				"message": "Something happened when trying to upload the object!",
			})
		}

		c.JSON(201, gin.H{
			"message": "Upload object successfully!",
		})
	})

	r.PUT("/upload", func(c *gin.Context) {

		var uploadModel UploadModel

		if err := c.ShouldBind(&uploadModel); err != nil {
			c.JSON(400, gin.H{
				"message": "Format is incorrect!",
			})
		}
		file, err := c.FormFile("file")
		if err != nil {
			c.JSON(400, gin.H{
				"message": "File is missing!",
			})
		}

		tags := uploadModel.Tags

		var mapTags map[string]string

		err = json.Unmarshal([]byte(tags), &mapTags)

		if err != nil {
			c.JSON(400, gin.H{
				"message": "Tags are not in the right format!",
			})
		}

		fileSize := file.Size

		content, err := file.Open()
		defer func(content multipart.File) {
			err := content.Close()
			if err != nil {
				c.JSON(500, gin.H{
					"message": "Error closing the file!",
				})
			}
		}(content)

		if err != nil {
			c.JSON(400, gin.H{
				"message": "File is empty!",
			})
		}
		reader := io.Reader(content)

		err = minio.uploadFile(reader, mapTags, float64(fileSize), file.Filename)

		if err != nil {
			c.JSON(500, gin.H{
				"message": "Something happened when trying to upload the file!",
			})
		} else {
			c.JSON(201, gin.H{
				"message": "File uploaded successfully!",
			})
		}
	})

	err = r.Run(":8000")
	if err != nil {
		return
	}
}
