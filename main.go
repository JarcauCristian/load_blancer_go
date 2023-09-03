package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
)

func main() {
	var minio, err = NewMinIO()

	if err != nil {
		fmt.Println("Something happened when creating the instance!")
		return
	}

	r := gin.Default()
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

	err = r.Run()
	if err != nil {
		return
	}
}
