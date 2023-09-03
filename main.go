package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"sync"
)

type user struct {
	USER     string `json:"user" binding:"required"`
	PASSWORD string `json:"password" binding:"required"`
}

var wg sync.WaitGroup

func main() {
	var minio, err = NewMinIO()

	if err != nil {
		fmt.Println("Something happened when creating the instance!")
		return
	}

	r := gin.Default()
	r.POST("/add_instance", func(c *gin.Context) {
		var instance Instance
		addInstanceErr := c.BindJSON(&instance)
		if addInstanceErr != nil {
			c.JSON(400, gin.H{
				"error_message": "Body is not correct!",
			})
		} else {
			addInstanceErr = minio.addInstance(instance)
			if addInstanceErr != nil {
				c.JSON(500, gin.H{
					"error_message": fmt.Sprintf("Something happened when trying to add the instance!%e", addInstanceErr),
				})
			} else {
				c.JSON(200, gin.H{
					"message": "Success!",
				})
			}
		}
	})
	r.POST("/get_health", func(c *gin.Context) {
		healths, err := minio.Healths()
		if err != nil {
			return
		}
		c.JSON(200, gin.H{
			"message": healths,
		})
	})
	err = r.Run()
	if err != nil {
		return
	}
}
