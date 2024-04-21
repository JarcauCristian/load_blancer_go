package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func main() {
	var minio, err = NewMinIO()

	if err != nil {
		fmt.Println("Something happened when creating the instance!")
		return
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.Use(CORSMiddleware())

	r.GET("/balancer/get_all_objects", func(c *gin.Context) {
		extension, okExtension := c.GetQuery("extension")

		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {
				if !okExtension {
					c.JSON(400, gin.H{
						"message": "Parameter extension is required!",
					})
				} else {
					data, err := minio.getAllObjects(extension)
					if err != nil {
						c.JSON(500, gin.H{
							"message": fmt.Sprintf("An error occurred when fetching all objects: %s", err.Error()),
						})
					} else {
						c.JSON(200, data)
					}
				}
			}
		}
	})

	r.POST("/balancer/get_objects", func(c *gin.Context) {
		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {

				var datasetPath Path
				err := c.BindJSON(&datasetPath)
				if err != nil {
					c.JSON(400, gin.H{
						"message": "Body is not correct!",
					})
				} else {
					data, err := getObject(datasetPath.DatasetPath)
					if err != nil {
						c.JSON(500, gin.H{
							"message": fmt.Sprintf("An error occurred when fetching all objects: %s", err.Error()),
						})
					} else {
						c.JSON(200, gin.H{
							"shareURL": data,
						})
					}
				}
			}
		}
	})

	r.POST("/balancer/add_instance", func(c *gin.Context) {

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

	r.POST("/balancer/add_instances", func(c *gin.Context) {

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

	r.POST("/balancer/search_by_tags", func(c *gin.Context) {
		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {

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
			}
		}
	})

	r.POST("/balancer/search_by_content_type", func(c *gin.Context) {

		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {

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
			}
		}
	})

	r.POST("/balancer/search_by_extension", func(c *gin.Context) {

		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {

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
			}
		}
	})

	r.GET("/balancer/get_object", func(c *gin.Context) {

		datasetPath, exists := c.GetQuery("dataset_path")
		forever, foreverExists := c.GetQuery("forever")

		if !exists {
			c.JSON(400, gin.H{
				"message": "dataset_path parameter is required!",
			})
		} else if !foreverExists {
			c.JSON(400, gin.H{
				"message": "forever parameter is required!",
			})
		} else {
			foreverBool, _ := strconv.ParseBool(forever)

			if foreverBool {
				datasetPath = "dataspace/" + datasetPath
			} else {
				datasetPath = "temp/" + datasetPath
			}

			data, err := minio.findObject(datasetPath)

			if err != nil {
				c.JSON(500, gin.H{
					"message": "Something went wrong when getting the object!",
				})
			} else {
				c.JSON(200, gin.H{
					"url": data,
				})
			}
		}
	})

	r.GET("/balancer/get/object", func(c *gin.Context) {

		datasetPath, exists := c.GetQuery("path")

		if !exists {
			c.JSON(400, gin.H{
				"message": "dataset_path parameter is required!",
			})
		} else {
			datasetPath = strings.Replace(datasetPath, "'", "\"", -1)
			var datasetPaths []string

			if err := json.Unmarshal([]byte(datasetPath), &datasetPaths); err != nil {
				c.JSON(500, gin.H{"message": err.Error()})
				return
			}

			data, err := minio.getDirectObject(datasetPaths)

			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			defer data.Close()

			c.Header("Content-Disposition", "attachment; filename=downloaded_file.csv")

			_, err = io.Copy(c.Writer, data)
			if err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
		}
	})

	r.GET("/balancer/list_location", func(c *gin.Context) {
		path, exists := c.GetQuery("path")

		if !exists {
			c.JSON(400, gin.H{
				"message": "path parameter is required!",
			})
		} else {
			files, err := minio.listPath(path)

			if err != nil {
				c.JSON(404, gin.H{
					"message": "Path not found!",
				})
			}

			c.JSON(200, gin.H{
				"files": files,
			})
		}
	})

	r.PUT("/balancer/put_object", func(c *gin.Context) {
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
			content := []byte(file)
			contentSize := float64(len(content))
			location, err := minio.putObject(content, fileName, mapTags, contentSize)

			if err != nil {
				c.JSON(500, gin.H{
					"message": "Something happened when trying to upload the object!",
				})
			} else {
				c.JSON(201, gin.H{
					"location": location,
				})
			}
		}
	})

	r.PUT("/balancer/upload", func(c *gin.Context) {

		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]
			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {
				file, header, err := c.Request.FormFile("file")
				if err != nil {
					c.JSON(400, gin.H{
						"message": "File is missing!",
					})
					return
				}

				defer func() {
					if file != nil {
						if err := file.Close(); err != nil {
							c.JSON(500, gin.H{"message": "Could not close the file!"})
						}
					}
				}()

				buffer := make([]byte, 1024)
				var buf bytes.Buffer
				for {
					bytesRead, err := file.Read(buffer)
					if err != nil && err != io.EOF {
						c.String(http.StatusInternalServerError, "Error reading the file")
						return
					}
					if bytesRead == 0 {
						break
					}
					buf.Write(buffer[:bytesRead])
				}

				tags, tagsExists := c.GetPostForm("tags")
				fileName, fileNameExists := c.GetPostForm("name")
				temporary, temporaryExists := c.GetPostForm("temporary")

				var tagData map[string]interface{}

				marshalError := json.Unmarshal([]byte(tags), &tagData)

				fileSize := header.Size
				contentType := "application/octet-stream"

				if err != nil && !tagsExists && !temporaryExists && marshalError != nil && !fileNameExists {
					c.JSON(400, gin.H{
						"message": "Please provide all the fields!",
					})
				}

				var mapTags = make(map[string]string, len(tagData))
				for k, v := range tagData {
					mapTags[k] = v.(string)
				}

				boolTemporary, _ := strconv.ParseBool(temporary)

				reader := bytes.NewReader(buf.Bytes())

				results, err := minio.uploadFile(reader, mapTags, float64(fileSize), fileName, contentType, boolTemporary)

				if err != nil {
					c.JSON(500, gin.H{
						"message": "Something happened when trying to upload the file!",
					})
				} else {
					if len(results) == 0 {
						c.JSON(404, gin.H{
							"message": "Could not upload the file in any of the instances!",
						})
					} else {
						c.JSON(201, results)
					}
				}
			}
		}
	})

	r.POST("/balancer/upload_free", func(c *gin.Context) {
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			c.JSON(400, gin.H{
				"message": "File is missing!",
			})
			return
		}

		defer func() {
			if file != nil {
				if err := file.Close(); err != nil {
					c.JSON(500, gin.H{"message": "Could not close the file!"})
				}
			}
		}()

		tags, tagsExists := c.GetPostForm("tags")
		fileName, fileNameExists := c.GetPostForm("name")
		temporary, temporaryExists := c.GetPostForm("temporary")

		var tagData map[string]interface{}

		marshalError := json.Unmarshal([]byte(tags), &tagData)

		fileSize := header.Size
		contentType := "application/octet-stream"

		buffer := make([]byte, 1024)
		var buf bytes.Buffer
		for {
			bytesRead, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				c.String(http.StatusInternalServerError, "Error reading the file")
				return
			}
			if bytesRead == 0 {
				break
			}
			buf.Write(buffer[:bytesRead])
		}

		if err != nil && !tagsExists && !temporaryExists && marshalError != nil && !fileNameExists {
			c.JSON(400, gin.H{
				"message": "Please provide all the fields!",
			})
		}
		reader := bytes.NewReader(buf.Bytes())

		var mapTags = make(map[string]string, len(tagData))
		for k, v := range tagData {
			mapTags[k] = v.(string)
		}

		boolTemporary, _ := strconv.ParseBool(temporary)

		results, err := minio.uploadFile(reader, mapTags, float64(fileSize), fileName, contentType, boolTemporary)

		if err != nil {
			c.JSON(500, gin.H{
				"message": "Something happened when trying to upload the file!",
			})
		} else {
			if len(results) == 0 {
				c.JSON(404, gin.H{
					"message": "Could not upload the file in any of the instances!",
				})
			} else {
				c.JSON(201, results)
			}
		}
	})

	r.DELETE("/balancer/delete_path", func(c *gin.Context) {
		authorization := c.Request.Header["Authorization"]

		if len(authorization) == 0 {
			c.JSON(400, gin.H{
				"message": "You need to pass the authorization header!",
			})
		} else {
			tokenString := strings.Split(c.Request.Header["Authorization"][0], " ")[1]

			if tokenString == "" {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			}

			verification := verifyToken(tokenString)

			if !verification {
				c.JSON(401, gin.H{
					"message": "You are unauthorized!",
				})
			} else {
				path, exists := c.GetQuery("path")
				temporary, tempExists := c.GetQuery("temp")

				if !exists {
					c.JSON(400, gin.H{
						"message": "Required parameter path not provided!",
					})
					return
				}

				if !tempExists {
					c.JSON(400, gin.H{
						"message": "Required parameter temp not provided!",
					})
					return
				} else {
					boolTemp, _ := strconv.ParseBool(temporary)
					err := minio.deleteFile(path, boolTemp)

					if err != nil {
						c.JSON(500, gin.H{
							"message": "Something happened when deleting the path!",
						})
					} else {
						c.JSON(200, gin.H{
							"message": "Path deleted successfully!",
						})
					}
				}
			}
		}
	})

	err = r.Run(":9000")
	if err != nil {
		return
	}
}

func verifyToken(tokenString string) bool {
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	req, _ := http.NewRequest("GET", "https://keycloak.sedimark.work/auth/realms/react-keycloak/protocol/openid-connect/userinfo", nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenString))
	response, err := client.Do(req)
	if err != nil {
		return false
	}
	if response.StatusCode != 200 {
		return false
	}
	return true
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
