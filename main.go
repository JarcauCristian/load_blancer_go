package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

func main() {
	var minio, err = NewMinIO()

	if err != nil {
		fmt.Println("Something happened when creating the instance!")
		return
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.MaxMultipartMemory = 100 << 20

	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"https://localhost:3000"}
	config.AllowHeaders = []string{"Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With"}
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}

	r.Use(cors.New(config))

	r.GET("/get_all_objects", func(c *gin.Context) {
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

	r.POST("/get_objects", func(c *gin.Context) {
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

	r.POST("/search_by_content_type", func(c *gin.Context) {

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

	r.POST("/search_by_extension", func(c *gin.Context) {

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

	r.PUT("/put_object", func(c *gin.Context) {
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
			}
		}
	})

	r.PUT("/upload", func(c *gin.Context) {

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

				//var uploadModel UploadModel
				//
				//if err := c.ShouldBind(&uploadModel); err != nil {
				//	c.JSON(400, gin.H{
				//		"message": "Format is incorrect!",
				//	})
				//}
				file, err := c.FormFile("file")
				if err != nil {
					c.JSON(400, gin.H{
						"message": "File is missing!",
					})
				}

				//tags := uploadModel.Tags
				//
				//var mapTags map[string]string
				//
				//err = json.Unmarshal([]byte(tags), &mapTags)
				//
				//if err != nil {
				//	c.JSON(400, gin.H{
				//		"message": "Tags are not in the right format!",
				//	})
				//}

				fileSize := file.Size
				contentType := file.Header["Content-Type"][0]

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
				mapTags := map[string]string{}
				result, err := minio.uploadFile(reader, mapTags, float64(fileSize), file.Filename, contentType)

				if err != nil {
					c.JSON(500, gin.H{
						"message": "Something happened when trying to upload the file!",
					})
				} else {
					c.JSON(201, gin.H{
						"message": result,
					})
				}
			}
		}
	})

	err = r.Run(":8000")
	if err != nil {
		return
	}
}

func verifyToken(tokenString string) bool {
	publicKeyPath := "./public_key.pem"

	publicKeyBytes, err := ioutil.ReadFile(publicKeyPath)
	if err != nil {
		fmt.Println("Error reading public key:", err)
		return false
	}

	// Parse the RSA public key
	publicKeyBlock, _ := pem.Decode(publicKeyBytes)
	if publicKeyBlock == nil {
		fmt.Println("Error decoding public key")
		return false
	}

	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		fmt.Println("Error parsing public key:", err)
		return false
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return publicKey.(*rsa.PublicKey), nil
	})

	if err != nil {
		fmt.Println("JWT parsing error:", err)
		return false
	}

	if token.Valid {
		claims, ok := token.Claims.(jwt.MapClaims)

		if err != nil || !ok {
			fmt.Println("Getting error!")
			return false
		}

		exp, ok := claims["exp"].(float64)

		if !ok {
			fmt.Println("Exp not found")
			return false
		}

		expirationTime := time.Unix(int64(exp), 0)
		currentTime := time.Now()
		return currentTime.Before(expirationTime)
	}
	return false
}
