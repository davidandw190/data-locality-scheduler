package minio

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"k8s.io/klog/v2"
)

// Client provides an interface to interact with Minio object storage
type Client struct {
	client      *minio.Client
	endpoint    string
	secure      bool
	connected   bool
	lastRefresh time.Time
}

const (
	DefaultAccessKey = "minioadmin"
	DefaultSecretKey = "minioadmin"
)

func NewClient(endpoint string, secure bool) *Client {
	return &Client{
		endpoint:    endpoint,
		secure:      secure,
		connected:   false,
		lastRefresh: time.Time{},
	}
}

func (c *Client) Connect() error {
	if c.connected && c.client != nil {
		return nil
	}

	endpoint := c.endpoint
	if !strings.Contains(endpoint, "://") {
		if c.secure {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}

	host := endpoint
	if strings.Contains(endpoint, "://") {
		parts := strings.SplitN(endpoint, "://", 2)
		if len(parts) == 2 {
			host = parts[1]
		}
	}

	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(DefaultAccessKey, DefaultSecretKey, ""),
		Secure: c.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create Minio client: %w", err)
	}

	c.client = client
	c.connected = true
	klog.V(2).Infof("Connected to Minio server at %s", endpoint)
	return nil
}

func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	buckets, err := c.client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	bucketNames := make([]string, len(buckets))
	for i, bucket := range buckets {
		bucketNames[i] = bucket.Name
	}

	return bucketNames, nil
}

func (c *Client) ListObjects(ctx context.Context, bucketName string) ([]ObjectInfo, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	exists, err := c.client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket %s exists: %w", bucketName, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucketName)
	}

	objectCh := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	var objects []ObjectInfo
	for object := range objectCh {
		if object.Err != nil {
			klog.Warningf("Error listing object in bucket %s: %v", bucketName, object.Err)
			continue
		}

		objects = append(objects, ObjectInfo{
			Key:          object.Key,
			Size:         object.Size,
			LastModified: object.LastModified,
			ContentType:  object.ContentType,
		})
	}

	return objects, nil
}

// ObjectInfo represents information about an object in a bucket
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ContentType  string
}
