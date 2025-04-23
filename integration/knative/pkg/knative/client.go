package knative

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Client struct {
	clientset *kubernetes.Clientset
}

func NewClient(clientset *kubernetes.Clientset) (*Client, error) {
	return &Client{
		clientset: clientset,
	}, nil
}

func IsKnativePod(pod *corev1.Pod) bool {
	if _, ok := pod.Labels["serving.knative.dev/service"]; ok {
		return true
	}
	if _, ok := pod.Labels["serving.knative.dev/revision"]; ok {
		return true
	}
	if _, ok := pod.Labels["serving.knative.dev/configuration"]; ok {
		return true
	}
	return false
}

func (c *Client) GetServiceAnnotations(serviceName, namespace string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resource, err := c.clientset.RESTClient().
		Get().
		AbsPath("/apis/serving.knative.dev/v1").
		Namespace(namespace).
		Resource("services").
		Name(serviceName).
		DoRaw(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to get Knative service %s/%s: %w", namespace, serviceName, err)
	}

	// parsing the JSON response to extract annotations
	var serviceData map[string]interface{}
	if err := json.Unmarshal(resource, &serviceData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service data: %w", err)
	}

	// extracting annotations from the service
	annotations := make(map[string]string)

	metadata, ok := serviceData["metadata"].(map[string]interface{})
	if !ok {
		return annotations, nil
	}

	if metaAnnotations, ok := metadata["annotations"].(map[string]interface{}); ok {
		for k, v := range metaAnnotations {
			if vs, ok := v.(string); ok {
				annotations[k] = vs
			}
		}
	}

	// filtering to only include scheduler-related annotations
	schedulerAnnotations := make(map[string]string)
	for k, v := range annotations {
		if strings.HasPrefix(k, "data.scheduler.thesis/") ||
			strings.HasPrefix(k, "scheduler.thesis/") {
			schedulerAnnotations[k] = v
		}
	}

	return schedulerAnnotations, nil
}

func (c *Client) GetRevisionAnnotations(revisionName, namespace string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// since we don't have direct access to the Knative API types in this client,
	// we'll use the Kubernetes API to get the custom resource
	resource, err := c.clientset.RESTClient().
		Get().
		AbsPath("/apis/serving.knative.dev/v1").
		Namespace(namespace).
		Resource("revisions").
		Name(revisionName).
		DoRaw(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to get Knative revision %s/%s: %w", namespace, revisionName, err)
	}

	// parse the JSON response to extract annotations
	var revisionData map[string]interface{}
	if err := json.Unmarshal(resource, &revisionData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal revision data: %w", err)
	}

	// extracting annotations from the revision
	annotations := make(map[string]string)

	metadata, ok := revisionData["metadata"].(map[string]interface{})
	if !ok {
		return annotations, nil
	}

	if metaAnnotations, ok := metadata["annotations"].(map[string]interface{}); ok {
		for k, v := range metaAnnotations {
			if vs, ok := v.(string); ok {
				annotations[k] = vs
			}
		}
	}

	schedulerAnnotations := make(map[string]string)
	for k, v := range annotations {
		if strings.HasPrefix(k, "data.scheduler.thesis/") ||
			strings.HasPrefix(k, "scheduler.thesis/") {
			schedulerAnnotations[k] = v
		}
	}

	return schedulerAnnotations, nil
}

func (c *Client) GetPodForRevision(revisionName, namespace string) (*corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("serving.knative.dev/revision=%s", revisionName),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list pods for revision %s/%s: %w", namespace, revisionName, err)
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for revision %s/%s", namespace, revisionName)
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return &pod, nil
		}
	}

	return &pods.Items[0], nil
}
