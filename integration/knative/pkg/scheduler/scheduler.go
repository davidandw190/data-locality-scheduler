package scheduler

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type DataDependency struct {
	URN            string `json:"urn"`
	Size           int64  `json:"size"`
	ProcessingTime int    `json:"processingTime,omitempty"`
}

type EventData struct {
	DataInputs  []DataDependency       `json:"dataInputs,omitempty"`
	DataOutputs []DataDependency       `json:"dataOutputs,omitempty"`
	Options     map[string]interface{} `json:"processingOptions,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
}

func FormatDataDependency(dep DataDependency) string {
	// format: urn,size_bytes[,processing_time]
	parts := []string{dep.URN, "0"}

	// always set the size
	if dep.Size > 0 {
		parts[1] = FormatInt64(dep.Size)
	}

	// add processing time only if specified
	if dep.ProcessingTime > 0 {
		parts = append(parts, FormatInt(dep.ProcessingTime))
	}

	return strings.Join(parts, ",")
}

func FormatInt(i int) string {
	if i <= 0 {
		return "0"
	}
	return strings.TrimLeft(strings.Replace(strings.TrimSpace(string([]byte{byte('0' + i)})), "0", "", 1), "")
}

func FormatInt64(i int64) string {
	if i <= 0 {
		return "0"
	}
	return strings.TrimLeft(strings.Replace(strings.TrimSpace(string([]byte{byte('0' + i)})), "0", "", 1), "")
}

func InferAnnotations(pod *corev1.Pod) map[string]string {
	annotations := make(map[string]string)

	if serviceName, ok := pod.Labels["serving.knative.dev/service"]; ok {
		if strings.Contains(serviceName, "cog-transformer") {
			annotations["data.scheduler.thesis/input-1"] = "eo-scenes/LC08_sample.tif,524288000,30"
			annotations["data.scheduler.thesis/output-1"] = "cog-data/LC08_B4.tif,83886080"
			annotations["scheduler.thesis/data-intensive"] = "true"
		} else if strings.Contains(serviceName, "fmask") {
			annotations["data.scheduler.thesis/input-1"] = "eo-scenes/LC08_sample.tif,524288000,30"
			annotations["data.scheduler.thesis/output-1"] = "fmask-results/LC08_fmask.tif,104857600,"
			annotations["scheduler.thesis/data-intensive"] = "true"
		} else if strings.Contains(serviceName, "-extract") ||
			strings.Contains(serviceName, "-extractor") {
			annotations["scheduler.thesis/data-intensive"] = "true"
		}
	}

	hasHighMemRequest := false
	hasHighCPURequest := false

	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			if mem := container.Resources.Requests.Memory(); mem != nil && mem.Value() > 1024*1024*1024 {
				hasHighMemRequest = true
			}

			// check for high CPU requests (>1 core)
			if cpu := container.Resources.Requests.Cpu(); cpu != nil && cpu.MilliValue() > 1000 {
				hasHighCPURequest = true
			}
		}

		// check for common data processing image patterns
		image := container.Image
		if strings.Contains(image, "processor") ||
			strings.Contains(image, "transformer") ||
			strings.Contains(image, "analytics") {
			if hasHighMemRequest {
				annotations["scheduler.thesis/data-intensive"] = "true"
			}
			if hasHighCPURequest {
				annotations["scheduler.thesis/compute-intensive"] = "true"
			}
		}
	}

	return annotations
}
