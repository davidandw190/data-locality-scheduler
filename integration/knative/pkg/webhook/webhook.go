package webhook

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/davidandw190/data-locality-scheduler/integration/knative/pkg/knative"
	"github.com/davidandw190/data-locality-scheduler/integration/knative/pkg/scheduler"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
)

type PatchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

type WebhookServer struct {
	clientset *kubernetes.Clientset
	server    *http.Server
	knClient  *knative.Client
}

func NewWebhookServer(clientset *kubernetes.Clientset, tlsConfig *tls.Config) (*WebhookServer, error) {
	knClient, err := knative.NewClient(clientset)
	if err != nil {
		return nil, fmt.Errorf("failed to create Knative client: %w", err)
	}

	return &WebhookServer{
		clientset: clientset,
		knClient:  knClient,
		server: &http.Server{
			Addr:      ":8443",
			TLSConfig: tlsConfig,
		},
	}, nil
}

func (ws *WebhookServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/mutate", ws.HandleMutate)
	mux.HandleFunc("/health", ws.HandleHealth)

	ws.server.Handler = mux

	klog.Info("Starting webhook server on :8443")
	return ws.server.ListenAndServeTLS("/etc/webhook/certs/tls.crt", "/etc/webhook/certs/tls.key")
}

func (ws *WebhookServer) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (ws *WebhookServer) HandleMutate(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	var body []byte
	var err error

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is supported", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	if body, err = io.ReadAll(r.Body); err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	var admissionReviewReq admissionv1.AdmissionReview
	if _, _, err := deserializer.Decode(body, nil, &admissionReviewReq); err != nil {
		http.Error(w, fmt.Sprintf("Could not decode body: %v", err), http.StatusBadRequest)
		return
	}

	if admissionReviewReq.Request == nil {
		http.Error(w, "AdmissionReview request is nil", http.StatusBadRequest)
		return
	}

	klog.V(4).Infof("Handling admission review request %s (resource: %s, operation: %s)",
		admissionReviewReq.Request.UID,
		admissionReviewReq.Request.Resource.Resource,
		admissionReviewReq.Request.Operation)

	var pod corev1.Pod
	var admissionResponse *admissionv1.AdmissionResponse

	// we only process if the request is for a pod and the operation is CREATE
	if admissionReviewReq.Request.Resource.Resource == "pods" && admissionReviewReq.Request.Operation == admissionv1.Create {
		// parse pod from the request
		if err := json.Unmarshal(admissionReviewReq.Request.Object.Raw, &pod); err != nil {
			klog.Errorf("Failed to unmarshal pod: %v", err)
			admissionResponse = &admissionv1.AdmissionResponse{
				UID:     admissionReviewReq.Request.UID,
				Allowed: true, // allow but don't modify on error
				Result: &metav1.Status{
					Message: fmt.Sprintf("Failed to unmarshal pod: %v", err),
				},
			}
		} else {
			// process the pod
			patchOps, err := ws.createPatchOperations(&pod)
			if err != nil {
				klog.Errorf("Error creating patch operations: %v", err)
				admissionResponse = &admissionv1.AdmissionResponse{
					UID:     admissionReviewReq.Request.UID,
					Allowed: true,
					Result: &metav1.Status{
						Message: fmt.Sprintf("Error creating patch operations: %v", err),
					},
				}
			} else if len(patchOps) > 0 {
				// create patch bytes for the response
				patchBytes, err := json.Marshal(patchOps)
				if err != nil {
					klog.Errorf("Failed to marshal patch: %v", err)
					admissionResponse = &admissionv1.AdmissionResponse{
						UID:     admissionReviewReq.Request.UID,
						Allowed: true,
						Result: &metav1.Status{
							Message: fmt.Sprintf("Failed to marshal patch: %v", err),
						},
					}
				} else {
					// create admission response with patch
					patchType := admissionv1.PatchTypeJSONPatch
					admissionResponse = &admissionv1.AdmissionResponse{
						UID:       admissionReviewReq.Request.UID,
						Allowed:   true,
						PatchType: &patchType,
						Patch:     patchBytes,
					}
					klog.V(2).Infof("Created patch for pod %s/%s with %d operations",
						pod.Namespace, pod.Name, len(patchOps))
				}
			} else {
				// no patches needed
				klog.V(4).Infof("No patches needed for pod %s/%s", pod.Namespace, pod.Name)
				admissionResponse = &admissionv1.AdmissionResponse{
					UID:     admissionReviewReq.Request.UID,
					Allowed: true,
				}
			}
		}
	} else {
		// default response for non-pod or non-CREATE operations
		admissionResponse = &admissionv1.AdmissionResponse{
			UID:     admissionReviewReq.Request.UID,
			Allowed: true,
		}
	}

	admissionReviewResponse := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionv1.SchemeGroupVersion.String(),
			Kind:       "AdmissionReview",
		},
		Response: admissionResponse,
	}

	respBytes, err := json.Marshal(admissionReviewResponse)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal response: %v", err), http.StatusInternalServerError)
		return
	}

	klog.V(4).Infof("Webhook request processed in %v", time.Since(startTime))

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)
}

func (ws *WebhookServer) createPatchOperations(pod *corev1.Pod) ([]PatchOperation, error) {
	var patches []PatchOperation

	if !knative.IsKnativePod(pod) {
		klog.V(4).Infof("Skipping non-Knative pod: %s/%s", pod.Namespace, pod.Name)
		return patches, nil
	}

	klog.V(3).Infof("Processing Knative pod: %s/%s", pod.Namespace, pod.Name)

	// set scheduler name if not already set to data-locality-scheduler
	if pod.Spec.SchedulerName != "data-locality-scheduler" {
		patches = append(patches, PatchOperation{
			Op:    "replace",
			Path:  "/spec/schedulerName",
			Value: "data-locality-scheduler",
		})
		klog.V(3).Infof("Setting schedulerName for pod %s/%s", pod.Namespace, pod.Name)
	}

	annotations, err := ws.collectAnnotations(pod)
	if err != nil {
		return patches, fmt.Errorf("error collecting annotations: %w", err)
	}

	if len(annotations) > 0 {
		if pod.Annotations == nil {
			patches = append(patches, PatchOperation{
				Op:    "add",
				Path:  "/metadata/annotations",
				Value: annotations,
			})
			klog.V(3).Infof("Adding annotations to pod %s/%s", pod.Namespace, pod.Name)
		} else {
			for k, v := range annotations {
				if currVal, exists := pod.Annotations[k]; !exists || currVal != v {
					// JSON Pointer requires escaping ~ and /
					escapedPath := escapeJSONPointerPath(k)
					patches = append(patches, PatchOperation{
						Op:    "add",
						Path:  "/metadata/annotations/" + escapedPath,
						Value: v,
					})
					klog.V(4).Infof("Adding/updating annotation %s for pod %s/%s", k, pod.Namespace, pod.Name)
				}
			}
		}
	}

	return patches, nil
}

func (ws *WebhookServer) collectAnnotations(pod *corev1.Pod) (map[string]string, error) {
	annotations := make(map[string]string)

	// 1. extract annotations from service/revision if this is a Knative pod
	serviceAnnotations, err := ws.getKnativeServiceAnnotations(pod)
	if err != nil {
		klog.Warningf("Error getting Knative service annotations: %v", err)
	} else {
		for k, v := range serviceAnnotations {
			annotations[k] = v
		}
	}

	// 2. process cloud event data in annotations if present
	cloudEventAnnotations, err := ws.processCloudEventData(pod)
	if err != nil {
		klog.Warningf("Error processing cloud event data: %v", err)
	} else {
		for k, v := range cloudEventAnnotations {
			annotations[k] = v
		}
	}

	// 3. process existing scheduler-related annotations
	for k, v := range pod.Annotations {
		if strings.HasPrefix(k, "data.scheduler.thesis/") ||
			strings.HasPrefix(k, "scheduler.thesis/") {
			annotations[k] = v
		}
	}

	// 4. add inferred annotations based on workload characteristics
	inferredAnnotations := scheduler.InferAnnotations(pod)
	for k, v := range inferredAnnotations {
		if _, exists := annotations[k]; !exists {
			annotations[k] = v
		}
	}

	return annotations, nil
}

func (ws *WebhookServer) getKnativeServiceAnnotations(pod *corev1.Pod) (map[string]string, error) {
	serviceName, hasService := pod.Labels["serving.knative.dev/service"]
	revisionName, hasRevision := pod.Labels["serving.knative.dev/revision"]

	if !hasService && !hasRevision {
		return nil, fmt.Errorf("pod doesn't have Knative service or revision labels")
	}

	if hasService {
		annotations, err := ws.knClient.GetServiceAnnotations(serviceName, pod.Namespace)
		if err == nil && len(annotations) > 0 {
			return annotations, nil
		}
	}

	if hasRevision {
		return ws.knClient.GetRevisionAnnotations(revisionName, pod.Namespace)
	}

	return nil, fmt.Errorf("could not find Knative service or revision")
}

func (ws *WebhookServer) processCloudEventData(pod *corev1.Pod) (map[string]string, error) {
	annotations := make(map[string]string)
	eventDataStr, hasEventData := pod.Annotations["knative.event-data"]
	if !hasEventData {
		return annotations, nil
	}

	var eventData scheduler.EventData
	if err := json.Unmarshal([]byte(eventDataStr), &eventData); err != nil {
		return annotations, fmt.Errorf("failed to unmarshal event data: %w", err)
	}

	// input data dependencies
	for i, input := range eventData.DataInputs {
		key := fmt.Sprintf("data.scheduler.thesis/input-%d", i+1)
		annotations[key] = scheduler.FormatDataDependency(input)
	}

	// output data dependencies
	for i, output := range eventData.DataOutputs {
		key := fmt.Sprintf("data.scheduler.thesis/output-%d", i+1)
		annotations[key] = scheduler.FormatDataDependency(output)
	}

	// processing options
	if eventData.Options != nil {
		if dataIntensive, ok := eventData.Options["dataIntensive"].(bool); ok && dataIntensive {
			annotations["scheduler.thesis/data-intensive"] = "true"
		}

		if preferEdge, ok := eventData.Options["preferEdge"].(bool); ok && preferEdge {
			annotations["scheduler.thesis/prefer-edge"] = "true"
		}

		if preferRegion, ok := eventData.Options["preferRegion"].(string); ok && preferRegion != "" {
			annotations["scheduler.thesis/prefer-region"] = preferRegion
		}
	}

	return annotations, nil
}

// escapeJSONPointerPath escapes a string for use in a JSON Pointer path
func escapeJSONPointerPath(s string) string {
	// ~ needs to be encoded as ~0
	s = strings.ReplaceAll(s, "~", "~0")
	// / needs to be encoded as ~1
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}
