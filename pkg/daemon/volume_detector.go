package daemon

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// VolumeDetector detects local and remote volumes attached to a node
type VolumeDetector struct {
	clientset kubernetes.Interface
	nodeName  string
}

// VolumeInfo contains information about a detected volume
type VolumeInfo struct {
	Type              string // "local", "pvc", "csi", etc.
	Path              string
	CapacityBytes     int64
	AvailableBytes    int64
	StorageTechnology string // "ssd", "hdd", "nvme"
	Labels            map[string]string
}

func NewVolumeDetector(clientset kubernetes.Interface, nodeName string) *VolumeDetector {
	return &VolumeDetector{
		clientset: clientset,
		nodeName:  nodeName,
	}
}

func (d *VolumeDetector) DetectLocalVolumes(ctx context.Context) ([]VolumeInfo, error) {
	var volumes []VolumeInfo

	mountedVolumes, err := d.detectMountedVolumes()
	if err != nil {
		klog.Warningf("Error detecting mounted volumes: %v", err)
	} else {
		volumes = append(volumes, mountedVolumes...)
	}

	pvVolumes, err := d.detectPersistentVolumes(ctx)
	if err != nil {
		klog.Warningf("Error detecting PersistentVolumes: %v", err)
	} else {
		volumes = append(volumes, pvVolumes...)
	}

	csiVolumes, err := d.detectCSIVolumes(ctx)
	if err != nil {
		klog.Warningf("Error detecting CSI volumes: %v", err)
	} else {
		volumes = append(volumes, csiVolumes...)
	}

	return volumes, nil
}

func (d *VolumeDetector) detectMountedVolumes() ([]VolumeInfo, error) {
	var volumes []VolumeInfo
	var err error

	output, err := exec.Command("df", "-B1", "--output=source,target,size,avail,fstype").Output()
	if err != nil {
		klog.Warningf("Failed to execute df command: %v", err)
		output, err = exec.Command("mount").Output()
		if err != nil {
			return nil, fmt.Errorf("failed to get mount information: %w", err)
		}
	}

	lines := strings.Split(string(output), "\n")
	for i, line := range lines {
		if i == 0 || len(strings.TrimSpace(line)) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		var source, mountPoint, fsType string
		var size, avail int64

		if strings.Contains(line, "avail") {
			if len(fields) < 5 {
				continue
			}
			source = fields[0]
			mountPoint = fields[1]
			sizeStr := fields[2]
			availStr := fields[3]
			fsType = fields[4]

			size, _ = strconv.ParseInt(sizeStr, 10, 64)
			avail, _ = strconv.ParseInt(availStr, 10, 64)
		} else {
			parts := strings.SplitN(line, " ", 4)
			if len(parts) < 3 {
				continue
			}
			source = parts[0]
			mountPoint = parts[2]

			if strings.Contains(line, "type") {
				typeMatch := regexp.MustCompile(`type=(\w+)`).FindStringSubmatch(line)
				if len(typeMatch) > 1 {
					fsType = typeMatch[1]
				}
			}

			sizeOutput, _ := exec.Command("df", "-B1", "--output=size,avail", mountPoint).Output()
			sizeLines := strings.Split(string(sizeOutput), "\n")
			if len(sizeLines) > 1 {
				sizeFields := strings.Fields(sizeLines[1])
				if len(sizeFields) >= 2 {
					size, _ = strconv.ParseInt(sizeFields[0], 10, 64)
					avail, _ = strconv.ParseInt(sizeFields[1], 10, 64)
				}
			}
		}

		// we skip pseudo filesystems and system directories
		if fsType == "tmpfs" || fsType == "devtmpfs" || fsType == "sysfs" || fsType == "proc" {
			continue
		}
		if strings.HasPrefix(mountPoint, "/sys") || strings.HasPrefix(mountPoint, "/proc") ||
			strings.HasPrefix(mountPoint, "/dev") || mountPoint == "/" {
			continue
		}

		isKubeVolume := strings.Contains(mountPoint, "/var/lib/kubelet/pods") ||
			strings.Contains(mountPoint, "/var/lib/rancher") ||
			strings.Contains(mountPoint, "/mnt/k8s-volumes")

		volume := VolumeInfo{
			Type:              "local",
			Path:              mountPoint,
			CapacityBytes:     size,
			AvailableBytes:    avail,
			StorageTechnology: "unknown",
			Labels:            make(map[string]string),
		}

		volume.Labels["filesystem"] = fsType
		volume.Labels["device"] = source

		if isKubeVolume {
			volume.Labels["kubernetes-volume"] = "true"

			if strings.Contains(mountPoint, "pvc-") {
				volume.Type = "pvc"

				pvcMatch := regexp.MustCompile(`pvc-([0-9a-f-]+)`).FindStringSubmatch(mountPoint)
				if len(pvcMatch) > 1 {
					volume.Labels["pvc-id"] = pvcMatch[1]
				}
			} else if strings.Contains(mountPoint, "configmap") {
				volume.Type = "configmap"
			} else if strings.Contains(mountPoint, "secret") {
				volume.Type = "secret"
			}

			storageTech, _ := d.detectStorageTechnology(source)
			if storageTech != "unknown" {
				volume.StorageTechnology = storageTech
			}
		}

		volumes = append(volumes, volume)
	}

	return volumes, nil
}

func (d *VolumeDetector) detectPersistentVolumes(ctx context.Context) ([]VolumeInfo, error) {
	var volumes []VolumeInfo

	pvs, err := d.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list PersistentVolumes: %w", err)
	}

	for _, pv := range pvs.Items {
		if !d.isPVBoundToNode(&pv) {
			continue
		}

		volume := VolumeInfo{
			Type:              "pv",
			Path:              d.getPathFromPV(&pv),
			StorageTechnology: d.getStorageTechFromPV(&pv),
			Labels:            make(map[string]string),
		}

		if capacity, exists := pv.Spec.Capacity["storage"]; exists {
			volume.CapacityBytes = capacity.Value()
		}

		volume.Labels["pv-name"] = pv.Name
		if pv.Spec.StorageClassName != "" {
			volume.Labels["storage-class"] = pv.Spec.StorageClassName
		}

		for k, v := range pv.Annotations {
			if strings.Contains(k, "storage") || strings.Contains(k, "volume") {
				volume.Labels[k] = v
			}
		}

		volumes = append(volumes, volume)
	}

	return volumes, nil
}

func (d *VolumeDetector) detectCSIVolumes(ctx context.Context) ([]VolumeInfo, error) {
	var volumes []VolumeInfo

	csiPaths := []string{
		"/var/lib/kubelet/plugins/kubernetes.io/csi",
		"/var/lib/kubelet/plugins_registry",
		"/var/lib/kubelet/pods/*/volumes/kubernetes.io~csi/*",
	}

	for _, csiPath := range csiPaths {
		matches, err := filepath.Glob(csiPath)
		if err != nil {
			klog.Warningf("Error finding CSI paths: %v", err)
			continue
		}

		for _, path := range matches {
			cmd := exec.Command("findmnt", "-n", path)
			if err := cmd.Run(); err != nil {
				continue
			}

			// Get volume information
			capacity, avail := d.getPathCapacity(path)
			storageTech, _ := d.detectStorageTechnology(path)

			// Extract driver name from path
			parts := strings.Split(path, "/")
			driverName := "unknown"
			for i, part := range parts {
				if i > 0 && parts[i-1] == "csi" {
					driverName = part
					break
				}
			}

			volume := VolumeInfo{
				Type:              "csi",
				Path:              path,
				CapacityBytes:     capacity,
				AvailableBytes:    avail,
				StorageTechnology: storageTech,
				Labels:            make(map[string]string),
			}

			volume.Labels["csi-driver"] = driverName

			volumes = append(volumes, volume)
		}
	}

	return volumes, nil
}

func (d *VolumeDetector) isPVBoundToNode(pv *v1.PersistentVolume) bool {
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" && expr.Operator == "In" {
					for _, value := range expr.Values {
						if value == d.nodeName {
							return true
						}
					}
				}
			}
		}
	}

	if pv.Spec.Local != nil {
		return true
	}

	if nodeName, exists := pv.Annotations["volume.kubernetes.io/selected-node"]; exists {
		return nodeName == d.nodeName
	}

	return false
}

func (d *VolumeDetector) getPathFromPV(pv *v1.PersistentVolume) string {
	if pv.Spec.Local != nil {
		return pv.Spec.Local.Path
	}

	// TODO: for other volume types, we cant straight-forwardly determine the path
	return ""
}

func (d *VolumeDetector) getStorageTechFromPV(pv *v1.PersistentVolume) string {
	if tech, exists := pv.Annotations["storage.kubernetes.io/technology"]; exists {
		return tech
	}

	for k, v := range pv.Labels {
		if strings.Contains(k, "ssd") || strings.Contains(v, "ssd") {
			return "ssd"
		}
		if strings.Contains(k, "hdd") || strings.Contains(v, "hdd") {
			return "hdd"
		}
		if strings.Contains(k, "nvme") || strings.Contains(v, "nvme") {
			return "nvme"
		}
	}

	return "unknown"
}

func (d *VolumeDetector) getPathCapacity(path string) (int64, int64) {
	cmd := exec.Command("df", "-B1", "--output=size,avail", path)
	output, err := cmd.Output()
	if err != nil {
		return 0, 0
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return 0, 0
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 2 {
		return 0, 0
	}

	size, _ := strconv.ParseInt(fields[0], 10, 64)
	avail, _ := strconv.ParseInt(fields[1], 10, 64)

	return size, avail
}

func (d *VolumeDetector) detectStorageTechnology(devicePath string) (string, error) {
	device := devicePath
	if strings.HasPrefix(device, "/dev/") {
		device = strings.TrimPrefix(device, "/dev/")
		device = strings.TrimRight(device, "0123456789")
	}

	rotPath := fmt.Sprintf("/sys/block/%s/queue/rotational", device)
	cmd := exec.Command("cat", rotPath)
	output, err := cmd.Output()
	if err == nil {
		if strings.TrimSpace(string(output)) == "0" {
			// Check if it's NVMe
			if strings.HasPrefix(device, "nvme") {
				return "nvme", nil
			}
			return "ssd", nil
		} else {
			return "hdd", nil
		}
	}

	if strings.HasPrefix(device, "nvme") {
		return "nvme", nil
	}

	if strings.Contains(devicePath, "ssd") {
		return "ssd", nil
	}

	if strings.Contains(devicePath, "hdd") {
		return "hdd", nil
	}

	return "unknown", fmt.Errorf("could not determine storage technology")
}

func (d *VolumeDetector) AddVolumeLabelsToNode(volumes []VolumeInfo, labels map[string]string) {
	if len(volumes) == 0 {
		klog.V(4).Infof("No storage volumes detected on node %s", d.nodeName)
		return
	}

	var validVolumes []VolumeInfo
	var totalCapacity int64

	for _, vol := range volumes {
		if vol.CapacityBytes > 100*1024*1024 {
			validVolumes = append(validVolumes, vol)
			totalCapacity += vol.CapacityBytes
		}
	}

	if len(validVolumes) == 0 || totalCapacity == 0 {
		klog.V(3).Infof("No valid storage volumes with capacity detected on node %s", d.nodeName)
		return
	}

	node, err := d.clientset.CoreV1().Nodes().Get(context.Background(), d.nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s: %v", d.nodeName, err)
		return
	}

	region := node.Labels["topology.kubernetes.io/region"]
	zone := node.Labels["topology.kubernetes.io/zone"]
	if region == "" || zone == "" {
		klog.Warningf("Node %s missing region/zone information, storage functionality may be limited", d.nodeName)
	}

	labels[StorageNodeLabel] = "true"

	var localCount, pvcCount, csiCount int
	for _, vol := range validVolumes {
		switch vol.Type {
		case "local":
			localCount++
		case "pv":
			pvcCount++
		case "csi":
			csiCount++
		}
	}

	if localCount > 0 {
		labels[StorageTypeLabel] = "local"
	} else if pvcCount > 0 {
		labels[StorageTypeLabel] = "pv"
	} else if csiCount > 0 {
		labels[StorageTypeLabel] = "csi"
	} else {
		labels[StorageTypeLabel] = "unknown"
	}

	techCounts := make(map[string]int)
	for _, vol := range validVolumes {
		if vol.StorageTechnology != "unknown" {
			techCounts[vol.StorageTechnology]++
		}
	}

	maxCount := 0
	var predominantTech string
	for tech, count := range techCounts {
		if count > maxCount {
			maxCount = count
			predominantTech = tech
		}
	}

	if predominantTech != "" {
		labels[StorageTechLabel] = predominantTech
	}

	labels[StorageCapacityLabel] = strconv.FormatInt(totalCapacity, 10)

	if predominantTech == "nvme" || predominantTech == "ssd" {
		labels["node-capability/fast-storage"] = "true"
	}

	if totalCapacity > 500*1024*1024*1024 { // > 500GB
		labels["node-capability/high-capacity"] = "true"
	}

	labels["node-capability/storage-last-updated"] = time.Now().Format(time.RFC3339)

	klog.Infof("Added storage labels to node %s: type=%s, tech=%s, capacity=%d",
		d.nodeName, labels[StorageTypeLabel], predominantTech, totalCapacity)
}
