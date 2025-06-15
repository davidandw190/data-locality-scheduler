import os
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.colors import LinearSegmentedColormap
import csv
from matplotlib.gridspec import GridSpec
from matplotlib import cm
from matplotlib.patches import Rectangle, FancyBboxPatch
import matplotlib as mpl
from matplotlib.lines import Line2D
import textwrap

class BenchmarkVisualizer:
    
    def __init__(self, results_dir='benchmarks/simulated/results', output_dir='benchmarks/simulated/visualizations'):
        self.results_dir = results_dir
        self.output_dir = output_dir
        self.data = None
        self.summary_data = None
        self.report_data = None
        
        os.makedirs(output_dir, exist_ok=True)
        
        self.colors = {
            'data-locality-scheduler': '#009499',  # Teal 
            'default-scheduler': '#5E8AC7',        # Blue 
            'improvement': '#2ca02c',              # Green
            'reduction': '#d62728',                # Red
            'local': '#009499',                    # Teal 
            'same-zone': '#8c564b',                # Brown
            'same-region': '#9467bd',              # Purple
            'cross-region': '#e377c2',             # Pink
            'edge-to-cloud': '#7f7f7f',            # Gray
            'cloud-to-edge': '#bcbd22',            # Olive
            'edge': '#ff7f0e',                     # Orange
            'cloud': '#1f77b4',                    # Blue
            'background': '#f5f5f5',               # Light gray
            'grid': '#e0e0e0',                     # Slightly darker gray
        }
        
        plt.rcParams.update({
            # Font settings
            'font.family': 'sans-serif',
            'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
            'font.size': 11,
            'axes.labelsize': 12,
            'axes.titlesize': 13,
            'xtick.labelsize': 10,
            'ytick.labelsize': 10,
            'legend.fontsize': 10,
            'figure.titlesize': 14,
            
            # Figure properties
            'figure.figsize': (12, 5),
            'figure.dpi': 100,
            'figure.facecolor': 'white',
            'figure.constrained_layout.use': False,  
            
            # Grid and axes
            'axes.grid': True,
            'grid.linestyle': '-',
            'grid.linewidth': 0.5,
            'grid.alpha': 0.3,
            'axes.linewidth': 1.2,
            'axes.edgecolor': '#333333',
            'axes.facecolor': 'white',
            'axes.spines.top': False,
            'axes.spines.right': False,
            
            # Legend
            'legend.frameon': True,
            'legend.framealpha': 0.9,
            'legend.edgecolor': '#333333',
            'legend.fancybox': False,
            
            # Saving properties
            'savefig.dpi': 300,
            'savefig.bbox': 'tight',
            'savefig.pad_inches': 0.1,
        })
    
    def load_data(self, results_file=None, summary_file=None, report_file=None):
        if results_file is None:
            json_files = [f for f in os.listdir(self.results_dir) if f.startswith('benchmark_results_') and f.endswith('.json')]
            if json_files:
                results_file = os.path.join(self.results_dir, sorted(json_files)[-1])
        
        if summary_file is None:
            csv_files = [f for f in os.listdir(self.results_dir) if f.startswith('benchmark_summary_') and f.endswith('.csv')]
            if csv_files:
                summary_file = os.path.join(self.results_dir, sorted(csv_files)[-1])
        
        # Load JSON results - this is the primary data source
        if results_file and os.path.exists(results_file):
            try:
                with open(results_file, 'r') as f:
                    self.data = json.load(f)
                print(f"Loaded benchmark results from {results_file}")
            except Exception as e:
                print(f"Error loading benchmark results: {e}")
                raise
        
        # Load CSV summary if available
        if summary_file and os.path.exists(summary_file):
            try:
                self.summary_data = []
                with open(summary_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.summary_data.append(row)
                print(f"Loaded benchmark summary from {summary_file}")
            except Exception as e:
                print(f"Error loading benchmark summary: {e}")
        
        if not self.data:
            raise ValueError("No benchmark data found. Please run benchmarks first.")
    
    def _format_workload_name(self, workload):
        workload = workload.replace('-pipeline', '')
        
        parts = workload.split('-')
        
        if workload == 'ml-training':
            return 'ML Training'
        elif workload == 'image-processing':
            return 'Image\nProcessing'
        elif workload == 'etl':
            return 'ETL'
        elif workload == 'cross-region-data-processing':
            return 'Cross-Region\nData Processing'
        elif workload == 'stream-processing':
            return 'Stream\nProcessing'
        elif workload == 'edge-to-cloud':
            return 'Edge to Cloud'
        else:
            formatted = ' '.join([p.capitalize() for p in parts])
            if len(formatted) > 12:
                words = formatted.split()
                mid = len(words) // 2
                formatted = ' '.join(words[:mid]) + '\n' + ' '.join(words[mid:])
            return formatted
    
    def _add_value_labels(self, ax, bars, values, format_str='{:.1f}', offset=3, 
                         fontsize=9, ha='center', va='bottom', rotation=0, 
                         color='black', fontweight='normal'):
        for bar, value in zip(bars, values):
            height = bar.get_height()
            
            if abs(height) < 0.01:
                continue
            
            x = bar.get_x() + bar.get_width() / 2
            y = height
            
            if height < 0:
                va = 'top'
                y = height - offset * 0.01 * ax.get_ylim()[1]
            else:
                y = height + offset * 0.01 * ax.get_ylim()[1]
            
            label = format_str.format(value)
            
            ax.text(x, y, label, ha=ha, va=va, fontsize=fontsize,
                   fontweight=fontweight, color=color, rotation=rotation)
    
    def visualize_data_locality_comparison(self, filename='data_locality_comparison.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Data Locality Scores
        data_locality_scores = []
        default_scores = []
        improvements = []
        
        for workload in workloads:
            comp_data = self.data['comparison'][workload]['data_locality_comparison']
            data_locality_scores.append(comp_data['data-locality-scheduler']['mean'])
            default_scores.append(comp_data['default-scheduler']['mean'])
            improvements.append(comp_data['improvement_percentage'])
        
        x = np.arange(len(workloads))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, data_locality_scores, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars2 = ax1.bar(x + width/2, default_scores, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax1, bars1, data_locality_scores, format_str='{:.3f}')
        self._add_value_labels(ax1, bars2, default_scores, format_str='{:.3f}')
        
        for i, imp in enumerate(improvements):
            y_pos = max(data_locality_scores[i], default_scores[i]) + 0.05
            ax1.annotate(f'+{imp:.0f}%',
                        xy=(x[i], y_pos),
                        ha='center', va='bottom',
                        fontsize=9, fontweight='bold',
                        color=self.colors['improvement'])
        
        ax1.set_xlabel('Workload Type')
        ax1.set_ylabel('Data Locality Score')
        ax1.set_title('Data Locality Score Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax1.legend(loc='upper left')
        ax1.set_ylim(0, 1.1)
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Size-Weighted Scores
        size_weighted_dl = []
        size_weighted_default = []
        size_improvements = []
        
        for workload in workloads:
            comp_data = self.data['comparison'][workload]['data_locality_comparison']
            size_weighted_dl.append(comp_data['data-locality-scheduler']['size_weighted_mean'])
            size_weighted_default.append(comp_data['default-scheduler']['size_weighted_mean'])
            size_improvements.append(comp_data['size_weighted_improvement_percentage'])
        
        bars3 = ax2.bar(x - width/2, size_weighted_dl, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars4 = ax2.bar(x + width/2, size_weighted_default, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax2, bars3, size_weighted_dl, format_str='{:.3f}')
        self._add_value_labels(ax2, bars4, size_weighted_default, format_str='{:.3f}')
        
        for i, imp in enumerate(size_improvements):
            y_pos = max(size_weighted_dl[i], size_weighted_default[i]) + 0.05
            ax2.annotate(f'+{imp:.0f}%',
                        xy=(x[i], y_pos),
                        ha='center', va='bottom',
                        fontsize=9, fontweight='bold',
                        color=self.colors['improvement'])
        
        ax2.set_xlabel('Workload Type')
        ax2.set_ylabel('Size-Weighted Data Locality Score')
        ax2.set_title('Size-Weighted Data Locality Score Comparison')
        ax2.set_xticks(x)
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax2.legend(loc='upper left')
        ax2.set_ylim(0, 1.1)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved data locality comparison to {os.path.join(self.output_dir, filename)}")
    
    def visualize_local_data_access_patterns(self, filename='local_data_access_patterns.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Local Data Access Percentage
        local_dl = []
        local_default = []
        local_improvements = []
        
        for workload in workloads:
            comp_data = self.data['comparison'][workload]['data_locality_comparison']
            local_dl.append(comp_data['data-locality-scheduler']['local_data_percentage'])
            local_default.append(comp_data['default-scheduler']['local_data_percentage'])
            local_improvements.append(comp_data['local_data_improvement_percentage'])
        
        x = np.arange(len(workloads))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, local_dl, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars2 = ax1.bar(x + width/2, local_default, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax1, bars1, local_dl, format_str='{:.1f}%')
        self._add_value_labels(ax1, bars2, local_default, format_str='{:.1f}%')
        
        ax1.set_xlabel('Workload Type')
        ax1.set_ylabel('Local Data Access (%)')
        ax1.set_title('Local Data Access Percentage by Scheduler')
        ax1.set_xticks(x)
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax1.legend(loc='upper left')
        ax1.set_ylim(0, 100)
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Cross-Region Data Access
        cross_dl = []
        cross_default = []
        
        for workload in workloads:
            comp_data = self.data['comparison'][workload]['data_locality_comparison']
            cross_dl.append(comp_data['data-locality-scheduler']['cross_region_percentage'])
            cross_default.append(comp_data['default-scheduler']['cross_region_percentage'])
        
        bars3 = ax2.bar(x - width/2, cross_dl, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars4 = ax2.bar(x + width/2, cross_default, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax2, bars3, cross_dl, format_str='{:.1f}%')
        self._add_value_labels(ax2, bars4, cross_default, format_str='{:.1f}%')
        
        ax2.set_xlabel('Workload Type')
        ax2.set_ylabel('Cross-Region Data Access (%)')
        ax2.set_title('Cross-Region Data Access Percentage by Scheduler')
        ax2.set_xticks(x)
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax2.legend(loc='upper right')
        ax2.set_ylim(0, 105)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved local data access patterns to {os.path.join(self.output_dir, filename)}")
    
    def visualize_network_transfer_analysis(self, filename='network_transfer_analysis.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Total Network Transfer Volume
        network_dl = []
        network_default = []
        total_data = []
        
        for workload in workloads:
            net_comp = self.data['comparison'][workload]['network_comparison']
            total_mb = net_comp['data-locality-scheduler']['total_data_mb']
            local_mb = net_comp['data-locality-scheduler']['local_data_mb']
            network_dl.append(total_mb - local_mb)
            
            default_total = net_comp['default-scheduler']['total_data_mb']
            default_local = net_comp['default-scheduler']['local_data_mb']
            network_default.append(default_total - default_local)
            
            total_data.append(total_mb)
        
        x = np.arange(len(workloads))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, network_default, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars2 = ax1.bar(x + width/2, network_dl, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        for i in range(len(workloads)):
            if network_default[i] > 0:
                reduction = (network_default[i] - network_dl[i]) / network_default[i] * 100
                y_pos = max(network_default[i], network_dl[i]) + 20
                ax1.annotate(f'-{reduction:.0f}%',
                            xy=(x[i], y_pos),
                            ha='center', va='bottom',
                            fontsize=9, fontweight='bold',
                            color=self.colors['reduction'])
        
        ax1.set_xlabel('Workload Type')
        ax1.set_ylabel('Network Transfer Volume (MB)')
        ax1.set_title('Network Data Transfer Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax1.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Transfer Type Distribution
        edge_to_cloud_dl = []
        edge_to_cloud_default = []
        cloud_to_edge_dl = []
        cloud_to_edge_default = []
        
        for workload in workloads:
            net_comp = self.data['comparison'][workload]['network_comparison']
            edge_to_cloud_dl.append(net_comp['data-locality-scheduler']['edge_to_cloud_data_mb'])
            edge_to_cloud_default.append(net_comp['default-scheduler']['edge_to_cloud_data_mb'])
            
            metrics_key = f"{workload}_data-locality-scheduler_1"
            if metrics_key in self.data['metrics']:
                c2e = self.data['metrics'][metrics_key]['data_locality_metrics']['network_metrics'].get('cloud_to_edge_data_size_bytes', 0) / 1048576
                cloud_to_edge_dl.append(c2e)
            else:
                cloud_to_edge_dl.append(0)
            
            metrics_key = f"{workload}_default-scheduler_1"
            if metrics_key in self.data['metrics']:
                c2e = self.data['metrics'][metrics_key]['data_locality_metrics']['network_metrics'].get('cloud_to_edge_data_size_bytes', 0) / 1048576
                cloud_to_edge_default.append(c2e)
            else:
                cloud_to_edge_default.append(0)
        
        bar_width = 0.2
        r1 = np.arange(len(workloads))
        r2 = [x + bar_width for x in r1]
        r3 = [x + bar_width for x in r2]
        r4 = [x + bar_width for x in r3]
        
        ax2.bar(r1, edge_to_cloud_dl, bar_width, label='E→C (DL)', 
                color=self.colors['edge-to-cloud'], edgecolor='black', linewidth=0.8)
        ax2.bar(r2, edge_to_cloud_default, bar_width, label='E→C (Default)', 
                color=self.colors['edge-to-cloud'], alpha=0.6, edgecolor='black', linewidth=0.8)
        ax2.bar(r3, cloud_to_edge_dl, bar_width, label='C→E (DL)', 
                color=self.colors['cloud-to-edge'], edgecolor='black', linewidth=0.8)
        ax2.bar(r4, cloud_to_edge_default, bar_width, label='C→E (Default)', 
                color=self.colors['cloud-to-edge'], alpha=0.6, edgecolor='black', linewidth=0.8)
        
        ax2.set_xlabel('Workload Type')
        ax2.set_ylabel('Transfer Volume (MB)')
        ax2.set_title('Edge-Cloud Transfer Patterns')
        ax2.set_xticks([r + 1.5 * bar_width for r in range(len(workloads))])
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax2.legend(loc='upper right', ncol=2)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved network transfer analysis to {os.path.join(self.output_dir, filename)}")
    
    def visualize_scheduling_latency_analysis(self, filename='scheduling_latency_analysis.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        # Single plot figure
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
        # Average Scheduling Latency with error bars
        latency_dl = []
        latency_default = []
        latency_min_dl = []
        latency_max_dl = []
        latency_min_default = []
        latency_max_default = []
        
        for workload in workloads:
            lat_comp = self.data['comparison'][workload]['scheduling_latency_comparison']
            latency_dl.append(lat_comp['data-locality-scheduler']['mean'])
            latency_default.append(lat_comp['default-scheduler']['mean'])
            latency_min_dl.append(lat_comp['data-locality-scheduler']['min'])
            latency_max_dl.append(lat_comp['data-locality-scheduler']['max'])
            latency_min_default.append(lat_comp['default-scheduler']['min'])
            latency_max_default.append(lat_comp['default-scheduler']['max'])
        
        x = np.arange(len(workloads))
        width = 0.35
        
        errors_dl = [(np.array(latency_dl) - np.array(latency_min_dl)).tolist(),
                    (np.array(latency_max_dl) - np.array(latency_dl)).tolist()]
        
        bars1 = ax.bar(x - width/2, latency_dl, width, yerr=errors_dl,
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=1.2, capsize=5)
        
        # Show baseline as very small bars for visibility
        default_display = [0.1 if v == 0 else v for v in latency_default]
        bars2 = ax.bar(x + width/2, default_display, width,
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=1.2, alpha=0.7)
        
        # Clear value labels for academic presentation
        for i, (bar, value) in enumerate(zip(bars1, latency_dl)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.15,
                    f'{value:.2f}s', ha='center', va='bottom', 
                    fontsize=11, fontweight='bold')
            
            # Show range if there's variation
            if latency_min_dl[i] != latency_max_dl[i]:
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.6,
                        f'({latency_min_dl[i]:.1f}-{latency_max_dl[i]:.1f})',
                        ha='center', va='bottom', fontsize=9, color='gray')
        
        # Label baseline bars
        for bar in bars2:
            ax.text(bar.get_x() + bar.get_width()/2., 0.12,
                    '~0s', ha='center', va='bottom', 
                    fontsize=10, color='gray', fontweight='bold')
        
        ax.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
        ax.set_ylabel('Scheduling Latency (seconds)', fontsize=13, fontweight='bold')
        ax.set_title('Scheduling Latency Comparison', fontsize=15, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                        rotation=0, ha='center', fontsize=11)
        ax.legend(loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
        ax.set_ylim(0, max(latency_max_dl) + 1.5)
        ax.grid(True, alpha=0.3)
        
        # Improve tick label formatting
        ax.tick_params(axis='both', which='major', labelsize=11)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved scheduling latency analysis to {os.path.join(self.output_dir, filename)}")
        
    def visualize_node_placement_distribution(self, filename='node_placement_distribution.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Edge vs Cloud Placement Count
        edge_dl = []
        cloud_dl = []
        edge_default = []
        cloud_default = []
        
        for workload in workloads:
            node_comp = self.data['comparison'][workload]['node_distribution_comparison']
            edge_default.append(node_comp['data-locality-scheduler']['edge_placements'])
            cloud_default.append(node_comp['data-locality-scheduler']['cloud_placements'])
            edge_dl.append(node_comp['default-scheduler']['edge_placements'])
            cloud_dl.append(node_comp['default-scheduler']['cloud_placements'])
        
        x = np.arange(len(workloads))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, edge_dl, width, label='Edge (DL)', 
                        color=self.colors['edge'], edgecolor='black', linewidth=0.8)
        bars2 = ax1.bar(x - width/2, cloud_dl, width, bottom=edge_dl, label='Cloud (DL)', 
                        color=self.colors['cloud'], edgecolor='black', linewidth=0.8)
        
        bars3 = ax1.bar(x + width/2, edge_default, width, label='Edge (Default)', 
                        color=self.colors['edge'], alpha=0.6, edgecolor='black', linewidth=0.8)
        bars4 = ax1.bar(x + width/2, cloud_default, width, bottom=edge_default, label='Cloud (Default)', 
                        color=self.colors['cloud'], alpha=0.6, edgecolor='black', linewidth=0.8)
        
        ax1.set_xlabel('Workload Type')
        ax1.set_ylabel('Number of Pod Placements')
        ax1.set_title('Pod Placement Distribution by Node Type')
        ax1.set_xticks(x)
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax1.legend(loc='upper left', ncol=2)
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Placement Percentage Distribution
        edge_pct_dl = []
        cloud_pct_dl = []
        edge_pct_default = []
        cloud_pct_default = []
        
        for workload in workloads:
            node_comp = self.data['comparison'][workload]['node_distribution_comparison']
            edge_pct_default.append(node_comp['data-locality-scheduler']['edge_percentage'])
            cloud_pct_default.append(node_comp['data-locality-scheduler']['cloud_percentage'])
            edge_pct_dl.append(node_comp['default-scheduler']['edge_percentage'])
            cloud_pct_dl.append(node_comp['default-scheduler']['cloud_percentage'])
        
        bar_width = 0.2
        r1 = np.arange(len(workloads))
        r2 = [x + bar_width for x in r1]
        r3 = [x + bar_width for x in r2]
        r4 = [x + bar_width for x in r3]
        
        ax2.bar(r1, edge_pct_dl, bar_width, label='Edge % (DL)', 
                color=self.colors['edge'], edgecolor='black', linewidth=0.8)
        ax2.bar(r2, edge_pct_default, bar_width, label='Edge % (Default)', 
                color=self.colors['edge'], alpha=0.6, edgecolor='black', linewidth=0.8)
        ax2.bar(r3, cloud_pct_dl, bar_width, label='Cloud % (DL)', 
                color=self.colors['cloud'], edgecolor='black', linewidth=0.8)
        ax2.bar(r4, cloud_pct_default, bar_width, label='Cloud % (Default)', 
                color=self.colors['cloud'], alpha=0.6, edgecolor='black', linewidth=0.8)
        
        ax2.set_xlabel('Workload Type')
        ax2.set_ylabel('Placement Percentage (%)')
        ax2.set_title('Node Type Utilization Percentage')
        ax2.set_xticks([r + 1.5 * bar_width for r in range(len(workloads))])
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax2.legend(loc='upper right', ncol=2)
        ax2.set_ylim(0, 105)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved node placement distribution to {os.path.join(self.output_dir, filename)}")
    
    def visualize_data_access_heatmap(self, filename='data_access_heatmap.png'):
        if not self.data or 'metrics' not in self.data:
            print("No metrics data available")
            return
        
        workloads = []
        schedulers = ['data-locality-scheduler', 'default-scheduler']
        access_types = ['Local', 'Same Zone', 'Same Region', 'Cross Region']
        
        data_matrix_dl = []
        data_matrix_default = []
        
        for key in self.data['metrics'].keys():
            workload = key.split('_')[0]
            if workload not in workloads:
                workloads.append(workload)
        
        for workload in workloads:
            key = f"{workload}_data-locality-scheduler_1"
            if key in self.data['metrics']:
                metrics = self.data['metrics'][key]['data_locality_metrics']
                total_refs = metrics['total_refs']
                if total_refs > 0:
                    row = [
                        metrics['local_refs'] / total_refs * 100,
                        metrics['same_zone_refs'] / total_refs * 100,
                        metrics['same_region_refs'] / total_refs * 100,
                        metrics['cross_region_refs'] / total_refs * 100
                    ]
                else:
                    row = [0, 0, 0, 0]
                data_matrix_dl.append(row)
            
            key = f"{workload}_default-scheduler_1"
            if key in self.data['metrics']:
                metrics = self.data['metrics'][key]['data_locality_metrics']
                total_refs = metrics['total_refs']
                if total_refs > 0:
                    row = [
                        metrics['local_refs'] / total_refs * 100,
                        metrics['same_zone_refs'] / total_refs * 100,
                        metrics['same_region_refs'] / total_refs * 100,
                        metrics['cross_region_refs'] / total_refs * 100
                    ]
                else:
                    row = [0, 0, 0, 0]
                data_matrix_default.append(row)
        
        # More compact figure size
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
        
        # Plot 1: Data Locality Scheduler Heatmap
        data_matrix_dl = np.array(data_matrix_dl).T
        im1 = ax1.imshow(data_matrix_dl, cmap='YlOrRd', aspect='auto', vmin=0, vmax=100)
        
        ax1.set_xticks(np.arange(len(workloads)))
        ax1.set_yticks(np.arange(len(access_types)))
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], rotation=45, ha='right', fontsize=11)
        ax1.set_yticklabels(access_types, fontsize=12)
        
        # Bigger and bolder text for values
        for i in range(len(access_types)):
            for j in range(len(workloads)):
                value = data_matrix_dl[i, j]
                if value > 0:
                    text = ax1.text(j, i, f'{value:.0f}%',
                                ha="center", va="center", 
                                color="black" if value < 50 else "white",
                                fontsize=13, fontweight='bold')
        
        ax1.set_title('Data Access Patterns - Data Locality Scheduler', fontsize=14, fontweight='bold')
        
        # Plot 2: Default Scheduler Heatmap
        data_matrix_default = np.array(data_matrix_default).T
        im2 = ax2.imshow(data_matrix_default, cmap='YlOrRd', aspect='auto', vmin=0, vmax=100)
        
        ax2.set_xticks(np.arange(len(workloads)))
        ax2.set_yticks(np.arange(len(access_types)))
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], rotation=45, ha='right', fontsize=11)
        ax2.set_yticklabels(access_types, fontsize=12)
        
        # Bigger and bolder text for values
        for i in range(len(access_types)):
            for j in range(len(workloads)):
                value = data_matrix_default[i, j]
                if value > 0:
                    text = ax2.text(j, i, f'{value:.0f}%',
                                ha="center", va="center", 
                                color="black" if value < 50 else "white",
                                fontsize=13, fontweight='bold')
        
        ax2.set_title('Data Access Patterns - Default Scheduler', fontsize=14, fontweight='bold')
        
        # Adjust colorbar size and position
        cbar1 = plt.colorbar(im1, ax=ax1, fraction=0.04, pad=0.02)
        cbar1.set_label('Percentage (%)', rotation=270, labelpad=15, fontsize=12)
        cbar1.ax.tick_params(labelsize=11)
        
        cbar2 = plt.colorbar(im2, ax=ax2, fraction=0.04, pad=0.02)
        cbar2.set_label('Percentage (%)', rotation=270, labelpad=15, fontsize=12)
        cbar2.ax.tick_params(labelsize=11)
        
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved data access heatmap to {os.path.join(self.output_dir, filename)}")


    def visualize_improvement_summary(self, filename='improvement_summary.png'):
        if not self.data or 'comparison' not in self.data:
            print("No comparison data available")
            return
        
        overall = self.data['comparison']['overall_averages']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Overall Improvement Metrics
        metrics = ['Data Locality', 'Size-Weighted', 'Local Data Access', 'Cross-Region\nReduction']
        values = [
            overall['data_locality_improvement'],
            overall['size_weighted_improvement'],
            overall['local_data_improvement'],
            overall['cross_region_reduction']
        ]
        
        colors = [self.colors['improvement'] if v >= 0 else self.colors['reduction'] for v in values]
        
        bars = ax1.bar(range(len(metrics)), values, color=colors, edgecolor='black', linewidth=0.8)
        
        for bar, value in zip(bars, values):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 20,
                    f'{value:.0f}%', ha='center', va='bottom', fontweight='bold')
        
        ax1.set_xlabel('Metric')
        ax1.set_ylabel('Improvement Percentage (%)')
        ax1.set_title('Overall Performance Improvements')
        ax1.set_xticks(range(len(metrics)))
        ax1.set_xticklabels(metrics)
        ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(min(0, min(values) - 100), max(values) + 200)
        
        # Plot 2: Enhanced Multi-Metric Radar Chart
        workloads = list(self.data['comparison'].keys())
        workloads = [w for w in workloads if w != 'overall_averages']
        
        categories = [self._format_workload_name(w).replace('\n', ' ') for w in workloads]
        
        metrics_data = {
            'Data Locality': [],
            'Network Reduction': [],
            'Local Access': [],
            'Cloud Utilization': []
        }
        
        for workload in workloads:
            comp = self.data['comparison'][workload]
            
            # Data locality improvement (normalized to 0-100 scale)
            dl_imp = min(comp['data_locality_comparison']['improvement_percentage'], 500) / 5
            metrics_data['Data Locality'].append(dl_imp)
            
            # Network reduction (normalized to 0-100 scale)
            net_red = comp['network_comparison']['transfer_reduction_percentage']
            metrics_data['Network Reduction'].append(net_red)
            
            # Local data access improvement (normalized to 0-100 scale)
            local_imp = min(comp['data_locality_comparison']['local_data_improvement_percentage'], 500) / 5
            metrics_data['Local Access'].append(local_imp)
            
            # Cloud utilization improvement (can be negative, normalize differently)
            cloud_util = comp['node_distribution_comparison']['cloud_utilization_improvement_percentage']
            cloud_util_norm = 50 + (cloud_util / 4)  # Center at 50, scale down
            metrics_data['Cloud Utilization'].append(max(0, min(100, cloud_util_norm)))
        
        angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
        
        ax2 = plt.subplot(122, projection='polar')
        
        colors_radar = ['#009499', '#2ca02c', '#ff7f0e', '#1f77b4']
        for i, (metric, values) in enumerate(metrics_data.items()):
            values_plot = values + values[:1] 
            angles_plot = angles + angles[:1]
            
            ax2.plot(angles_plot, values_plot, 'o-', linewidth=2, 
                    label=metric, color=colors_radar[i])
            ax2.fill(angles_plot, values_plot, alpha=0.15, color=colors_radar[i])
        
        ax2.set_xticks(angles)
        ax2.set_xticklabels(categories, size=10)
        ax2.set_ylim(0, 100)
        ax2.set_yticks([20, 40, 60, 80, 100])
        ax2.set_yticklabels(['20%', '40%', '60%', '80%', '100%'], size=8)
        ax2.set_title('Multi-Metric Performance Improvements by Workload', 
                     pad=20, size=13)
        
        ax2.grid(True, linestyle='--', alpha=0.7)
        
        ax2.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0), 
                  frameon=True, fancybox=True, shadow=True)
        
        for angle in angles:
            ax2.plot([angle, angle], [0, 100], 'k-', linewidth=0.5, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved improvement summary to {os.path.join(self.output_dir, filename)}")
    
    def visualize_workload_characteristics(self, filename='workload_characteristics.png'):
        if not self.data or 'metrics' not in self.data:
            print("No metrics data available")
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        workloads = []
        total_data_sizes = []
        pod_counts = []
        avg_transfer_sizes = []
        
        for key in self.data['metrics'].keys():
            if 'data-locality-scheduler' in key:
                workload = key.split('_')[0]
                if workload not in workloads:
                    workloads.append(workload)
                    
                    total_size = self.data['metrics'][key]['data_locality_metrics']['total_data_size'] / 1048576  # MB
                    total_data_sizes.append(total_size)
                    
                    pod_count = len(self.data['metrics'][key]['pod_metrics'])
                    pod_counts.append(pod_count)
                    
                    network_metrics = self.data['metrics'][key]['data_locality_metrics']['network_metrics']
                    total_transfers = (network_metrics.get('edge_to_cloud_transfers', 0) + 
                                     network_metrics.get('cloud_to_edge_transfers', 0))
                    if total_transfers > 0:
                        avg_transfer = total_size / total_transfers
                    else:
                        avg_transfer = 0
                    avg_transfer_sizes.append(avg_transfer)
        
        # Plot 1: Workload Scale Characteristics
        x = np.arange(len(workloads))
        width = 0.35
        
        # Normalize pod counts for dual axis
        ax1_twin = ax1.twinx()
        
        bars1 = ax1.bar(x - width/2, total_data_sizes, width, 
                        label='Total Data (MB)', color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        bars2 = ax1_twin.bar(x + width/2, pod_counts, width, 
                            label='Pod Count', color=self.colors['edge'], 
                            edgecolor='black', linewidth=0.8)
        
        ax1.set_xlabel('Workload Type')
        ax1.set_ylabel('Total Data Size (MB)', color=self.colors['data-locality-scheduler'])
        ax1_twin.set_ylabel('Number of Pods', color=self.colors['edge'])
        ax1.set_title('Workload Scale Characteristics')
        ax1.set_xticks(x)
        ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        
        ax1.tick_params(axis='y', labelcolor=self.colors['data-locality-scheduler'])
        ax1_twin.tick_params(axis='y', labelcolor=self.colors['edge'])
        
        ax1.legend(loc='upper left')
        ax1_twin.legend(loc='upper right')
        
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Data Movement Efficiency
        # Calculate data movement efficiency for each workload
        movement_efficiency_dl = []
        movement_efficiency_default = []
        
        for workload in workloads:
            # Data locality scheduler
            key_dl = f"{workload}_data-locality-scheduler_1"
            if key_dl in self.data['metrics']:
                net_metrics = self.data['metrics'][key_dl]['data_locality_metrics']['network_metrics']
                total_data = net_metrics['total_data_size_bytes'] / 1048576
                local_data = net_metrics['local_data_size_bytes'] / 1048576
                efficiency = (local_data / total_data * 100) if total_data > 0 else 0
                movement_efficiency_dl.append(efficiency)
            else:
                movement_efficiency_dl.append(0)
            
            # Default scheduler
            key_default = f"{workload}_default-scheduler_1"
            if key_default in self.data['metrics']:
                net_metrics = self.data['metrics'][key_default]['data_locality_metrics']['network_metrics']
                total_data = net_metrics['total_data_size_bytes'] / 1048576
                local_data = net_metrics['local_data_size_bytes'] / 1048576
                efficiency = (local_data / total_data * 100) if total_data > 0 else 0
                movement_efficiency_default.append(efficiency)
            else:
                movement_efficiency_default.append(0)
        
        bars3 = ax2.bar(x - width/2, movement_efficiency_dl, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars4 = ax2.bar(x + width/2, movement_efficiency_default, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax2, bars3, movement_efficiency_dl, format_str='{:.1f}%', offset=2)
        self._add_value_labels(ax2, bars4, movement_efficiency_default, format_str='{:.1f}%', offset=2)
        
        ax2.set_xlabel('Workload Type')
        ax2.set_ylabel('Data Movement Efficiency (%)')
        ax2.set_title('Data Movement Efficiency Comparison')
        ax2.set_xticks(x)
        ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
                           rotation=0, ha='center')
        ax2.legend(loc='upper left')
        ax2.set_ylim(0, 105)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved workload characteristics to {os.path.join(self.output_dir, filename)}")
    
    def visualize_pod_placement_timeline(self, filename='pod_placement_timeline.png'):
        if not self.data or 'workloads' not in self.data:
            print("No workload timeline data available")
            return
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
        
        dl_timelines = []
        default_timelines = []
        workload_names = []
        
        for workload_key, workload_data in self.data['workloads'].items():
            workload = workload_key.split('_')[0]
            scheduler = workload_key.split('_')[1]
            
            if 'start_time' in workload_data and 'completion_time' in workload_data:
                start = workload_data['start_time']
                end = workload_data['completion_time']
                duration = workload_data['duration']
                
                if scheduler == 'data-locality-scheduler':
                    dl_timelines.append({
                        'workload': workload,
                        'start': start,
                        'end': end,
                        'duration': duration
                    })
                else:
                    default_timelines.append({
                        'workload': workload,
                        'start': start,
                        'end': end,
                        'duration': duration
                    })
                
                if workload not in workload_names:
                    workload_names.append(workload)
        
        # Sort by start time
        dl_timelines.sort(key=lambda x: x['start'])
        default_timelines.sort(key=lambda x: x['start'])
        
        # Calculate relative times
        if dl_timelines:
            min_time = min([t['start'] for t in dl_timelines + default_timelines])
            
            for i, timeline in enumerate(dl_timelines):
                start_rel = timeline['start'] - min_time
                duration = timeline['duration']
                workload = timeline['workload']
                
                color_idx = workload_names.index(workload) % len(plt.cm.tab10.colors)
                color = plt.cm.tab10.colors[color_idx]
                
                rect = plt.Rectangle((start_rel, i), duration, 0.8,
                                   facecolor=color, edgecolor='black', linewidth=1)
                ax1.add_patch(rect)
                
                ax1.text(start_rel + duration/2, i + 0.4, 
                        self._format_workload_name(workload).replace('\n', ' '),
                        ha='center', va='center', fontsize=9)
            
            ax1.set_ylim(-0.5, len(dl_timelines))
            ax1.set_ylabel('Workload Instance')
            ax1.set_title('Scheduling Timeline - Data Locality Scheduler')
            ax1.grid(True, alpha=0.3, axis='x')
            
            for i, timeline in enumerate(default_timelines):
                start_rel = timeline['start'] - min_time
                duration = timeline['duration']
                workload = timeline['workload']
                
                color_idx = workload_names.index(workload) % len(plt.cm.tab10.colors)
                color = plt.cm.tab10.colors[color_idx]
                
                rect = plt.Rectangle((start_rel, i), duration, 0.8,
                                   facecolor=color, edgecolor='black', linewidth=1, alpha=0.7)
                ax2.add_patch(rect)
                
                ax2.text(start_rel + duration/2, i + 0.4, 
                        self._format_workload_name(workload).replace('\n', ' '),
                        ha='center', va='center', fontsize=9)
            
            ax2.set_ylim(-0.5, len(default_timelines))
            ax2.set_xlabel('Time (seconds)')
            ax2.set_ylabel('Workload Instance')
            ax2.set_title('Scheduling Timeline - Default Scheduler')
            ax2.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved pod placement timeline to {os.path.join(self.output_dir, filename)}")
    
    def visualize_network_topology_impact(self, filename='network_topology_impact.png'):
        if not self.data or 'metrics' not in self.data:
            print("No metrics data available")
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        transfer_types = ['Local', 'Same Region', 'Cross Region']
        dl_transfers = {'Local': 0, 'Same Region': 0, 'Cross Region': 0}
        default_transfers = {'Local': 0, 'Same Region': 0, 'Cross Region': 0}
        
        for key, metrics in self.data['metrics'].items():
            if 'data_locality_metrics' in metrics:
                net_metrics = metrics['data_locality_metrics']['network_metrics']
                
                if 'data-locality-scheduler' in key:
                    dl_transfers['Local'] += net_metrics['local_data_size_bytes'] / 1048576
                    dl_transfers['Same Region'] += net_metrics['same_region_data_size_bytes'] / 1048576
                    dl_transfers['Cross Region'] += net_metrics['cross_region_data_size_bytes'] / 1048576
                else:
                    default_transfers['Local'] += net_metrics['local_data_size_bytes'] / 1048576
                    default_transfers['Same Region'] += net_metrics['same_region_data_size_bytes'] / 1048576
                    default_transfers['Cross Region'] += net_metrics['cross_region_data_size_bytes'] / 1048576
        
        # Plot 1: Transfer Volume by Distance
        x = np.arange(len(transfer_types))
        width = 0.35
        
        dl_values = [dl_transfers[t] for t in transfer_types]
        default_values = [default_transfers[t] for t in transfer_types]
        
        bars1 = ax1.bar(x - width/2, dl_values, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars2 = ax1.bar(x + width/2, default_values, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        self._add_value_labels(ax1, bars1, dl_values, format_str='{:.0f} MB')
        self._add_value_labels(ax1, bars2, default_values, format_str='{:.0f} MB')
        
        ax1.set_xlabel('Transfer Distance')
        ax1.set_ylabel('Total Data Volume (MB)')
        ax1.set_title('Data Transfer Volume by Network Distance')
        ax1.set_xticks(x)
        ax1.set_xticklabels(transfer_types)
        ax1.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Network Cost Model (simplified)
        # Assume costs: Local=1, Same Region=5, Cross Region=10
        costs = {'Local': 1, 'Same Region': 5, 'Cross Region': 10}
        
        dl_cost = sum(dl_transfers[t] * costs[t] for t in transfer_types)
        default_cost = sum(default_transfers[t] * costs[t] for t in transfer_types)
        
        dl_cost_breakdown = [dl_transfers[t] * costs[t] for t in transfer_types]
        default_cost_breakdown = [default_transfers[t] * costs[t] for t in transfer_types]
        
        colors_cost = [self.colors['local'], self.colors['same-region'], self.colors['cross-region']]
        
        bottom_dl = 0
        bottom_default = 0
        
        for i, (t, color) in enumerate(zip(transfer_types, colors_cost)):
            ax2.bar(0, dl_cost_breakdown[i], width, bottom=bottom_dl, 
                   color=color, edgecolor='black', linewidth=0.8, label=t if i == 0 else "")
            ax2.bar(1, default_cost_breakdown[i], width, bottom=bottom_default, 
                   color=color, edgecolor='black', linewidth=0.8, alpha=0.7)
            
            # Add cost labels
            if dl_cost_breakdown[i] > 0:
                ax2.text(0, bottom_dl + dl_cost_breakdown[i]/2, f'{dl_cost_breakdown[i]:.0f}',
                        ha='center', va='center', fontsize=9, color='white' if i == 2 else 'black')
            if default_cost_breakdown[i] > 0:
                ax2.text(1, bottom_default + default_cost_breakdown[i]/2, f'{default_cost_breakdown[i]:.0f}',
                        ha='center', va='center', fontsize=9, color='white' if i == 2 else 'black')
            
            bottom_dl += dl_cost_breakdown[i]
            bottom_default += default_cost_breakdown[i]
        
        ax2.text(0, bottom_dl + 50, f'Total: {dl_cost:.0f}', ha='center', fontweight='bold')
        ax2.text(1, bottom_default + 50, f'Total: {default_cost:.0f}', ha='center', fontweight='bold')
        
        reduction = (default_cost - dl_cost) / default_cost * 100 if default_cost > 0 else 0
        ax2.text(0.5, max(bottom_dl, bottom_default) + 100, f'Cost Reduction: {reduction:.1f}%',
                ha='center', fontsize=12, fontweight='bold', color=self.colors['improvement'])
        
        ax2.set_xlabel('Scheduler')
        ax2.set_ylabel('Network Cost (Arbitrary Units)')
        ax2.set_title('Estimated Network Cost Comparison')
        ax2.set_xticks([0, 1])
        ax2.set_xticklabels(['Data Locality', 'Default'])
        ax2.set_xlim(-0.5, 1.5)
        
        legend_elements = [plt.Rectangle((0,0),1,1, facecolor=c, edgecolor='black') 
                          for c in colors_cost]
        ax2.legend(legend_elements, transfer_types, loc='upper right')
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved network topology impact to {os.path.join(self.output_dir, filename)}")
    
    def visualize_per_pod_analysis(self, filename='per_pod_analysis.png'):
        if not self.data or 'metrics' not in self.data:
            print("No metrics data available")
            return
        
        workload = 'ml-training-pipeline'  
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        dl_pods = []
        default_pods = []
        
        key_dl = f"{workload}_data-locality-scheduler_1"
        key_default = f"{workload}_default-scheduler_1"
        
        if key_dl in self.data['metrics']:
            dl_pods = self.data['metrics'][key_dl]['pod_metrics']
        
        if key_default in self.data['metrics']:
            default_pods = self.data['metrics'][key_default]['pod_metrics']
        
        # Plot 1: Pod Placement by Node Type
        pod_names = []
        dl_node_types = []
        default_node_types = []
        
        for pod in dl_pods:
            base_name = pod['pod_name'].split('-6bba97c0-')[0]
            if base_name not in pod_names:
                pod_names.append(base_name)
        
        for pod_name in pod_names:
            for pod in dl_pods:
                if pod_name in pod['pod_name']:
                    dl_node_types.append(pod['node_type'])
                    break
            
            for pod in default_pods:
                if pod_name in pod['pod_name']:
                    default_node_types.append(pod['node_type'])
                    break
        
        y_pos = np.arange(len(pod_names))
        
        node_type_map = {'edge': 0, 'cloud': 1}
        dl_values = [node_type_map.get(nt, 0) for nt in dl_node_types]
        default_values = [node_type_map.get(nt, 0) for nt in default_node_types]
        
        ax1.scatter(dl_values, y_pos, s=100, c=self.colors['data-locality-scheduler'], 
                   label='Data Locality', edgecolors='black', linewidths=1, zorder=3)
        ax1.scatter(default_values, y_pos + 0.1, s=100, c=self.colors['default-scheduler'], 
                   label='Default', edgecolors='black', linewidths=1, marker='s', zorder=3)
        
        for i in range(len(y_pos)):
            if dl_values[i] != default_values[i]:
                ax1.plot([dl_values[i], default_values[i]], [y_pos[i], y_pos[i] + 0.1], 
                        'k--', alpha=0.3, zorder=1)
        
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels([name.replace('-', '\n') for name in pod_names])
        ax1.set_xticks([0, 1])
        ax1.set_xticklabels(['Edge', 'Cloud'])
        ax1.set_xlabel('Node Type')
        ax1.set_title(f'Pod Placement Decisions - {self._format_workload_name(workload)}')
        ax1.legend(loc='upper right')
        ax1.grid(True, alpha=0.3, axis='x')
        ax1.set_xlim(-0.5, 1.5)
        
        dl_scores = []
        default_scores = []
        pod_labels = []
        
        for pod_name in pod_names:
            for pod in dl_pods:
                if pod_name in pod['pod_name']:
                    dl_scores.append(pod['data_locality']['score'])
                    break
            
            for pod in default_pods:
                if pod_name in pod['pod_name']:
                    default_scores.append(pod['data_locality']['score'])
                    break
            
            pod_labels.append(pod_name.split('-')[-1])
        
        x = np.arange(len(pod_labels))
        width = 0.35
        
        bars1 = ax2.bar(x - width/2, dl_scores, width, 
                        label='Data Locality Scheduler',
                        color=self.colors['data-locality-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        bars2 = ax2.bar(x + width/2, default_scores, width, 
                        label='Default Scheduler',
                        color=self.colors['default-scheduler'], 
                        edgecolor='black', linewidth=0.8)
        
        ax2.set_xlabel('Pod')
        ax2.set_ylabel('Data Locality Score')
        ax2.set_title('Per-Pod Data Locality Scores')
        ax2.set_xticks(x)
        ax2.set_xticklabels(pod_labels, rotation=45, ha='right')
        ax2.legend(loc='upper left')
        ax2.set_ylim(0, 1.1)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
                   format='pdf', bbox_inches='tight')
        plt.close()
        
        print(f"Saved per-pod analysis to {os.path.join(self.output_dir, filename)}")
    
    def generate_all_visualizations(self):
        print("Generating visualizations...")
        
        self.visualize_data_locality_comparison()
        self.visualize_local_data_access_patterns()
        self.visualize_network_transfer_analysis()
        self.visualize_scheduling_latency_analysis()
        self.visualize_node_placement_distribution()
        
        self.visualize_data_access_heatmap()
        self.visualize_improvement_summary()
        self.visualize_workload_characteristics()
        # self.visualize_pod_placement_timeline()
        self.visualize_network_topology_impact()
        # self.visualize_per_pod_analysis()
        
        print(f"\nAll visualizations saved to {self.output_dir}")
        print("Generated files:")
        print("- data_locality_comparison.png/pdf")
        print("- local_data_access_patterns.png/pdf")
        print("- network_transfer_analysis.png/pdf")
        print("- scheduling_latency_analysis.png/pdf")
        print("- node_placement_distribution.png/pdf")
        print("- data_access_heatmap.png/pdf")
        print("- improvement_summary.png/pdf")
        print("- workload_characteristics.png/pdf")
        print("- pod_placement_timeline.png/pdf")
        print("- network_topology_impact.png/pdf")
        print("- per_pod_analysis.png/pdf")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate visualizations for scheduler benchmarks')
    parser.add_argument('--results-dir', type=str, default='benchmarks/simulated/results',
                        help='Directory containing benchmark results')
    parser.add_argument('--output-dir', type=str, default='benchmarks/simulated/visualizations',
                        help='Directory to save visualizations')
    parser.add_argument('--results-file', type=str, default=None,
                        help='Specific JSON results file to use')
    parser.add_argument('--summary-file', type=str, default=None,
                        help='Specific CSV summary file to use')
    
    args = parser.parse_args()
    
    visualizer = BenchmarkVisualizer(results_dir=args.results_dir, output_dir=args.output_dir)
    
    try:
        visualizer.load_data(results_file=args.results_file, summary_file=args.summary_file)
        visualizer.generate_all_visualizations()
    except Exception as e:
        print(f"Error generating visualizations: {e}")
        raise


if __name__ == '__main__':
    main()
    
    
    
    
    
    
    
    
    
    
    
    
# import os
# import json
# import numpy as np
# import matplotlib.pyplot as plt
# import matplotlib.ticker as mtick
# from matplotlib.colors import LinearSegmentedColormap
# import csv
# from matplotlib.gridspec import GridSpec
# from matplotlib import cm
# from matplotlib.patches import Rectangle, FancyBboxPatch
# import matplotlib as mpl
# from matplotlib.lines import Line2D
# import textwrap

# class BenchmarkVisualizer:
    
#     def __init__(self, results_dir='benchmarks/simulated/results', output_dir='benchmarks/simulated/visualizations'):
#         self.results_dir = results_dir
#         self.output_dir = output_dir
#         self.data = None
#         self.summary_data = None
#         self.report_data = None
        
#         os.makedirs(output_dir, exist_ok=True)
        
#         self.colors = {
#             'data-locality-scheduler': '#009499',  # Teal 
#             'default-scheduler': '#5E8AC7',        # Blue 
#             'improvement': '#2ca02c',              # Green
#             'reduction': '#d62728',                # Red
#             'local': '#009499',                    # Teal 
#             'same-zone': '#8c564b',                # Brown
#             'same-region': '#9467bd',              # Purple
#             'cross-region': '#e377c2',             # Pink
#             'edge-to-cloud': '#7f7f7f',            # Gray
#             'cloud-to-edge': '#bcbd22',            # Olive
#             'edge': '#ff7f0e',                     # Orange
#             'cloud': '#1f77b4',                    # Blue
#             'background': '#f5f5f5',               # Light gray
#             'grid': '#e0e0e0',                     # Slightly darker gray
#         }
        
#         plt.rcParams.update({
#             # Font settings
#             'font.family': 'sans-serif',
#             'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
#             'font.size': 11,
#             'axes.labelsize': 12,
#             'axes.titlesize': 13,
#             'xtick.labelsize': 10,
#             'ytick.labelsize': 10,
#             'legend.fontsize': 10,
#             'figure.titlesize': 14,
            
#             # Figure properties
#             'figure.figsize': (12, 5),
#             'figure.dpi': 100,
#             'figure.facecolor': 'white',
#             'figure.constrained_layout.use': False,  
            
#             # Grid and axes
#             'axes.grid': True,
#             'grid.linestyle': '-',
#             'grid.linewidth': 0.5,
#             'grid.alpha': 0.3,
#             'axes.linewidth': 1.2,
#             'axes.edgecolor': '#333333',
#             'axes.facecolor': 'white',
#             'axes.spines.top': False,
#             'axes.spines.right': False,
            
#             # Legend
#             'legend.frameon': True,
#             'legend.framealpha': 0.9,
#             'legend.edgecolor': '#333333',
#             'legend.fancybox': False,
            
#             # Saving properties
#             'savefig.dpi': 300,
#             'savefig.bbox': 'tight',
#             'savefig.pad_inches': 0.1,
#         })
    
#     def load_data(self, results_file=None, summary_file=None, report_file=None):
#         if results_file is None:
#             json_files = [f for f in os.listdir(self.results_dir) if f.startswith('benchmark_results_') and f.endswith('.json')]
#             if json_files:
#                 results_file = os.path.join(self.results_dir, sorted(json_files)[-1])
        
#         if summary_file is None:
#             csv_files = [f for f in os.listdir(self.results_dir) if f.startswith('benchmark_summary_') and f.endswith('.csv')]
#             if csv_files:
#                 summary_file = os.path.join(self.results_dir, sorted(csv_files)[-1])
        
#         # Load JSON results - this is the primary data source
#         if results_file and os.path.exists(results_file):
#             try:
#                 with open(results_file, 'r') as f:
#                     self.data = json.load(f)
#                 print(f"Loaded benchmark results from {results_file}")
#             except Exception as e:
#                 print(f"Error loading benchmark results: {e}")
#                 raise
        
#         # Load CSV summary if available
#         if summary_file and os.path.exists(summary_file):
#             try:
#                 self.summary_data = []
#                 with open(summary_file, 'r') as f:
#                     reader = csv.DictReader(f)
#                     for row in reader:
#                         self.summary_data.append(row)
#                 print(f"Loaded benchmark summary from {summary_file}")
#             except Exception as e:
#                 print(f"Error loading benchmark summary: {e}")
        
#         if not self.data:
#             raise ValueError("No benchmark data found. Please run benchmarks first.")
    
#     def _format_workload_name(self, workload):
#         workload = workload.replace('-pipeline', '')
        
#         parts = workload.split('-')
        
#         if workload == 'ml-training':
#             return 'ML Training'
#         elif workload == 'image-processing':
#             return 'Image\nProcessing'
#         elif workload == 'etl':
#             return 'ETL'
#         elif workload == 'cross-region-data-processing':
#             return 'Cross-Region\nData Processing'
#         elif workload == 'stream-processing':
#             return 'Stream\nProcessing'
#         elif workload == 'edge-to-cloud':
#             return 'Edge to Cloud'
#         else:
#             formatted = ' '.join([p.capitalize() for p in parts])
#             if len(formatted) > 12:
#                 words = formatted.split()
#                 mid = len(words) // 2
#                 formatted = ' '.join(words[:mid]) + '\n' + ' '.join(words[mid:])
#             return formatted
    
#     def _add_value_labels(self, ax, bars, values, format_str='{:.1f}', offset=3, 
#                          fontsize=9, ha='center', va='bottom', rotation=0, 
#                          color='black', fontweight='normal'):
#         for bar, value in zip(bars, values):
#             height = bar.get_height()
            
#             if abs(height) < 0.01:
#                 continue
            
#             x = bar.get_x() + bar.get_width() / 2
#             y = height
            
#             if height < 0:
#                 va = 'top'
#                 y = height - offset * 0.01 * ax.get_ylim()[1]
#             else:
#                 y = height + offset * 0.01 * ax.get_ylim()[1]
            
#             label = format_str.format(value)
            
#             ax.text(x, y, label, ha=ha, va=va, fontsize=fontsize,
#                    fontweight=fontweight, color=color, rotation=rotation)
    
#     def visualize_data_locality_comparison(self, filename='data_locality_comparison.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         # Plot 1: Data Locality Scores
#         data_locality_scores = []
#         default_scores = []
#         improvements = []
        
#         for workload in workloads:
#             comp_data = self.data['comparison'][workload]['data_locality_comparison']
#             data_locality_scores.append(comp_data['data-locality-scheduler']['mean'])
#             default_scores.append(comp_data['default-scheduler']['mean'])
#             improvements.append(comp_data['improvement_percentage'])
        
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         bars1 = ax1.bar(x - width/2, data_locality_scores, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
#         bars2 = ax1.bar(x + width/2, default_scores, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
        
#         self._add_value_labels(ax1, bars1, data_locality_scores, format_str='{:.3f}', 
#                             fontsize=11, fontweight='bold')
#         self._add_value_labels(ax1, bars2, default_scores, format_str='{:.3f}', 
#                             fontsize=11, fontweight='bold')
        
#         for i, imp in enumerate(improvements):
#             y_pos = max(data_locality_scores[i], default_scores[i]) + 0.05
#             ax1.annotate(f'+{imp:.0f}%',
#                         xy=(x[i], y_pos),
#                         ha='center', va='bottom',
#                         fontsize=11, fontweight='bold',
#                         color=self.colors['improvement'])
        
#         ax1.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax1.set_ylabel('Data Locality Score', fontsize=13, fontweight='bold')
#         ax1.set_title('Data Locality Score Comparison', fontsize=15, fontweight='bold')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax1.legend(loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax1.set_ylim(0, 1.1)
#         ax1.grid(True, alpha=0.3)
#         ax1.tick_params(axis='both', which='major', labelsize=11)
        
#         # Plot 2: Size-Weighted Scores
#         size_weighted_dl = []
#         size_weighted_default = []
#         size_improvements = []
        
#         for workload in workloads:
#             comp_data = self.data['comparison'][workload]['data_locality_comparison']
#             size_weighted_dl.append(comp_data['data-locality-scheduler']['size_weighted_mean'])
#             size_weighted_default.append(comp_data['default-scheduler']['size_weighted_mean'])
#             size_improvements.append(comp_data['size_weighted_improvement_percentage'])
        
#         bars3 = ax2.bar(x - width/2, size_weighted_dl, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
#         bars4 = ax2.bar(x + width/2, size_weighted_default, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
        
#         self._add_value_labels(ax2, bars3, size_weighted_dl, format_str='{:.3f}', 
#                             fontsize=11, fontweight='bold')
#         self._add_value_labels(ax2, bars4, size_weighted_default, format_str='{:.3f}', 
#                             fontsize=11, fontweight='bold')
        
#         for i, imp in enumerate(size_improvements):
#             y_pos = max(size_weighted_dl[i], size_weighted_default[i]) + 0.05
#             ax2.annotate(f'+{imp:.0f}%',
#                         xy=(x[i], y_pos),
#                         ha='center', va='bottom',
#                         fontsize=11, fontweight='bold',
#                         color=self.colors['improvement'])
        
#         ax2.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax2.set_ylabel('Size-Weighted Data Locality Score', fontsize=13, fontweight='bold')
#         ax2.set_title('Size-Weighted Data Locality Score Comparison', fontsize=15, fontweight='bold')
#         ax2.set_xticks(x)
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax2.legend(loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax2.set_ylim(0, 1.1)
#         ax2.grid(True, alpha=0.3)
#         ax2.tick_params(axis='both', which='major', labelsize=11)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                 format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved data locality comparison to {os.path.join(self.output_dir, filename)}")

#     def visualize_local_data_access_patterns(self, filename='local_data_access_patterns.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         # Plot 1: Local Data Access Percentage
#         local_dl = []
#         local_default = []
#         local_improvements = []
        
#         for workload in workloads:
#             comp_data = self.data['comparison'][workload]['data_locality_comparison']
#             local_dl.append(comp_data['data-locality-scheduler']['local_data_percentage'])
#             local_default.append(comp_data['default-scheduler']['local_data_percentage'])
#             local_improvements.append(comp_data['local_data_improvement_percentage'])
        
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         bars1 = ax1.bar(x - width/2, local_dl, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
#         bars2 = ax1.bar(x + width/2, local_default, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
        
#         self._add_value_labels(ax1, bars1, local_dl, format_str='{:.1f}%', 
#                             fontsize=11, fontweight='bold')
#         self._add_value_labels(ax1, bars2, local_default, format_str='{:.1f}%', 
#                             fontsize=11, fontweight='bold')
        
#         ax1.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax1.set_ylabel('Local Data Access (%)', fontsize=13, fontweight='bold')
#         ax1.set_title('Local Data Access Percentage by Scheduler', fontsize=15, fontweight='bold')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax1.legend(loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax1.set_ylim(0, 100)
#         ax1.grid(True, alpha=0.3)
#         ax1.tick_params(axis='both', which='major', labelsize=11)
        
#         # Plot 2: Cross-Region Data Access
#         cross_dl = []
#         cross_default = []
        
#         for workload in workloads:
#             comp_data = self.data['comparison'][workload]['data_locality_comparison']
#             cross_dl.append(comp_data['data-locality-scheduler']['cross_region_percentage'])
#             cross_default.append(comp_data['default-scheduler']['cross_region_percentage'])
        
#         bars3 = ax2.bar(x - width/2, cross_dl, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
#         bars4 = ax2.bar(x + width/2, cross_default, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
        
#         self._add_value_labels(ax2, bars3, cross_dl, format_str='{:.1f}%', 
#                             fontsize=11, fontweight='bold')
#         self._add_value_labels(ax2, bars4, cross_default, format_str='{:.1f}%', 
#                             fontsize=11, fontweight='bold')
        
#         ax2.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax2.set_ylabel('Cross-Region Data Access (%)', fontsize=13, fontweight='bold')
#         ax2.set_title('Cross-Region Data Access Percentage by Scheduler', fontsize=15, fontweight='bold')
#         ax2.set_xticks(x)
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax2.legend(loc='upper right', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax2.set_ylim(0, 105)
#         ax2.grid(True, alpha=0.3)
#         ax2.tick_params(axis='both', which='major', labelsize=11)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                 format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved local data access patterns to {os.path.join(self.output_dir, filename)}")


#     def visualize_network_transfer_analysis(self, filename='network_transfer_analysis.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         # Plot 1: Total Network Transfer Volume
#         network_dl = []
#         network_default = []
#         total_data = []
        
#         for workload in workloads:
#             net_comp = self.data['comparison'][workload]['network_comparison']
#             total_mb = net_comp['data-locality-scheduler']['total_data_mb']
#             local_mb = net_comp['data-locality-scheduler']['local_data_mb']
#             network_dl.append(total_mb - local_mb)
            
#             default_total = net_comp['default-scheduler']['total_data_mb']
#             default_local = net_comp['default-scheduler']['local_data_mb']
#             network_default.append(default_total - default_local)
            
#             total_data.append(total_mb)
        
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         bars1 = ax1.bar(x - width/2, network_default, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
#         bars2 = ax1.bar(x + width/2, network_dl, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2)
        
#         # Add value labels
#         self._add_value_labels(ax1, bars1, network_default, format_str='{:.0f} MB', 
#                             fontsize=10, fontweight='bold')
#         self._add_value_labels(ax1, bars2, network_dl, format_str='{:.0f} MB', 
#                             fontsize=10, fontweight='bold')
        
#         for i in range(len(workloads)):
#             if network_default[i] > 0:
#                 reduction = (network_default[i] - network_dl[i]) / network_default[i] * 100
#                 y_pos = max(network_default[i], network_dl[i]) + 20
#                 ax1.annotate(f'-{reduction:.0f}%',
#                             xy=(x[i], y_pos),
#                             ha='center', va='bottom',
#                             fontsize=11, fontweight='bold',
#                             color=self.colors['reduction'])
        
#         ax1.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax1.set_ylabel('Network Transfer Volume (MB)', fontsize=13, fontweight='bold')
#         ax1.set_title('Network Data Transfer Comparison', fontsize=15, fontweight='bold')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax1.legend(loc='upper right', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax1.grid(True, alpha=0.3)
#         ax1.tick_params(axis='both', which='major', labelsize=11)
        
#         # Plot 2: Transfer Type Distribution
#         edge_to_cloud_dl = []
#         edge_to_cloud_default = []
#         cloud_to_edge_dl = []
#         cloud_to_edge_default = []
        
#         for workload in workloads:
#             net_comp = self.data['comparison'][workload]['network_comparison']
#             edge_to_cloud_dl.append(net_comp['data-locality-scheduler']['edge_to_cloud_data_mb'])
#             edge_to_cloud_default.append(net_comp['default-scheduler']['edge_to_cloud_data_mb'])
            
#             metrics_key = f"{workload}_data-locality-scheduler_1"
#             if metrics_key in self.data['metrics']:
#                 c2e = self.data['metrics'][metrics_key]['data_locality_metrics']['network_metrics'].get('cloud_to_edge_data_size_bytes', 0) / 1048576
#                 cloud_to_edge_dl.append(c2e)
#             else:
#                 cloud_to_edge_dl.append(0)
            
#             metrics_key = f"{workload}_default-scheduler_1"
#             if metrics_key in self.data['metrics']:
#                 c2e = self.data['metrics'][metrics_key]['data_locality_metrics']['network_metrics'].get('cloud_to_edge_data_size_bytes', 0) / 1048576
#                 cloud_to_edge_default.append(c2e)
#             else:
#                 cloud_to_edge_default.append(0)
        
#         bar_width = 0.2
#         r1 = np.arange(len(workloads))
#         r2 = [x + bar_width for x in r1]
#         r3 = [x + bar_width for x in r2]
#         r4 = [x + bar_width for x in r3]
        
#         ax2.bar(r1, edge_to_cloud_dl, bar_width, label='E→C (DL)', 
#                 color=self.colors['edge-to-cloud'], edgecolor='black', linewidth=1.0)
#         ax2.bar(r2, edge_to_cloud_default, bar_width, label='E→C (Default)', 
#                 color=self.colors['edge-to-cloud'], alpha=0.6, edgecolor='black', linewidth=1.0)
#         ax2.bar(r3, cloud_to_edge_dl, bar_width, label='C→E (DL)', 
#                 color=self.colors['cloud-to-edge'], edgecolor='black', linewidth=1.0)
#         ax2.bar(r4, cloud_to_edge_default, bar_width, label='C→E (Default)', 
#                 color=self.colors['cloud-to-edge'], alpha=0.6, edgecolor='black', linewidth=1.0)
        
#         ax2.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax2.set_ylabel('Transfer Volume (MB)', fontsize=13, fontweight='bold')
#         ax2.set_title('Edge-Cloud Transfer Patterns', fontsize=15, fontweight='bold')
#         ax2.set_xticks([r + 1.5 * bar_width for r in range(len(workloads))])
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax2.legend(loc='upper right', ncol=2, fontsize=11, frameon=True, fancybox=True, shadow=True)
#         ax2.grid(True, alpha=0.3)
#         ax2.tick_params(axis='both', which='major', labelsize=11)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                 format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved network transfer analysis to {os.path.join(self.output_dir, filename)}")
        
#     def visualize_scheduling_latency_analysis(self, filename='scheduling_latency_analysis.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         # Single plot figure
#         fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
#         # Average Scheduling Latency with error bars
#         latency_dl = []
#         latency_default = []
#         latency_min_dl = []
#         latency_max_dl = []
#         latency_min_default = []
#         latency_max_default = []
        
#         for workload in workloads:
#             lat_comp = self.data['comparison'][workload]['scheduling_latency_comparison']
#             latency_dl.append(lat_comp['data-locality-scheduler']['mean'])
#             latency_default.append(lat_comp['default-scheduler']['mean'])
#             latency_min_dl.append(lat_comp['data-locality-scheduler']['min'])
#             latency_max_dl.append(lat_comp['data-locality-scheduler']['max'])
#             latency_min_default.append(lat_comp['default-scheduler']['min'])
#             latency_max_default.append(lat_comp['default-scheduler']['max'])
        
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         errors_dl = [(np.array(latency_dl) - np.array(latency_min_dl)).tolist(),
#                     (np.array(latency_max_dl) - np.array(latency_dl)).tolist()]
        
#         bars1 = ax.bar(x - width/2, latency_dl, width, yerr=errors_dl,
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=1.2, capsize=5)
        
#         # Show baseline as very small bars for visibility
#         default_display = [0.1 if v == 0 else v for v in latency_default]
#         bars2 = ax.bar(x + width/2, default_display, width,
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=1.2, alpha=0.7)
        
#         # Clear value labels for academic presentation
#         for i, (bar, value) in enumerate(zip(bars1, latency_dl)):
#             height = bar.get_height()
#             ax.text(bar.get_x() + bar.get_width()/2., height + 0.15,
#                     f'{value:.2f}s', ha='center', va='bottom', 
#                     fontsize=11, fontweight='bold')
            
#             # Show range if there's variation
#             if latency_min_dl[i] != latency_max_dl[i]:
#                 ax.text(bar.get_x() + bar.get_width()/2., height + 0.6,
#                         f'({latency_min_dl[i]:.1f}-{latency_max_dl[i]:.1f})',
#                         ha='center', va='bottom', fontsize=9, color='gray')
        
#         # Label baseline bars
#         for bar in bars2:
#             ax.text(bar.get_x() + bar.get_width()/2., 0.12,
#                     '~0s', ha='center', va='bottom', 
#                     fontsize=10, color='gray', fontweight='bold')
        
#         ax.set_xlabel('Workload Type', fontsize=13, fontweight='bold')
#         ax.set_ylabel('Scheduling Latency (seconds)', fontsize=13, fontweight='bold')
#         ax.set_title('Scheduling Latency Comparison', fontsize=15, fontweight='bold')
#         ax.set_xticks(x)
#         ax.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                         rotation=0, ha='center', fontsize=11)
#         ax.legend(loc='upper left', fontsize=12, frameon=True, fancybox=True, shadow=True)
#         ax.set_ylim(0, max(latency_max_dl) + 1.5)
#         ax.grid(True, alpha=0.3)
        
#         # Improve tick label formatting
#         ax.tick_params(axis='both', which='major', labelsize=11)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                 format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved scheduling latency analysis to {os.path.join(self.output_dir, filename)}")
        
#     def visualize_node_placement_distribution(self, filename='node_placement_distribution.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         # Plot 1: Edge vs Cloud Placement Count
#         edge_dl = []
#         cloud_dl = []
#         edge_default = []
#         cloud_default = []
        
#         for workload in workloads:
#             node_comp = self.data['comparison'][workload]['node_distribution_comparison']
#             edge_default.append(node_comp['data-locality-scheduler']['edge_placements'])
#             cloud_default.append(node_comp['data-locality-scheduler']['cloud_placements'])
#             edge_dl.append(node_comp['default-scheduler']['edge_placements'])
#             cloud_dl.append(node_comp['default-scheduler']['cloud_placements'])
        
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         bars1 = ax1.bar(x - width/2, edge_dl, width, label='Edge (DL)', 
#                         color=self.colors['edge'], edgecolor='black', linewidth=0.8)
#         bars2 = ax1.bar(x - width/2, cloud_dl, width, bottom=edge_dl, label='Cloud (DL)', 
#                         color=self.colors['cloud'], edgecolor='black', linewidth=0.8)
        
#         bars3 = ax1.bar(x + width/2, edge_default, width, label='Edge (Default)', 
#                         color=self.colors['edge'], alpha=0.6, edgecolor='black', linewidth=0.8)
#         bars4 = ax1.bar(x + width/2, cloud_default, width, bottom=edge_default, label='Cloud (Default)', 
#                         color=self.colors['cloud'], alpha=0.6, edgecolor='black', linewidth=0.8)
        
#         ax1.set_xlabel('Workload Type')
#         ax1.set_ylabel('Number of Pod Placements')
#         ax1.set_title('Pod Placement Distribution by Node Type')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                            rotation=0, ha='center')
#         ax1.legend(loc='upper left', ncol=2)
#         ax1.grid(True, alpha=0.3)
        
#         # Plot 2: Placement Percentage Distribution
#         edge_pct_dl = []
#         cloud_pct_dl = []
#         edge_pct_default = []
#         cloud_pct_default = []
        
#         for workload in workloads:
#             node_comp = self.data['comparison'][workload]['node_distribution_comparison']
#             edge_pct_default.append(node_comp['data-locality-scheduler']['edge_percentage'])
#             cloud_pct_default.append(node_comp['data-locality-scheduler']['cloud_percentage'])
#             edge_pct_dl.append(node_comp['default-scheduler']['edge_percentage'])
#             cloud_pct_dl.append(node_comp['default-scheduler']['cloud_percentage'])
        
#         bar_width = 0.2
#         r1 = np.arange(len(workloads))
#         r2 = [x + bar_width for x in r1]
#         r3 = [x + bar_width for x in r2]
#         r4 = [x + bar_width for x in r3]
        
#         ax2.bar(r1, edge_pct_dl, bar_width, label='Edge % (DL)', 
#                 color=self.colors['edge'], edgecolor='black', linewidth=0.8)
#         ax2.bar(r2, edge_pct_default, bar_width, label='Edge % (Default)', 
#                 color=self.colors['edge'], alpha=0.6, edgecolor='black', linewidth=0.8)
#         ax2.bar(r3, cloud_pct_dl, bar_width, label='Cloud % (DL)', 
#                 color=self.colors['cloud'], edgecolor='black', linewidth=0.8)
#         ax2.bar(r4, cloud_pct_default, bar_width, label='Cloud % (Default)', 
#                 color=self.colors['cloud'], alpha=0.6, edgecolor='black', linewidth=0.8)
        
#         ax2.set_xlabel('Workload Type')
#         ax2.set_ylabel('Placement Percentage (%)')
#         ax2.set_title('Node Type Utilization Percentage')
#         ax2.set_xticks([r + 1.5 * bar_width for r in range(len(workloads))])
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                            rotation=0, ha='center')
#         ax2.legend(loc='upper right', ncol=2)
#         ax2.set_ylim(0, 105)
#         ax2.grid(True, alpha=0.3)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved node placement distribution to {os.path.join(self.output_dir, filename)}")
    
#     def visualize_data_access_heatmap(self, filename='data_access_heatmap.png'):
#         if not self.data or 'metrics' not in self.data:
#             print("No metrics data available")
#             return
        
#         workloads = []
#         schedulers = ['data-locality-scheduler', 'default-scheduler']
#         access_types = ['Local', 'Same Zone', 'Same Region', 'Cross Region']
        
#         data_matrix_dl = []
#         data_matrix_default = []
        
#         for key in self.data['metrics'].keys():
#             workload = key.split('_')[0]
#             if workload not in workloads:
#                 workloads.append(workload)
        
#         for workload in workloads:
#             key = f"{workload}_data-locality-scheduler_1"
#             if key in self.data['metrics']:
#                 metrics = self.data['metrics'][key]['data_locality_metrics']
#                 total_refs = metrics['total_refs']
#                 if total_refs > 0:
#                     row = [
#                         metrics['local_refs'] / total_refs * 100,
#                         metrics['same_zone_refs'] / total_refs * 100,
#                         metrics['same_region_refs'] / total_refs * 100,
#                         metrics['cross_region_refs'] / total_refs * 100
#                     ]
#                 else:
#                     row = [0, 0, 0, 0]
#                 data_matrix_dl.append(row)
            
#             key = f"{workload}_default-scheduler_1"
#             if key in self.data['metrics']:
#                 metrics = self.data['metrics'][key]['data_locality_metrics']
#                 total_refs = metrics['total_refs']
#                 if total_refs > 0:
#                     row = [
#                         metrics['local_refs'] / total_refs * 100,
#                         metrics['same_zone_refs'] / total_refs * 100,
#                         metrics['same_region_refs'] / total_refs * 100,
#                         metrics['cross_region_refs'] / total_refs * 100
#                     ]
#                 else:
#                     row = [0, 0, 0, 0]
#                 data_matrix_default.append(row)
        
#         # More compact figure size
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
        
#         # Plot 1: Data Locality Scheduler Heatmap
#         data_matrix_dl = np.array(data_matrix_dl).T
#         im1 = ax1.imshow(data_matrix_dl, cmap='YlOrRd', aspect='auto', vmin=0, vmax=100)
        
#         ax1.set_xticks(np.arange(len(workloads)))
#         ax1.set_yticks(np.arange(len(access_types)))
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], rotation=45, ha='right', fontsize=11)
#         ax1.set_yticklabels(access_types, fontsize=12)
        
#         # Bigger and bolder text for values
#         for i in range(len(access_types)):
#             for j in range(len(workloads)):
#                 value = data_matrix_dl[i, j]
#                 if value > 0:
#                     text = ax1.text(j, i, f'{value:.0f}%',
#                                 ha="center", va="center", 
#                                 color="black" if value < 50 else "white",
#                                 fontsize=13, fontweight='bold')
        
#         ax1.set_title('Data Access Patterns - Data Locality Scheduler', fontsize=14, fontweight='bold')
        
#         # Plot 2: Default Scheduler Heatmap
#         data_matrix_default = np.array(data_matrix_default).T
#         im2 = ax2.imshow(data_matrix_default, cmap='YlOrRd', aspect='auto', vmin=0, vmax=100)
        
#         ax2.set_xticks(np.arange(len(workloads)))
#         ax2.set_yticks(np.arange(len(access_types)))
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], rotation=45, ha='right', fontsize=11)
#         ax2.set_yticklabels(access_types, fontsize=12)
        
#         # Bigger and bolder text for values
#         for i in range(len(access_types)):
#             for j in range(len(workloads)):
#                 value = data_matrix_default[i, j]
#                 if value > 0:
#                     text = ax2.text(j, i, f'{value:.0f}%',
#                                 ha="center", va="center", 
#                                 color="black" if value < 50 else "white",
#                                 fontsize=13, fontweight='bold')
        
#         ax2.set_title('Data Access Patterns - Default Scheduler', fontsize=14, fontweight='bold')
        
#         # Adjust colorbar size and position
#         cbar1 = plt.colorbar(im1, ax=ax1, fraction=0.04, pad=0.02)
#         cbar1.set_label('Percentage (%)', rotation=270, labelpad=15, fontsize=12)
#         cbar1.ax.tick_params(labelsize=11)
        
#         cbar2 = plt.colorbar(im2, ax=ax2, fraction=0.04, pad=0.02)
#         cbar2.set_label('Percentage (%)', rotation=270, labelpad=15, fontsize=12)
#         cbar2.ax.tick_params(labelsize=11)
        
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                 format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved data access heatmap to {os.path.join(self.output_dir, filename)}")


#     def visualize_improvement_summary(self, filename='improvement_summary.png'):
#         if not self.data or 'comparison' not in self.data:
#             print("No comparison data available")
#             return
        
#         overall = self.data['comparison']['overall_averages']
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         # Plot 1: Overall Improvement Metrics
#         metrics = ['Data Locality', 'Size-Weighted', 'Local Data Access', 'Cross-Region\nReduction']
#         values = [
#             overall['data_locality_improvement'],
#             overall['size_weighted_improvement'],
#             overall['local_data_improvement'],
#             overall['cross_region_reduction']
#         ]
        
#         colors = [self.colors['improvement'] if v >= 0 else self.colors['reduction'] for v in values]
        
#         bars = ax1.bar(range(len(metrics)), values, color=colors, edgecolor='black', linewidth=0.8)
        
#         for bar, value in zip(bars, values):
#             height = bar.get_height()
#             ax1.text(bar.get_x() + bar.get_width()/2., height + 20,
#                     f'{value:.0f}%', ha='center', va='bottom', fontweight='bold')
        
#         ax1.set_xlabel('Metric')
#         ax1.set_ylabel('Improvement Percentage (%)')
#         ax1.set_title('Overall Performance Improvements')
#         ax1.set_xticks(range(len(metrics)))
#         ax1.set_xticklabels(metrics)
#         ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
#         ax1.grid(True, alpha=0.3)
#         ax1.set_ylim(min(0, min(values) - 100), max(values) + 200)
        
#         # Plot 2: Enhanced Multi-Metric Radar Chart
#         workloads = list(self.data['comparison'].keys())
#         workloads = [w for w in workloads if w != 'overall_averages']
        
#         categories = [self._format_workload_name(w).replace('\n', ' ') for w in workloads]
        
#         metrics_data = {
#             'Data Locality': [],
#             'Network Reduction': [],
#             'Local Access': [],
#             'Cloud Utilization': []
#         }
        
#         for workload in workloads:
#             comp = self.data['comparison'][workload]
            
#             # Data locality improvement (normalized to 0-100 scale)
#             dl_imp = min(comp['data_locality_comparison']['improvement_percentage'], 500) / 5
#             metrics_data['Data Locality'].append(dl_imp)
            
#             # Network reduction (normalized to 0-100 scale)
#             net_red = comp['network_comparison']['transfer_reduction_percentage']
#             metrics_data['Network Reduction'].append(net_red)
            
#             # Local data access improvement (normalized to 0-100 scale)
#             local_imp = min(comp['data_locality_comparison']['local_data_improvement_percentage'], 500) / 5
#             metrics_data['Local Access'].append(local_imp)
            
#             # Cloud utilization improvement (can be negative, normalize differently)
#             cloud_util = comp['node_distribution_comparison']['cloud_utilization_improvement_percentage']
#             cloud_util_norm = 50 + (cloud_util / 4)  # Center at 50, scale down
#             metrics_data['Cloud Utilization'].append(max(0, min(100, cloud_util_norm)))
        
#         angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
        
#         ax2 = plt.subplot(122, projection='polar')
        
#         colors_radar = ['#009499', '#2ca02c', '#ff7f0e', '#1f77b4']
#         for i, (metric, values) in enumerate(metrics_data.items()):
#             values_plot = values + values[:1] 
#             angles_plot = angles + angles[:1]
            
#             ax2.plot(angles_plot, values_plot, 'o-', linewidth=2, 
#                     label=metric, color=colors_radar[i])
#             ax2.fill(angles_plot, values_plot, alpha=0.15, color=colors_radar[i])
        
#         ax2.set_xticks(angles)
#         ax2.set_xticklabels(categories, size=10)
#         ax2.set_ylim(0, 100)
#         ax2.set_yticks([20, 40, 60, 80, 100])
#         ax2.set_yticklabels(['20%', '40%', '60%', '80%', '100%'], size=8)
#         ax2.set_title('Multi-Metric Performance Improvements by Workload', 
#                      pad=20, size=13)
        
#         ax2.grid(True, linestyle='--', alpha=0.7)
        
#         ax2.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0), 
#                   frameon=True, fancybox=True, shadow=True)
        
#         for angle in angles:
#             ax2.plot([angle, angle], [0, 100], 'k-', linewidth=0.5, alpha=0.3)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved improvement summary to {os.path.join(self.output_dir, filename)}")
    
#     def visualize_workload_characteristics(self, filename='workload_characteristics.png'):
#         if not self.data or 'metrics' not in self.data:
#             print("No metrics data available")
#             return
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         workloads = []
#         total_data_sizes = []
#         pod_counts = []
#         avg_transfer_sizes = []
        
#         for key in self.data['metrics'].keys():
#             if 'data-locality-scheduler' in key:
#                 workload = key.split('_')[0]
#                 if workload not in workloads:
#                     workloads.append(workload)
                    
#                     total_size = self.data['metrics'][key]['data_locality_metrics']['total_data_size'] / 1048576  # MB
#                     total_data_sizes.append(total_size)
                    
#                     pod_count = len(self.data['metrics'][key]['pod_metrics'])
#                     pod_counts.append(pod_count)
                    
#                     network_metrics = self.data['metrics'][key]['data_locality_metrics']['network_metrics']
#                     total_transfers = (network_metrics.get('edge_to_cloud_transfers', 0) + 
#                                      network_metrics.get('cloud_to_edge_transfers', 0))
#                     if total_transfers > 0:
#                         avg_transfer = total_size / total_transfers
#                     else:
#                         avg_transfer = 0
#                     avg_transfer_sizes.append(avg_transfer)
        
#         # Plot 1: Workload Scale Characteristics
#         x = np.arange(len(workloads))
#         width = 0.35
        
#         # Normalize pod counts for dual axis
#         ax1_twin = ax1.twinx()
        
#         bars1 = ax1.bar(x - width/2, total_data_sizes, width, 
#                         label='Total Data (MB)', color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
        
#         bars2 = ax1_twin.bar(x + width/2, pod_counts, width, 
#                             label='Pod Count', color=self.colors['edge'], 
#                             edgecolor='black', linewidth=0.8)
        
#         ax1.set_xlabel('Workload Type')
#         ax1.set_ylabel('Total Data Size (MB)', color=self.colors['data-locality-scheduler'])
#         ax1_twin.set_ylabel('Number of Pods', color=self.colors['edge'])
#         ax1.set_title('Workload Scale Characteristics')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                            rotation=0, ha='center')
        
#         ax1.tick_params(axis='y', labelcolor=self.colors['data-locality-scheduler'])
#         ax1_twin.tick_params(axis='y', labelcolor=self.colors['edge'])
        
#         ax1.legend(loc='upper left')
#         ax1_twin.legend(loc='upper right')
        
#         ax1.grid(True, alpha=0.3)
        
#         # Plot 2: Data Movement Efficiency
#         # Calculate data movement efficiency for each workload
#         movement_efficiency_dl = []
#         movement_efficiency_default = []
        
#         for workload in workloads:
#             # Data locality scheduler
#             key_dl = f"{workload}_data-locality-scheduler_1"
#             if key_dl in self.data['metrics']:
#                 net_metrics = self.data['metrics'][key_dl]['data_locality_metrics']['network_metrics']
#                 total_data = net_metrics['total_data_size_bytes'] / 1048576
#                 local_data = net_metrics['local_data_size_bytes'] / 1048576
#                 efficiency = (local_data / total_data * 100) if total_data > 0 else 0
#                 movement_efficiency_dl.append(efficiency)
#             else:
#                 movement_efficiency_dl.append(0)
            
#             # Default scheduler
#             key_default = f"{workload}_default-scheduler_1"
#             if key_default in self.data['metrics']:
#                 net_metrics = self.data['metrics'][key_default]['data_locality_metrics']['network_metrics']
#                 total_data = net_metrics['total_data_size_bytes'] / 1048576
#                 local_data = net_metrics['local_data_size_bytes'] / 1048576
#                 efficiency = (local_data / total_data * 100) if total_data > 0 else 0
#                 movement_efficiency_default.append(efficiency)
#             else:
#                 movement_efficiency_default.append(0)
        
#         bars3 = ax2.bar(x - width/2, movement_efficiency_dl, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
#         bars4 = ax2.bar(x + width/2, movement_efficiency_default, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
        
#         self._add_value_labels(ax2, bars3, movement_efficiency_dl, format_str='{:.1f}%', offset=2)
#         self._add_value_labels(ax2, bars4, movement_efficiency_default, format_str='{:.1f}%', offset=2)
        
#         ax2.set_xlabel('Workload Type')
#         ax2.set_ylabel('Data Movement Efficiency (%)')
#         ax2.set_title('Data Movement Efficiency Comparison')
#         ax2.set_xticks(x)
#         ax2.set_xticklabels([self._format_workload_name(w) for w in workloads], 
#                            rotation=0, ha='center')
#         ax2.legend(loc='upper left')
#         ax2.set_ylim(0, 105)
#         ax2.grid(True, alpha=0.3)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved workload characteristics to {os.path.join(self.output_dir, filename)}")
    
#     def visualize_pod_placement_timeline(self, filename='pod_placement_timeline.png'):
#         if not self.data or 'workloads' not in self.data:
#             print("No workload timeline data available")
#             return
        
#         fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
        
#         dl_timelines = []
#         default_timelines = []
#         workload_names = []
        
#         for workload_key, workload_data in self.data['workloads'].items():
#             workload = workload_key.split('_')[0]
#             scheduler = workload_key.split('_')[1]
            
#             if 'start_time' in workload_data and 'completion_time' in workload_data:
#                 start = workload_data['start_time']
#                 end = workload_data['completion_time']
#                 duration = workload_data['duration']
                
#                 if scheduler == 'data-locality-scheduler':
#                     dl_timelines.append({
#                         'workload': workload,
#                         'start': start,
#                         'end': end,
#                         'duration': duration
#                     })
#                 else:
#                     default_timelines.append({
#                         'workload': workload,
#                         'start': start,
#                         'end': end,
#                         'duration': duration
#                     })
                
#                 if workload not in workload_names:
#                     workload_names.append(workload)
        
#         # Sort by start time
#         dl_timelines.sort(key=lambda x: x['start'])
#         default_timelines.sort(key=lambda x: x['start'])
        
#         # Calculate relative times
#         if dl_timelines:
#             min_time = min([t['start'] for t in dl_timelines + default_timelines])
            
#             for i, timeline in enumerate(dl_timelines):
#                 start_rel = timeline['start'] - min_time
#                 duration = timeline['duration']
#                 workload = timeline['workload']
                
#                 color_idx = workload_names.index(workload) % len(plt.cm.tab10.colors)
#                 color = plt.cm.tab10.colors[color_idx]
                
#                 rect = plt.Rectangle((start_rel, i), duration, 0.8,
#                                    facecolor=color, edgecolor='black', linewidth=1)
#                 ax1.add_patch(rect)
                
#                 ax1.text(start_rel + duration/2, i + 0.4, 
#                         self._format_workload_name(workload).replace('\n', ' '),
#                         ha='center', va='center', fontsize=9)
            
#             ax1.set_ylim(-0.5, len(dl_timelines))
#             ax1.set_ylabel('Workload Instance')
#             ax1.set_title('Scheduling Timeline - Data Locality Scheduler')
#             ax1.grid(True, alpha=0.3, axis='x')
            
#             for i, timeline in enumerate(default_timelines):
#                 start_rel = timeline['start'] - min_time
#                 duration = timeline['duration']
#                 workload = timeline['workload']
                
#                 color_idx = workload_names.index(workload) % len(plt.cm.tab10.colors)
#                 color = plt.cm.tab10.colors[color_idx]
                
#                 rect = plt.Rectangle((start_rel, i), duration, 0.8,
#                                    facecolor=color, edgecolor='black', linewidth=1, alpha=0.7)
#                 ax2.add_patch(rect)
                
#                 ax2.text(start_rel + duration/2, i + 0.4, 
#                         self._format_workload_name(workload).replace('\n', ' '),
#                         ha='center', va='center', fontsize=9)
            
#             ax2.set_ylim(-0.5, len(default_timelines))
#             ax2.set_xlabel('Time (seconds)')
#             ax2.set_ylabel('Workload Instance')
#             ax2.set_title('Scheduling Timeline - Default Scheduler')
#             ax2.grid(True, alpha=0.3, axis='x')
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved pod placement timeline to {os.path.join(self.output_dir, filename)}")
    
#     def visualize_network_topology_impact(self, filename='network_topology_impact.png'):
#         if not self.data or 'metrics' not in self.data:
#             print("No metrics data available")
#             return
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         transfer_types = ['Local', 'Same Region', 'Cross Region']
#         dl_transfers = {'Local': 0, 'Same Region': 0, 'Cross Region': 0}
#         default_transfers = {'Local': 0, 'Same Region': 0, 'Cross Region': 0}
        
#         for key, metrics in self.data['metrics'].items():
#             if 'data_locality_metrics' in metrics:
#                 net_metrics = metrics['data_locality_metrics']['network_metrics']
                
#                 if 'data-locality-scheduler' in key:
#                     dl_transfers['Local'] += net_metrics['local_data_size_bytes'] / 1048576
#                     dl_transfers['Same Region'] += net_metrics['same_region_data_size_bytes'] / 1048576
#                     dl_transfers['Cross Region'] += net_metrics['cross_region_data_size_bytes'] / 1048576
#                 else:
#                     default_transfers['Local'] += net_metrics['local_data_size_bytes'] / 1048576
#                     default_transfers['Same Region'] += net_metrics['same_region_data_size_bytes'] / 1048576
#                     default_transfers['Cross Region'] += net_metrics['cross_region_data_size_bytes'] / 1048576
        
#         # Plot 1: Transfer Volume by Distance
#         x = np.arange(len(transfer_types))
#         width = 0.35
        
#         dl_values = [dl_transfers[t] for t in transfer_types]
#         default_values = [default_transfers[t] for t in transfer_types]
        
#         bars1 = ax1.bar(x - width/2, dl_values, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
#         bars2 = ax1.bar(x + width/2, default_values, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
        
#         self._add_value_labels(ax1, bars1, dl_values, format_str='{:.0f} MB')
#         self._add_value_labels(ax1, bars2, default_values, format_str='{:.0f} MB')
        
#         ax1.set_xlabel('Transfer Distance')
#         ax1.set_ylabel('Total Data Volume (MB)')
#         ax1.set_title('Data Transfer Volume by Network Distance')
#         ax1.set_xticks(x)
#         ax1.set_xticklabels(transfer_types)
#         ax1.legend(loc='upper right')
#         ax1.grid(True, alpha=0.3)
        
#         # Plot 2: Network Cost Model (simplified)
#         # Assume costs: Local=1, Same Region=5, Cross Region=10
#         costs = {'Local': 1, 'Same Region': 5, 'Cross Region': 10}
        
#         dl_cost = sum(dl_transfers[t] * costs[t] for t in transfer_types)
#         default_cost = sum(default_transfers[t] * costs[t] for t in transfer_types)
        
#         dl_cost_breakdown = [dl_transfers[t] * costs[t] for t in transfer_types]
#         default_cost_breakdown = [default_transfers[t] * costs[t] for t in transfer_types]
        
#         colors_cost = [self.colors['local'], self.colors['same-region'], self.colors['cross-region']]
        
#         bottom_dl = 0
#         bottom_default = 0
        
#         for i, (t, color) in enumerate(zip(transfer_types, colors_cost)):
#             ax2.bar(0, dl_cost_breakdown[i], width, bottom=bottom_dl, 
#                    color=color, edgecolor='black', linewidth=0.8, label=t if i == 0 else "")
#             ax2.bar(1, default_cost_breakdown[i], width, bottom=bottom_default, 
#                    color=color, edgecolor='black', linewidth=0.8, alpha=0.7)
            
#             # Add cost labels
#             if dl_cost_breakdown[i] > 0:
#                 ax2.text(0, bottom_dl + dl_cost_breakdown[i]/2, f'{dl_cost_breakdown[i]:.0f}',
#                         ha='center', va='center', fontsize=9, color='white' if i == 2 else 'black')
#             if default_cost_breakdown[i] > 0:
#                 ax2.text(1, bottom_default + default_cost_breakdown[i]/2, f'{default_cost_breakdown[i]:.0f}',
#                         ha='center', va='center', fontsize=9, color='white' if i == 2 else 'black')
            
#             bottom_dl += dl_cost_breakdown[i]
#             bottom_default += default_cost_breakdown[i]
        
#         ax2.text(0, bottom_dl + 50, f'Total: {dl_cost:.0f}', ha='center', fontweight='bold')
#         ax2.text(1, bottom_default + 50, f'Total: {default_cost:.0f}', ha='center', fontweight='bold')
        
#         reduction = (default_cost - dl_cost) / default_cost * 100 if default_cost > 0 else 0
#         ax2.text(0.5, max(bottom_dl, bottom_default) + 100, f'Cost Reduction: {reduction:.1f}%',
#                 ha='center', fontsize=12, fontweight='bold', color=self.colors['improvement'])
        
#         ax2.set_xlabel('Scheduler')
#         ax2.set_ylabel('Network Cost (Arbitrary Units)')
#         ax2.set_title('Estimated Network Cost Comparison')
#         ax2.set_xticks([0, 1])
#         ax2.set_xticklabels(['Data Locality', 'Default'])
#         ax2.set_xlim(-0.5, 1.5)
        
#         legend_elements = [plt.Rectangle((0,0),1,1, facecolor=c, edgecolor='black') 
#                           for c in colors_cost]
#         ax2.legend(legend_elements, transfer_types, loc='upper right')
#         ax2.grid(True, alpha=0.3, axis='y')
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved network topology impact to {os.path.join(self.output_dir, filename)}")
    
#     def visualize_per_pod_analysis(self, filename='per_pod_analysis.png'):
#         if not self.data or 'metrics' not in self.data:
#             print("No metrics data available")
#             return
        
#         workload = 'ml-training-pipeline'  
        
#         fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
#         dl_pods = []
#         default_pods = []
        
#         key_dl = f"{workload}_data-locality-scheduler_1"
#         key_default = f"{workload}_default-scheduler_1"
        
#         if key_dl in self.data['metrics']:
#             dl_pods = self.data['metrics'][key_dl]['pod_metrics']
        
#         if key_default in self.data['metrics']:
#             default_pods = self.data['metrics'][key_default]['pod_metrics']
        
#         # Plot 1: Pod Placement by Node Type
#         pod_names = []
#         dl_node_types = []
#         default_node_types = []
        
#         for pod in dl_pods:
#             base_name = pod['pod_name'].split('-6bba97c0-')[0]
#             if base_name not in pod_names:
#                 pod_names.append(base_name)
        
#         for pod_name in pod_names:
#             for pod in dl_pods:
#                 if pod_name in pod['pod_name']:
#                     dl_node_types.append(pod['node_type'])
#                     break
            
#             for pod in default_pods:
#                 if pod_name in pod['pod_name']:
#                     default_node_types.append(pod['node_type'])
#                     break
        
#         y_pos = np.arange(len(pod_names))
        
#         node_type_map = {'edge': 0, 'cloud': 1}
#         dl_values = [node_type_map.get(nt, 0) for nt in dl_node_types]
#         default_values = [node_type_map.get(nt, 0) for nt in default_node_types]
        
#         ax1.scatter(dl_values, y_pos, s=100, c=self.colors['data-locality-scheduler'], 
#                    label='Data Locality', edgecolors='black', linewidths=1, zorder=3)
#         ax1.scatter(default_values, y_pos + 0.1, s=100, c=self.colors['default-scheduler'], 
#                    label='Default', edgecolors='black', linewidths=1, marker='s', zorder=3)
        
#         for i in range(len(y_pos)):
#             if dl_values[i] != default_values[i]:
#                 ax1.plot([dl_values[i], default_values[i]], [y_pos[i], y_pos[i] + 0.1], 
#                         'k--', alpha=0.3, zorder=1)
        
#         ax1.set_yticks(y_pos)
#         ax1.set_yticklabels([name.replace('-', '\n') for name in pod_names])
#         ax1.set_xticks([0, 1])
#         ax1.set_xticklabels(['Edge', 'Cloud'])
#         ax1.set_xlabel('Node Type')
#         ax1.set_title(f'Pod Placement Decisions - {self._format_workload_name(workload)}')
#         ax1.legend(loc='upper right')
#         ax1.grid(True, alpha=0.3, axis='x')
#         ax1.set_xlim(-0.5, 1.5)
        
#         dl_scores = []
#         default_scores = []
#         pod_labels = []
        
#         for pod_name in pod_names:
#             for pod in dl_pods:
#                 if pod_name in pod['pod_name']:
#                     dl_scores.append(pod['data_locality']['score'])
#                     break
            
#             for pod in default_pods:
#                 if pod_name in pod['pod_name']:
#                     default_scores.append(pod['data_locality']['score'])
#                     break
            
#             pod_labels.append(pod_name.split('-')[-1])
        
#         x = np.arange(len(pod_labels))
#         width = 0.35
        
#         bars1 = ax2.bar(x - width/2, dl_scores, width, 
#                         label='Data Locality Scheduler',
#                         color=self.colors['data-locality-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
#         bars2 = ax2.bar(x + width/2, default_scores, width, 
#                         label='Default Scheduler',
#                         color=self.colors['default-scheduler'], 
#                         edgecolor='black', linewidth=0.8)
        
#         ax2.set_xlabel('Pod')
#         ax2.set_ylabel('Data Locality Score')
#         ax2.set_title('Per-Pod Data Locality Scores')
#         ax2.set_xticks(x)
#         ax2.set_xticklabels(pod_labels, rotation=45, ha='right')
#         ax2.legend(loc='upper left')
#         ax2.set_ylim(0, 1.1)
#         ax2.grid(True, alpha=0.3)
        
#         plt.tight_layout()
#         plt.savefig(os.path.join(self.output_dir, filename), dpi=300, bbox_inches='tight')
#         plt.savefig(os.path.join(self.output_dir, filename.replace('.png', '.pdf')), 
#                    format='pdf', bbox_inches='tight')
#         plt.close()
        
#         print(f"Saved per-pod analysis to {os.path.join(self.output_dir, filename)}")
    
#     def generate_all_visualizations(self):
#         print("Generating visualizations...")
        
#         self.visualize_data_locality_comparison()
#         self.visualize_local_data_access_patterns()
#         self.visualize_network_transfer_analysis()
#         self.visualize_scheduling_latency_analysis()
#         self.visualize_node_placement_distribution()
        
#         self.visualize_data_access_heatmap()
#         self.visualize_improvement_summary()
#         self.visualize_workload_characteristics()
#         # self.visualize_pod_placement_timeline()
#         self.visualize_network_topology_impact()
#         # self.visualize_per_pod_analysis()
        
#         print(f"\nAll visualizations saved to {self.output_dir}")
#         print("Generated files:")
#         print("- data_locality_comparison.png/pdf")
#         print("- local_data_access_patterns.png/pdf")
#         print("- network_transfer_analysis.png/pdf")
#         print("- scheduling_latency_analysis.png/pdf")
#         print("- node_placement_distribution.png/pdf")
#         print("- data_access_heatmap.png/pdf")
#         print("- improvement_summary.png/pdf")
#         print("- workload_characteristics.png/pdf")
#         print("- pod_placement_timeline.png/pdf")
#         print("- network_topology_impact.png/pdf")
#         print("- per_pod_analysis.png/pdf")


# def main():
#     import argparse
    
#     parser = argparse.ArgumentParser(description='Generate visualizations for scheduler benchmarks')
#     parser.add_argument('--results-dir', type=str, default='benchmarks/simulated/results',
#                         help='Directory containing benchmark results')
#     parser.add_argument('--output-dir', type=str, default='benchmarks/simulated/visualizations',
#                         help='Directory to save visualizations')
#     parser.add_argument('--results-file', type=str, default=None,
#                         help='Specific JSON results file to use')
#     parser.add_argument('--summary-file', type=str, default=None,
#                         help='Specific CSV summary file to use')
    
#     args = parser.parse_args()
    
#     visualizer = BenchmarkVisualizer(results_dir=args.results_dir, output_dir=args.output_dir)
    
#     try:
#         visualizer.load_data(results_file=args.results_file, summary_file=args.summary_file)
#         visualizer.generate_all_visualizations()
#     except Exception as e:
#         print(f"Error generating visualizations: {e}")
#         raise


# if __name__ == '__main__':
#     main()    