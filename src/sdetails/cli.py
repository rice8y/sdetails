import subprocess
import re
import sys
import argparse
from datetime import datetime
from typing import List, Dict, Tuple
import json
import os

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class SlurmMonitor:
    def __init__(self, use_color: bool = True):
        self.use_color = use_color
        self.data = []
        self.running_counts = {}
        self.queued_counts = {}
        self.queued_by_partition = {}
        self.multi_partitions = set()

    def colorize(self, text: str, color: str) -> str:
        if not self.use_color:
            return text
        return f"{color}{text}{Colors.END}"

    def get_state_color(self, state: str) -> str:
        if not self.use_color:
            return ""
        state_lower = state.lower()
        if 'idle' in state_lower:
            return Colors.GREEN
        elif 'alloc' in state_lower or 'mix' in state_lower:
            return Colors.YELLOW
        elif 'down' in state_lower or 'drain' in state_lower:
            return Colors.RED
        else:
            return Colors.WHITE

    def get_usage_color(self, used: int, total: int, threshold: float = 0.8) -> str:
        if not self.use_color or total == 0:
            return ""
        usage_rate = used / total
        if usage_rate >= threshold:
            return Colors.RED
        elif usage_rate >= 0.5:
            return Colors.YELLOW
        else:
            return Colors.GREEN

    def get_queue_color(self, running_count: int, queued_count: int) -> str:
        if not self.use_color:
            return ""
        total_jobs = running_count + queued_count
        if total_jobs >= 12:
            return Colors.RED
        elif total_jobs >= 6:
            return Colors.YELLOW
        elif total_jobs >= 0:
            return Colors.GREEN
        else:
            return Colors.WHITE

    def clear_screen(self):
        print('\033[2J\033[H', end='', flush=True)

    def fetch_queue_counts(self) -> bool:
        try:
            result = subprocess.run([
                'squeue', '-h', '-o', '%i %t %P %N'
            ], capture_output=True, text=True, check=True)
            
            self.running_counts = {}
            # self.queued_counts = {}
            self.queued_by_partition = {}
            
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 3:
                        job_state = parts[1]
                        part = parts[2]
                        if len(parts) == 3:
                            nodes = parts[2]
                        else:
                            nodes = parts[3]
                        
                        if job_state == 'R':
                            for node in nodes.split(','):
                                node = node.strip()
                                if node:
                                    self.running_counts[node] = self.running_counts.get(node, 0) + 1
                        elif job_state == 'PD':
                            self.queued_by_partition[part] = self.queued_by_partition.get(part, 0) + 1

            return True
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to execute squeue command: {e}")
            return False
        except Exception as e:
            print(f"Warning: Problem occurred during queue data fetching: {e}")
            return False

    def fetch_data(self) -> bool:
        try:
            result = subprocess.run([
                'sinfo', 
                '--Format=Partition,NodeHost,StateCompact,CPUsState,AllocMem,Memory,Gres,GresUsed'
            ], capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            if len(lines) < 2:
                print("Error: Insufficient data from sinfo")
                return False
            
            sinfo = subprocess.run(
                ['sinfo', '-h', '-o', '%P %D'],
                capture_output=True, text=True, check=True
            )
            for l in sinfo.stdout.strip().splitlines():
                p, d = l.split()
                if d.isdigit() and int(d) > 1:
                    self.multi_partitions.add(p)
            
            self.data = []
            for line in lines[1:]:
                fields = line.split()
                if len(fields) >= 8:
                    self.data.append({
                        'partition': fields[0],
                        'nodename': fields[1],
                        'state': fields[2],
                        'cpu': fields[3],
                        'allocmem': int(fields[4]) if fields[4].isdigit() else 0,
                        'memory': int(fields[5]) if fields[5].isdigit() else 0,
                        'gres': fields[6],
                        'gres_used': fields[7]
                    })
            
            self.fetch_queue_counts()
            
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to execute sinfo command: {e}")
            return False
        except Exception as e:
            print(f"Error: Problem occurred during data fetching: {e}")
            return False

    def parse_cpu_info(self, cpu_str: str) -> Tuple[int, int, int, int]:
        try:
            parts = cpu_str.split('/')
            if len(parts) == 4:
                allocated = int(parts[0])
                idle = int(parts[1])
                other = int(parts[2])
                total = int(parts[3])
                return allocated, idle, other, total
        except ValueError:
            pass
        return 0, 0, 0, 0

    def parse_gpu_info(self, gres: str, gres_used: str) -> Tuple[int, int]:
        try:
            total_gpu = 0
            if gres and 'gpu:' in gres:
                match = re.findall(r'gpu:(\d+)', gres)
                if match:
                    total_gpu = int(match[0])
            used_gpu = 0
            if gres_used and 'gpu:' in gres_used:
                match = re.search(r'gpu:\(.*?\):(\d+)', gres_used)
                if match:
                    used_gpu = int(match.group(1))
                else:
                    match = re.findall(r'gpu:.*?:(\d+)', gres_used)
                    if match:
                        used_gpu = sum(int(m) for m in match)
            return used_gpu, total_gpu
        except (ValueError, AttributeError):
            return 0, 0

    def format_memory(self, mem_mb: int) -> str:
        if mem_mb >= 1024 * 1024:
            return f"{mem_mb / (1024 * 1024):.1f}T"
        elif mem_mb >= 1024:
            return f"{mem_mb / 1024:.1f}G"
        else:
            return f"{mem_mb}M"

    def print_summary(self):
        if not self.data:
            return
        total_nodes = len(self.data)
        idle_nodes = sum(1 for node in self.data if 'idle' in node['state'].lower())
        alloc_nodes = sum(1 for node in self.data if 'alloc' in node['state'].lower())
        mix_nodes = sum(1 for node in self.data if 'mix' in node['state'].lower())
        down_nodes = sum(1 for node in self.data if 'down' in node['state'].lower() or 'drain' in node['state'].lower())
        total_cpus = sum(self.parse_cpu_info(node['cpu'])[3] for node in self.data)
        idle_cpus = sum(self.parse_cpu_info(node['cpu'])[1] for node in self.data)
        total_mem = sum(node['memory'] for node in self.data)
        free_mem = sum(node['memory'] - node['allocmem'] for node in self.data)
        total_gpus = 0
        used_gpus = 0
        for node in self.data:
            used_gpu, total_gpu = self.parse_gpu_info(node['gres'], node['gres_used'])
            total_gpus += total_gpu
            used_gpus += used_gpu
        free_gpus = total_gpus - used_gpus
        
        # Calculate total running and queued jobs
        total_running_jobs = sum(self.running_counts.values())
        total_queued_jobs = sum(self.queued_by_partition.values())
        
        print(f"\n{self.colorize('=== Cluster Summary ===', Colors.BOLD + Colors.CYAN)}")
        print(f"Nodes: {self.colorize(str(total_nodes), Colors.BOLD)} "
              f"(Idle: {self.colorize(str(idle_nodes), Colors.GREEN)}, "
              f"Mix: {self.colorize(str(mix_nodes), Colors.YELLOW)}, "
              f"Allocated: {self.colorize(str(alloc_nodes), Colors.YELLOW)}, "
              f"Down: {self.colorize(str(down_nodes), Colors.RED)})")
        cpu_pct = idle_cpus/total_cpus*100 if total_cpus > 0 else 0
        print(f"CPUs: {idle_cpus}/{total_cpus} available "
              f"({self.colorize(f'{cpu_pct:.1f}%', self.get_usage_color(total_cpus-idle_cpus, total_cpus))})")
        mem_pct = free_mem/total_mem*100 if total_mem > 0 else 0
        print(f"Memory: {self.format_memory(free_mem)}/{self.format_memory(total_mem)} available "
              f"({self.colorize(f'{mem_pct:.1f}%', self.get_usage_color(total_mem-free_mem, total_mem))})")
        if total_gpus > 0:
            gpu_pct = free_gpus/total_gpus*100
            print(f"GPUs: {free_gpus}/{total_gpus} available "
                  f"({self.colorize(f'{gpu_pct:.1f}%', self.get_usage_color(used_gpus, total_gpus))})")
        print(f"Jobs: {self.colorize(str(total_running_jobs), Colors.BOLD)} running, "
              f"{self.colorize(str(total_queued_jobs), Colors.BOLD)} queued")
        print()

    def get_display_width(self, text: str) -> int:
        clean_text = re.sub(r'\033\[[0-9;]*m', '', text)
        return len(clean_text)

    def pad_text(self, text: str, width: int, align: str = 'left') -> str:
        display_width = self.get_display_width(text)
        padding = width - display_width
        if padding <= 0:
            return text
        if align == 'right':
            return ' ' * padding + text
        elif align == 'center':
            left_pad = padding // 2
            right_pad = padding - left_pad
            return ' ' * left_pad + text + ' ' * right_pad
        else:
            return text + ' ' * padding

    def print_detailed_table(self, partition_filter: str = None, sort_by: str = 'nodename'):
        if not self.data:
            print("No data available")
            return
        filtered_data = self.data
        if partition_filter:
            filtered_data = [node for node in self.data if node['partition'] == partition_filter]
            if not filtered_data:
                print(f"Partition '{partition_filter}' not found")
                return
        if sort_by == 'partition':
            filtered_data.sort(key=lambda x: (x['partition'], x['nodename']))
        elif sort_by == 'state':
            filtered_data.sort(key=lambda x: (x['state'], x['nodename']))
        elif sort_by == 'cpu':
            filtered_data.sort(key=lambda x: (self.parse_cpu_info(x['cpu'])[1], x['nodename']), reverse=True)
        else:
            filtered_data.sort(key=lambda x: x['nodename'])
        
        headers = ["Partition", "NodeName", "State", "CPU (Free/Total)", "Memory (Free/Total)", "GPU (Used/Total)", "Jobs (Run/Queue)"]
        col_widths = [len(h) + 2 for h in headers]
        
        for node in filtered_data:
            allocated_cpu, idle_cpu, other_cpu, total_cpu = self.parse_cpu_info(node['cpu'])
            used_gpu, total_gpu = self.parse_gpu_info(node['gres'], node['gres_used'])
            free_mem_gb = (node['memory'] - node['allocmem']) // 1024
            total_mem_gb = node['memory'] // 1024
            running_count = self.running_counts.get(node['nodename'], 0)
            queued_count = self.queued_counts.get(node['nodename'], 0)
            
            partition_len = len(node['partition']) + 2
            nodename_len = len(node['nodename']) + 2
            state_len = len(node['state']) + 2
            cpu_len = len(f"{idle_cpu}/{total_cpu}") + 2
            mem_len = len(f"{free_mem_gb}G/{total_mem_gb}G") + 2
            gpu_len = len(f"{used_gpu}/{total_gpu}" if total_gpu > 0 else "N/A") + 2
            jobs_len = len(f"{running_count}/{queued_count}") + 2
            
            col_widths[0] = max(col_widths[0], partition_len)
            col_widths[1] = max(col_widths[1], nodename_len)
            col_widths[2] = max(col_widths[2], state_len)
            col_widths[3] = max(col_widths[3], cpu_len)
            col_widths[4] = max(col_widths[4], mem_len)
            col_widths[5] = max(col_widths[5], gpu_len)
            col_widths[6] = max(col_widths[6], jobs_len)
        
        header = "┌" + "┬".join("─" * w for w in col_widths) + "┐"
        separator = "├" + "┼".join("─" * w for w in col_widths) + "┤"
        footer = "└" + "┴".join("─" * w for w in col_widths) + "┘"
        
        print(header)
        header_row = "│"
        for header_text, width in zip(headers, col_widths):
            header_row += self.pad_text(header_text, width) + "│"
        print(header_row)
        print(separator)
        
        for node in filtered_data:
            allocated_cpu, idle_cpu, other_cpu, total_cpu = self.parse_cpu_info(node['cpu'])
            used_gpu, total_gpu = self.parse_gpu_info(node['gres'], node['gres_used'])
            free_mem_gb = (node['memory'] - node['allocmem']) // 1024
            total_mem_gb = node['memory'] // 1024
            # queue_count = self.queued_counts.get(node['nodename'], 0)
            running_count = self.running_counts.get(node['nodename'], 0)

            if node['partition'] in self.multi_partitions:
                if node['partition'].endswith('*'):
                    part = node['partition'].split('*')[0]
                else:
                    part = node['partition']
                queued_count = self.queued_by_partition.get(part, 0)
                marker = '**'
            else:
                queued_count = self.queued_by_partition.get(node['nodename'], 0)
                marker = ''
            jobs_str = f"{running_count}/{queued_count}{marker}"
            
            state_colored = self.colorize(node['state'], self.get_state_color(node['state']))
            cpu_info = f"{idle_cpu}/{total_cpu}"
            cpu_colored = self.colorize(cpu_info, self.get_usage_color(allocated_cpu, total_cpu))
            mem_info = f"{free_mem_gb}G/{total_mem_gb}G"
            mem_colored = self.colorize(mem_info, self.get_usage_color(node['allocmem'], node['memory']))
            gpu_info = f"{used_gpu}/{total_gpu}" if total_gpu > 0 else "N/A"
            gpu_colored = self.colorize(gpu_info, self.get_usage_color(used_gpu, total_gpu)) if total_gpu > 0 else "N/A"

            # jobs_str = f"{running_count}/{queue_count}"
            # jobs_colored = self.colorize(jobs_str, self.get_queue_color(running_count, queue_count))
            row_data = [node['partition'], node['nodename'], state_colored, cpu_colored, mem_colored, gpu_colored, jobs_str]


            row = "│"
            for data, width in zip(row_data, col_widths):
                row += self.pad_text(data, width) + "│"
            print(row)
        print(footer)
        print(" * --- Default Partition")
        if self.multi_partitions:
            print(" ** --- Queued job counts for this partition reflect the total before jobs are assigned to individual nodes")
        print()

    def export_json(self, filename: str):
        try:
            export_data = []
            for node in self.data:
                allocated_cpu, idle_cpu, other_cpu, total_cpu = self.parse_cpu_info(node['cpu'])
                used_gpu, total_gpu = self.parse_gpu_info(node['gres'], node['gres_used'])
                queue_count = self.queue_counts.get(node['nodename'], 0)
                
                export_data.append({
                    'partition': node['partition'],
                    'nodename': node['nodename'],
                    'state': node['state'],
                    'cpu': {
                        'allocated': allocated_cpu,
                        'idle': idle_cpu,
                        'other': other_cpu,
                        'total': total_cpu
                    },
                    'memory': {
                        'allocated_mb': node['allocmem'],
                        'total_mb': node['memory'],
                        'free_mb': node['memory'] - node['allocmem']
                    },
                    'gpu': {
                        'used': used_gpu,
                        'total': total_gpu,
                        'free': total_gpu - used_gpu
                    },
                    'running_jobs': queue_count
                })
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'nodes': export_data
                }, f, indent=2, ensure_ascii=False)
            print(f"Data exported to {filename}")
        except Exception as e:
            print(f"Error: JSON export failed: {e}")

def main():
    parser = argparse.ArgumentParser(description='Enhanced SLURM Node Monitor')
    parser.add_argument('-p', '--partition', help='Show only specified partition')
    parser.add_argument('-s', '--sort', choices=['nodename', 'partition', 'state', 'cpu'], default='nodename', help='Sort criteria (default: nodename)')
    parser.add_argument('--no-color', action='store_true', help='Disable color output')
    parser.add_argument('--no-summary', action='store_true', help='Disable summary display')
    parser.add_argument('--export', help='Export data to JSON file')
    parser.add_argument('--watch', type=int, metavar='SECONDS', help='Auto-refresh every N seconds')
    args = parser.parse_args()
    monitor = SlurmMonitor(use_color=not args.no_color)
    
    def display_data():
        if not monitor.fetch_data():
            sys.exit(1)
        if not args.no_summary:
            monitor.print_summary()
        monitor.print_detailed_table(partition_filter=args.partition, sort_by=args.sort)
        if args.export:
            monitor.export_json(args.export)
    
    if args.watch:
        import time
        try:
            while True:
                monitor.clear_screen()
                print(f"{monitor.colorize('SLURM Node Monitor', Colors.BOLD + Colors.CYAN)} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Auto-refresh: {args.watch}s interval (Ctrl+C to exit)\n")
                display_data()
                sys.stdout.flush()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print(f"\n{monitor.colorize('Monitoring stopped', Colors.YELLOW)}")
    else:
        display_data()

if __name__ == "__main__":
    main()