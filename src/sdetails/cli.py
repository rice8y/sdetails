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
        self.data: List[Dict] = []
        self.queue_counts: Dict[str, int] = {}

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

    def clear_screen(self):
        print('\033[2J\033[H', end='', flush=True)

    def fetch_queue_counts(self) -> None:
        """Fetch the number of running jobs per partition."""
        try:
            result = subprocess.run(
                ['squeue', '-h', '-o', '%P'], capture_output=True, text=True, check=True
            )
            parts = result.stdout.strip().split()
            counts: Dict[str, int] = {}
            for p in parts:
                counts[p] = counts.get(p, 0) + 1
            self.queue_counts = counts
        except Exception:
            self.queue_counts = {}

    def fetch_data(self) -> bool:
        try:
            sinfo = subprocess.run([
                'sinfo',
                '--Format=Partition,NodeHost,StateCompact,CPUsState,AllocMem,Memory,Gres,GresUsed'
            ], capture_output=True, text=True, check=True)
            lines = sinfo.stdout.strip().split('\n')
            if len(lines) < 2:
                print("Error: Insufficient data from sinfo")
                return False

            self.fetch_queue_counts()
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
                return tuple(int(p) for p in parts)
        except ValueError:
            pass
        return 0, 0, 0, 0

    def parse_gpu_info(self, gres: str, gres_used: str) -> Tuple[int, int]:
        try:
            total_gpu = 0
            if 'gpu:' in gres:
                total_gpu = int(re.findall(r'gpu:(\d+)', gres)[0])
            used_gpu = 0
            if 'gpu:' in gres_used:
                m = re.search(r'gpu:\(.*?\):(\d+)', gres_used)
                if m:
                    used_gpu = int(m.group(1))
                else:
                    used_gpu = sum(int(x) for x in re.findall(r'gpu:.*?:(\d+)', gres_used))
            return used_gpu, total_gpu
        except:
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
        idle_nodes = sum('idle' in n['state'].lower() for n in self.data)
        alloc_nodes = sum('alloc' in n['state'].lower() for n in self.data)
        mix_nodes = sum('mix' in n['state'].lower() for n in self.data)
        down_nodes = sum(any(x in n['state'].lower() for x in ['down','drain']) for n in self.data)
        total_cpus = sum(self.parse_cpu_info(n['cpu'])[3] for n in self.data)
        idle_cpus = sum(self.parse_cpu_info(n['cpu'])[1] for n in self.data)
        total_mem = sum(n['memory'] for n in self.data)
        free_mem = sum(n['memory'] - n['allocmem'] for n in self.data)
        total_gpus = used_gpus = 0
        for n in self.data:
            u, t = self.parse_gpu_info(n['gres'], n['gres_used'])
            total_gpus += t; used_gpus += u
        free_gpus = total_gpus - used_gpus

        print(f"\n{self.colorize('=== Cluster Summary ===', Colors.BOLD + Colors.CYAN)}")
        print(f"Nodes: {self.colorize(str(total_nodes), Colors.BOLD)} (Idle: {self.colorize(str(idle_nodes), Colors.GREEN)}, Mix: {self.colorize(str(mix_nodes), Colors.YELLOW)}, Alloc: {self.colorize(str(alloc_nodes), Colors.YELLOW)}, Down: {self.colorize(str(down_nodes), Colors.RED)})")
        print(f"CPUs: {idle_cpus}/{total_cpus} available ({self.colorize(f'{idle_cpus/total_cpus*100:.1f}%', self.get_usage_color(total_cpus-idle_cpus, total_cpus))})")
        print(f"Memory: {self.format_memory(free_mem)}/{self.format_memory(total_mem)} available ({self.colorize(f'{free_mem/total_mem*100:.1f}%', self.get_usage_color(total_mem-free_mem, total_mem))})")
        if total_gpus > 0:
            print(f"GPUs: {free_gpus}/{total_gpus} available ({self.colorize(f'{free_gpus/total_gpus*100:.1f}%', self.get_usage_color(used_gpus, total_gpus))})")

    def get_display_width(self, text: str) -> int:
        return len(re.sub(r'\033\[[0-9;]*m', '', text))

    def pad_text(self, text: str, width: int, align: str = 'left') -> str:
        display_width = self.get_display_width(text)
        pad = width - display_width
        if pad <= 0: return text
        if align == 'right': return ' ' * pad + text
        if align == 'center': return ' ' * (pad//2) + text + ' ' * (pad - pad//2)
        return text + ' ' * pad

    def print_detailed_table(self, partition_filter: str = None, sort_by: str = 'nodename'):
        if not self.data:
            print("No data available")
            return
        data = self.data if not partition_filter else [n for n in self.data if n['partition']==partition_filter]
        if not data:
            print(f"Partition '{partition_filter}' not found")
            return
        if sort_by == 'partition': data.sort(key=lambda x: (x['partition'], x['nodename']))
        elif sort_by == 'state': data.sort(key=lambda x: (x['state'], x['nodename']))
        elif sort_by == 'cpu': data.sort(key=lambda x: self.parse_cpu_info(x['cpu'])[1], reverse=True)
        else: data.sort(key=lambda x: x['nodename'])

        headers = ["Partition", "NodeName", "State", "CPU (Free/Total)", "Memory (Free/Total)", "GPU (Used/Total)", "Queue"]
        col_widths = [len(h)+2 for h in headers]
        rows = []
        for n in data:
            alloc, idle, other, total = self.parse_cpu_info(n['cpu'])
            ug, tg = self.parse_gpu_info(n['gres'], n['gres_used'])
            free_mem_g = (n['memory'] - n['allocmem'])//1024
            total_mem_g = n['memory']//1024
            q = self.queue_counts.get(n['partition'], 0)
            row = [n['partition'], n['nodename'], n['state'], f"{idle}/{total}", f"{free_mem_g}G/{total_mem_g}G", (f"{ug}/{tg}" if tg>0 else "N/A"), str(q)]
            rows.append(row)
            for idx, cell in enumerate(row):
                col_widths[idx] = max(col_widths[idx], len(cell)+2)

        sep_line = lambda sep: sep.join('─'*w for w in col_widths)
        header_line = '┌' + sep_line('┬') + '┐'
        mid_sep = '├' + sep_line('┼') + '┤'
        footer_line = '└' + sep_line('┴') + '┘'
        print(header_line)
        row_str = '│'
        for h, w in zip(headers, col_widths): row_str += self.pad_text(h, w) + '│'
        print(row_str)
        print(mid_sep)
        for row in rows:
            row_str = '│'
            for idx, cell in enumerate(row):
                text = cell
                if idx == 2:
                    text = self.colorize(cell, self.get_state_color(cell))
                if idx == 3:
                    used = total - int(cell.split('/')[1])
                    text = self.colorize(cell, self.get_usage_color(used, total))
                if idx == 4:
                    used_mem = total_mem_g*1024 - free_mem_g*1024
                    text = self.colorize(cell, self.get_usage_color(used_mem, total_mem_g*1024))
                if idx ==5 and tg>0:
                    text = self.colorize(cell, self.get_usage_color(ug, tg))
                row_str += self.pad_text(text, col_widths[idx]) + '│'
            print(row_str)
        print(footer_line)
        print(" * --- Default Partition\n")

    def export_json(self, filename: str):
        try:
            nodes = []
            for n in self.data:
                alloc, idle, other, total = self.parse_cpu_info(n['cpu'])
                ug, tg = self.parse_gpu_info(n['gres'], n['gres_used'])
                nodes.append({
                    'partition': n['partition'],
                    'nodename': n['nodename'],
                    'state': n['state'],
                    'cpu': {'allocated': alloc, 'idle': idle, 'other': other, 'total': total},
                    'memory': {'allocated_mb': n['allocmem'], 'total_mb': n['memory'], 'free_mb': n['memory']-n['allocmem']},
                    'gpu': {'used': ug, 'total': tg, 'free': tg-ug},
                    'queue': self.queue_counts.get(n['partition'], 0)
                })
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({'timestamp': datetime.now().isoformat(), 'nodes': nodes}, f, indent=2, ensure_ascii=False)
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