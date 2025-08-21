# SDetails

SDetails is a Python-based CLI utility designed to improve the visibility of SLURM cluster resources. It provides a user-friendly terminal dashboard showing per-node CPU, memory, and GPU utilization with optional color highlighting, sorting, filtering, and JSON export. Ideal for HPC users and admins who need real-time or static insights into node status.

## Installation

You can install this CLI tool using `uv` or `pip` in a few different ways:

### A. Install directly from GitHub (recommended)

Using `uv`:

```bash
uv tool install git+https://github.com/rice8y/sdetails.git
```

Or using `pip`:

```bash
pip install git+https://github.com/rice8y/sdetails.git
```

This will fetch and install the latest version directly from the repository.

### B. Install from a local clone

1. Clone the repository:

```bash
git clone https://github.com/rice8y/sdetails.git
```

2. Move into the project directory:

```bash
cd sdetails
```

3. Install the package in editable mode using `uv tool` (or `pip`):

```bash
uv tool install -e .
# or
pip install -e .
```

This is useful if you plan to modify the code locally.

## Usage

SDetails is a CLI tool that enhances SLURM cluster node monitoring with a colorized summary and detailed views.

### Example

```bash
sdetails
```

This command fetches and displays a summary and detailed table of all SLURM nodes.

### Options

- `-p`, `--partition <PART>`: Filter by a specific partition
- `-s`, `--sort <FIELD>`: Sort by `nodename`, `partition`, `state`, or `cpu` (default: `nodename`)
- `--no-color`: Disable color output
- `--no-summary`: Skip the cluster summary section
- `--export <FILE>`: Export the current view to a JSON file
- `--watch <SECONDS>`: Refresh the view every N seconds (Ctrl+C to exit)

### Example with options

```bash
sdetails -p gpu --sort cpu --watch 10 --export status.json
```

This will display only the `gpu` partition, sort by CPU availability, auto-refresh every 10 seconds, and export to `status.json`.

## License

This project is distributed under the MIT License. See [LICENSE](LICENSE).