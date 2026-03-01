#!/usr/bin/env bash
# Instance boot script: install dependencies, mount NVMe, run DEM pipeline.
# Runs automatically on Verda instance creation via startup_script_id.
# Target: CPU Node (360 vCPUs, 1440 GB RAM, AMD EPYC)
set -euo pipefail

LOG="/var/log/dem-pipeline-setup.log"
exec > >(tee -a "$LOG") 2>&1

echo "=== DEM Pipeline Setup — $(date -u) ==="
echo "CPUs: $(nproc), RAM: $(free -h | awk '/Mem:/{print $2}')"

# ---------------------------------------------------------------------------
# 1. Mount the 2TB NVMe volume
# ---------------------------------------------------------------------------
echo "--- Mounting NVMe volume ---"
DATA_DEV="/dev/vdb"  # Verda attaches volumes as vdb by default
MOUNT_POINT="/data"

if ! mountpoint -q "$MOUNT_POINT"; then
    mkdir -p "$MOUNT_POINT"
    if ! blkid "$DATA_DEV" | grep -q ext4; then
        mkfs.ext4 -F "$DATA_DEV"
    fi
    mount "$DATA_DEV" "$MOUNT_POINT"
    echo "$DATA_DEV $MOUNT_POINT ext4 defaults,nofail 0 2" >> /etc/fstab
fi
mkdir -p /data/scratch /data/project
echo "Volume mounted at $MOUNT_POINT"

# ---------------------------------------------------------------------------
# 2. Install system dependencies
# ---------------------------------------------------------------------------
echo "--- Installing system packages ---"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
    build-essential \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    tmux \
    curl \
    git \
    jq

# ---------------------------------------------------------------------------
# 3. Install uv
# ---------------------------------------------------------------------------
echo "--- Installing uv ---"
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi
echo "uv version: $(uv --version)"

# ---------------------------------------------------------------------------
# 4. Copy project files
# ---------------------------------------------------------------------------
echo "--- Setting up project ---"
PROJECT_DIR="/data/project/dem"
mkdir -p "$PROJECT_DIR"

if [ -f /root/dem-project/pyproject.toml ]; then
    cp -r /root/dem-project/* "$PROJECT_DIR/"
elif [ -f /tmp/dem-project/pyproject.toml ]; then
    cp -r /tmp/dem-project/* "$PROJECT_DIR/"
else
    echo "WARNING: Project files not found. Copy them manually:"
    echo "  scp -r ./pyproject.toml ./main.py root@<ip>:$PROJECT_DIR/"
fi

# ---------------------------------------------------------------------------
# 5. Set environment variables
# ---------------------------------------------------------------------------
echo "--- Configuring environment ---"
cat > /data/project/dem/.env <<ENVEOF
S3_BUCKET=${S3_BUCKET:-}
S3_PREFIX=${S3_PREFIX:-}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-}
AWS_REGION=${AWS_REGION:-us-east-1}
SCRATCH_DIR=/data/scratch
GDAL_CACHEMAX=8192
GDAL_NUM_THREADS=ALL_CPUS
CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif,.tiff
OMP_NUM_THREADS=360
OPENBLAS_NUM_THREADS=360
ENVEOF

set -a
source /data/project/dem/.env
set +a

echo "set -a; source /data/project/dem/.env; set +a" >> /root/.bashrc

# ---------------------------------------------------------------------------
# 6. Install Python dependencies (CPU-only, no GPU extras)
# ---------------------------------------------------------------------------
echo "--- Installing Python dependencies ---"
cd "$PROJECT_DIR"

if [ -f pyproject.toml ]; then
    uv sync 2>&1
fi

# ---------------------------------------------------------------------------
# 7. Run the pipeline in tmux (survives SSH disconnect)
# ---------------------------------------------------------------------------
echo "--- Launching pipeline in tmux ---"
tmux new-session -d -s dem -c "$PROJECT_DIR" \
    "set -a; source .env; set +a; uv run python main.py 2>&1 | tee /data/scratch/pipeline.log; echo 'PIPELINE FINISHED' >> /data/scratch/pipeline.log"

echo "=== Setup complete — $(date -u) ==="
echo "Pipeline running in tmux session 'dem'."
echo "Attach with: tmux attach -t dem"
echo "Monitor with: tail -f /data/scratch/pipeline.log"
