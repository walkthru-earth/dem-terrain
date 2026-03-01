# dem-terrain

One-time pipeline that converts the [GEDTM-30m](https://github.com/openlandmap/GEDTM30) global DEM into H3-indexed [native Parquet 2.11+](https://github.com/apache/parquet-format/blob/master/Geospatial.md) files with pre-computed terrain derivatives.

Replaces the 20-40 min on-the-fly DEM load in [walkthru-weather-index](../walkthru-weather-index/) with a ~5 second Parquet scan.

## What it produces

```
s3://{bucket}/{prefix}/dem-terrain/
  _metadata.json
  h3_res=1/data.parquet          # ~250 cells
  h3_res=2/data.parquet          # ~1,800 cells
  ...
  h3_res=5/data.parquet          # ~600K cells
  h3_res=6/h3_parent_2={id}/data.parquet   # partitioned, ~4.2M cells total
  ...
  h3_res=10/h3_parent_2={id}/data.parquet  # partitioned, ~10B cells total
```

Each file contains: `h3_index`, `geometry` (native Parquet GEOMETRY), `lat`, `lon`, `elev`, `slope`, `aspect`, `tri`, `tpi`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [OpenTofu](https://opentofu.org/) (for cloud deployment)
- AWS credentials with S3 write access
- Verda cloud account (for GPU instance)

## Quick start

```bash
# Clone and enter the project
cd dem/

# Install dependencies
uv sync

# Copy and fill in your credentials
cp .env.example .env
# edit .env with your S3 bucket, AWS keys, etc.

# Dry run — check STAC catalog connectivity
uv run python main.py --dry-run

# Run locally (low resolutions only, for testing)
uv run python main.py --resolutions 1,2,3,4,5 --scratch-dir ./scratch
```

## Cloud deployment (full global run)

The full pipeline (res 1-10) needs a GPU instance with ~182 GB RAM. See [docs/workflow.md](docs/workflow.md) for the complete workflow.

```bash
cd infra/

# Create secrets.tfvars (see infra/variables.tf for all vars)
cat > secrets.tfvars <<EOF
verda_client_id     = "your-client-id"
verda_client_secret = "your-client-secret"
ssh_public_key      = "ssh-ed25519 AAAA..."
s3_bucket           = "your-bucket"
s3_prefix           = "your-prefix"
aws_access_key_id   = "AKIA..."
aws_secret_access_key = "..."
EOF

# Deploy
tofu init
tofu plan -var-file="secrets.tfvars"
tofu apply -var-file="secrets.tfvars"

# SSH in and monitor
ssh root@$(tofu output -raw instance_ip)
tmux attach -t dem

# After completion, tear down
tofu destroy -var-file="secrets.tfvars"
```

## Query the output

```sql
-- DuckDB
INSTALL spatial; LOAD spatial;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-east-1';

SELECT h3_index, elev, slope
FROM read_parquet('s3://bucket/prefix/dem-terrain/h3_res=5/data.parquet')
WHERE lat BETWEEN 27.5 AND 28.5
  AND lon BETWEEN 86.5 AND 87.5
ORDER BY elev DESC
LIMIT 5;
```

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Setup pre-commit hooks
uv run pre-commit install

# Lint and format
uv run ruff check .
uv run ruff format .
```

## Docs

- [docs/workflow.md](docs/workflow.md) — full pipeline workflow, architecture, and processing details

## License

This project is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). See [LICENSE](LICENSE) for details.

Contact: [hi@walkthru.earth](mailto:hi@walkthru.earth)
