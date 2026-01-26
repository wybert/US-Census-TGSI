# Quick Start Guide

## What We Just Did ✅

1. **Centralized Configuration** - All paths now in `setting.json`
2. **Created Snakemake Workflow** - Automated pipeline in `Snakefile`
3. **Organized Outputs** - Structured directories for results
4. **Git Repository** - Version control initialized and first commit made
5. **Updated All Scripts** - 14 scripts now use centralized config

## File Structure

```
US-Census-TGSI/
├── setting.json          # Central configuration (all paths here)
├── Snakefile            # Snakemake workflow definition
├── PIPELINE.md          # Detailed pipeline documentation
├── CLAUDE.md            # Project architecture guide
├── .gitignore           # Git ignore patterns
│
├── 0.1-download_cenus_data.py                    # Census download
├── 0.2.1-combine-geo-tweets-archive-and-sentiment.py  # Tweet merge
├── 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py  # Spatial join
├── 0.4.x-validation-*.py                         # Validation scripts
├── 0.6.x-cor-*.py                                # Correlation analysis
│
└── outputs/             # All results go here
    ├── validation/      # Plots and visualizations
    ├── correlation/     # Correlation results
    ├── gini/           # Gini coefficient analysis
    └── logs/           # SLURM job logs
```

## How to Use Snakemake

### 1. See What Would Run (Dry Run)

```bash
snakemake -n
```

This shows you what rules would execute without actually running them.

### 2. Run Specific Parts

```bash
# Only download census data
snakemake download_only -j 1

# Only validation analysis
snakemake validation_only -j 4

# Only correlation analysis
snakemake correlation_only -j 4

# Everything
snakemake all -j 4
```

### 3. Run on SLURM

```bash
# Submit to cluster with automatic resource management
snakemake --cluster "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time} -o outputs/logs/{rule}_%j.out -e outputs/logs/{rule}_%j.err" -j 10
```

### 4. Visualize Pipeline

```bash
# Generate dependency graph (requires graphviz)
snakemake --dag | dot -Tpng > outputs/pipeline_dag.png
```

## Pipeline Stages

| Stage | Script | Resources | Time |
|-------|--------|-----------|------|
| 📥 Download | 0.1 | 1 CPU, 2GB | 2h |
| 🔗 Merge Tweets | 0.2.1 | 110 CPUs, 100GB | 12h |
| 📍 Spatial Join | 0.3.2 | 110 CPUs, 900GB | 3d |
| 📊 Validation | 0.4.x | 4 CPUs, 32GB | 1h each |
| 📈 Correlation | 0.6.x | 4 CPUs, 32GB | 1h each |

## Common Commands

```bash
# Check what needs to be updated
snakemake -n -r

# Run with detailed output
snakemake -j 4 --verbose

# Force re-run a specific rule
snakemake -f gini_analysis

# Keep going even if some jobs fail
snakemake -j 4 --keep-going

# Clean output files (safe)
snakemake clean
```

## Configuration

All paths are in `setting.json`:

```json
{
  "workspace": "/path/to/workspace",
  "census_data_2020": "/path/to/census",
  "outputs_dir": "/path/to/outputs",
  ...
}
```

Change paths there, not in individual scripts!

## Advantages of This Setup

### ✅ Centralized Config
- Change paths in ONE place (`setting.json`)
- No more hardcoded paths in scripts

### ✅ Smart Execution
- Only runs what's needed (checks file timestamps)
- Automatically resumes from where it stopped
- Parallel execution where possible

### ✅ SLURM Integration
- Automatic job submission with correct resources
- Logs organized in `outputs/logs/`
- Resource requirements defined per rule

### ✅ Reproducibility
- Version controlled with Git
- Documented dependencies
- Visual pipeline graphs

## Next Steps

1. **Review the pipeline**:
   ```bash
   snakemake -n
   ```

2. **Test with a small target**:
   ```bash
   snakemake download_only -j 1
   ```

3. **Run validation only** (if data already exists):
   ```bash
   snakemake validation_only -j 4
   ```

4. **Run full pipeline on SLURM**:
   ```bash
   snakemake --cluster "..." -j 20
   ```

## Troubleshooting

**Q: Snakemake not found?**
```bash
conda install -c conda-forge -c bioconda snakemake
```

**Q: Need to re-run everything?**
```bash
snakemake -f all -j 4
```

**Q: Check why a rule would run?**
```bash
snakemake -n -r <rule_name>
```

**Q: See shell commands without running?**
```bash
snakemake -n -p
```

## Documentation

- **PIPELINE.md** - Complete pipeline documentation
- **CLAUDE.md** - Project architecture and data flow
- **Snakefile** - Contains inline comments for each rule

## Git Status

All important files are now tracked:
```bash
git status          # Check status
git log --oneline   # See commits
git diff            # See changes
```

## Support

For Snakemake help:
- Official docs: https://snakemake.readthedocs.io/
- Tutorial: https://snakemake.readthedocs.io/en/stable/tutorial/tutorial.html

For project-specific questions, see CLAUDE.md and PIPELINE.md.

---

**Remember**: Always dry-run first!
```bash
snakemake -n
```
