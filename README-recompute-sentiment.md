# 重新计算缺失的Sentiment文件指南

## 概述 (Overview)

这套工具用于识别和重新计算缺失的BERT情感分析文件。

## 工作流程 (Workflow)

### 步骤 0: 检查缺失文件

首先运行检测脚本找出所有缺失的sentiment文件：

```bash
python 0.1.5-find-missing-sentiment-files.py
```

**输出**:
- `outputs/missing_sentiment_files.csv` - 缺失文件列表
- `outputs/missing_sentiment_summary.txt` - 摘要报告

### 步骤 1: 重新计算Sentiment

有两种方式运行重新计算：

#### 方法 A: 使用SLURM提交单个年份

```bash
# 提交单个年份的任务（例如2014年）
sbatch 0.1.6-recompute-sentiment-slurm.sh 2014
```

#### 方法 B: 批量提交所有缺失年份

```bash
# 一次性提交所有需要处理的年份（2014, 2017, 2020, 2023）
bash 0.1.7-submit-recompute-sentiment-jobs.sh
```

这会为每个年份创建一个独立的GPU任务，实现并行处理。

#### 方法 C: 手动运行（用于测试）

```bash
# 激活环境
conda activate geo

# 运行单个年份（dry run测试）
python 0.1.6-recompute-missing-sentiment.py --year 2014 --dry_run

# 实际运行
python 0.1.6-recompute-missing-sentiment.py --year 2014 --batch_size 100 --use_symlink
```

### 步骤 2: 监控任务进度

```bash
# 查看任务状态
squeue -u $USER

# 查看日志
tail -f outputs/logs/recompute_sentiment_<job_id>.out

# 实时监控所有日志
watch -n 5 'ls -lht outputs/logs/recompute_sentiment_*.out | head -10'
```

### 步骤 3: 验证结果

任务完成后，验证生成的sentiment文件：

```bash
# Dry run - 只检查不复制
python 0.1.8-verify-and-copy-sentiment.py --dry_run

# 验证特定年份
python 0.1.8-verify-and-copy-sentiment.py --year 2014 --dry_run
```

### 步骤 4: 复制到正式目录

验证无误后，将文件复制到正式的sentiment目录：

```bash
# 复制所有验证通过的文件
python 0.1.8-verify-and-copy-sentiment.py --copy

# 如果需要覆盖已存在的文件
python 0.1.8-verify-and-copy-sentiment.py --copy --overwrite
```

### 步骤 5: 最终验证

```bash
# 重新运行检测脚本，确认所有文件都已补齐
python 0.1.5-find-missing-sentiment-files.py

# 查看新的统计报告
cat outputs/missing_sentiment_summary.txt
```

## 文件说明 (File Descriptions)

| 文件 | 用途 |
|------|------|
| `0.1.5-find-missing-sentiment-files.py` | 检测缺失的sentiment文件 |
| `0.1.6-recompute-missing-sentiment.py` | 重新计算sentiment的主脚本 |
| `0.1.6-recompute-sentiment-slurm.sh` | SLURM提交脚本（GPU） |
| `0.1.7-submit-recompute-sentiment-jobs.sh` | 批量提交多个年份的脚本 |
| `0.1.8-verify-and-copy-sentiment.py` | 验证和复制结果的脚本 |

## 配置说明 (Configuration)

所有路径在 `setting.json` 中配置：

```json
{
  "geo_tweets_archive_base_path": "/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg-tweets",
  "sentiment_file_base_path": "/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg-sentiment",
  "sentiment_computing_path": "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path",
  "geotweet_sentiment_geography_repo": "/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography"
}
```

- `sentiment_computing_path`: 临时计算目录（你可以在验证后再决定是否复制）
- `geotweet_sentiment_geography_repo`: BERT模型和计算代码的位置

## 资源需求 (Resource Requirements)

### GPU任务 (0.1.6 recompute script)
- **Partition**: `gpu`
- **GPU**: 1x A100 或 V100
- **CPUs**: 8 cores
- **Memory**: 64GB
- **Time**: 12 hours per year
- **建议**: 使用 `--use_symlink` 节省磁盘空间

### 预估处理时间

基于文件数量：
- **2014年** (3,986 files): ~8-10 hours
- **2017年** (513 files): ~1-2 hours
- **2020年** (1 file): ~1 minute
- **2023年** (510 files): ~1-2 hours

## 磁盘空间估算 (Disk Space)

每个sentiment文件大约是对应tweet文件的 10-20% 大小：

```
2014: ~40-80 GB
2017: ~5-10 GB
2020: ~10 MB
2023: ~5-10 GB
Total: ~50-100 GB
```

临时目录 `sentiment_computing_path` 需要相同的空间。

## 故障排查 (Troubleshooting)

### 问题 1: GPU不可用

**错误信息**: "WARNING: Running on CPU"

**解决方案**:
- 检查是否在GPU节点上: `nvidia-smi`
- 检查CUDA是否加载: `module list`
- 重新提交到GPU分区: `sbatch -p gpu ...`

### 问题 2: 内存不足

**错误信息**: "CUDA out of memory"

**解决方案**:
- 减小batch size: `--batch_size 50`
- 请求更大内存的GPU节点

### 问题 3: 模型加载失败

**错误信息**: "Model file not found"

**解决方案**:
检查模型路径是否正确:
```bash
ls -l /n/holylfs/LABS/cga/data/geo-tweets/geotweet-sentiment-geography/training_model/
```

### 问题 4: 文件权限错误

**解决方案**:
```bash
# 检查目录权限
ls -ld /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path

# 创建目录并设置权限
mkdir -p /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path
chmod 755 /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path
```

## 最佳实践 (Best Practices)

1. **先测试小数据集**
   ```bash
   python 0.1.6-recompute-missing-sentiment.py --year 2020 --batch_size 100
   ```

2. **使用symlink节省空间**
   ```bash
   --use_symlink
   ```

3. **分批处理大年份**
   - 2014年可以拆分成多个批次处理

4. **定期检查日志**
   ```bash
   tail -f outputs/logs/recompute_sentiment_*.out
   ```

5. **验证后再复制**
   - 先用 `--dry_run` 检查
   - 再用 `--copy` 复制

## 完整示例 (Complete Example)

```bash
# 1. 检查缺失文件
python 0.1.5-find-missing-sentiment-files.py

# 2. 提交所有任务
bash 0.1.7-submit-recompute-sentiment-jobs.sh

# 3. 监控进度
squeue -u $USER
tail -f outputs/logs/recompute_sentiment_*.out

# 4. 等待完成后验证
python 0.1.8-verify-and-copy-sentiment.py --dry_run

# 5. 复制到正式目录
python 0.1.8-verify-and-copy-sentiment.py --copy

# 6. 最终确认
python 0.1.5-find-missing-sentiment-files.py
```

## 清理临时文件 (Cleanup)

验证无误并复制完成后，可以删除临时目录：

```bash
# 确认所有文件都已复制
python 0.1.5-find-missing-sentiment-files.py

# 如果没有缺失文件了，可以删除临时目录
rm -rf /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path
```

## 注意事项 (Important Notes)

⚠️ **运行前必读**:
1. 确保 `geo` conda环境已安装所有依赖
2. 检查磁盘空间是否充足
3. GPU任务可能需要排队，请提前规划时间
4. 建议先用2020年（只有1个文件）测试整个流程
5. 使用 `--dry_run` 模式先检查再执行

✓ **完成后检查**:
1. 所有文件都已验证通过
2. 重新运行缺失文件检测，确认0个缺失
3. 随机抽查几个文件确保格式正确
4. 备份原始数据（可选）
