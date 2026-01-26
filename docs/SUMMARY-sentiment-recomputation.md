# Sentiment文件重计算工具包 - 完整总结

## 📋 创建的文件清单

### 核心脚本 (Core Scripts)

| 文件名 | 功能 | 输入 | 输出 |
|--------|------|------|------|
| `0.1.5-find-missing-sentiment-files.py` | 检测缺失的sentiment文件 | Tweet和sentiment目录 | CSV报告 |
| `0.1.6-recompute-missing-sentiment.py` | 重新计算缺失的sentiment | 缺失文件列表 | Sentiment文件 |
| `0.1.6-recompute-sentiment-slurm.sh` | SLURM GPU任务提交脚本 | Year参数 | 计算任务 |
| `0.1.7-submit-recompute-sentiment-jobs.sh` | 批量提交多年份任务 | - | 多个SLURM任务 |
| `0.1.8-verify-and-copy-sentiment.py` | 验证并复制结果 | 计算结果 | 验证报告+复制 |
| `0.1.9-test-sentiment-pipeline.sh` | 测试整个流程 | - | 测试结果 |

### 文档 (Documentation)

| 文件名 | 内容 |
|--------|------|
| `README-recompute-sentiment.md` | 详细使用指南（中英文） |
| `SUMMARY-sentiment-recomputation.md` | 本文件 - 总结文档 |
| `data-pipeline-flowchart.txt` | 更新的流程图 |

### 配置更新 (Configuration Updates)

- **setting.json**: 添加了 `sentiment_computing_path` 配置
- **Snakefile**: 添加了 `find_missing_sentiment` 和 `check_data_quality` 规则

## 🔍 当前数据状态

基于 `0.1.5-find-missing-sentiment-files.py` 的检测结果：

```
总计扫描: 98,584 个tweet文件
Sentiment存在: 93,574 (94.92%)
Sentiment缺失: 5,010 (5.08%)
```

### 按年份细分

| 年份 | 总文件数 | 缺失数 | 覆盖率 | 优先级 |
|------|---------|--------|--------|--------|
| 2014 | 8,753 | 3,986 | 54.46% | 🔴 高 |
| 2017 | 8,760 | 513 | 94.14% | 🟡 中 |
| 2023 | 4,319 | 510 | 88.19% | 🟡 中 |
| 2020 | 8,557 | 1 | 99.99% | 🟢 低 |
| 其他 | - | 0 | 100% | ✅ 完整 |

## 🚀 快速开始指南

### 选项 1: 测试运行（推荐首次使用）

```bash
# 1. 先用2020年测试（只有1个文件）
bash 0.1.9-test-sentiment-pipeline.sh
```

### 选项 2: 批量处理所有年份

```bash
# 1. 检测缺失文件（如果还没运行）
python 0.1.5-find-missing-sentiment-files.py

# 2. 提交所有GPU任务
bash 0.1.7-submit-recompute-sentiment-jobs.sh

# 3. 监控任务
squeue -u $USER

# 4. 验证结果（任务完成后）
python 0.1.8-verify-and-copy-sentiment.py --dry_run

# 5. 复制到正式目录
python 0.1.8-verify-and-copy-sentiment.py --copy

# 6. 最终验证
python 0.1.5-find-missing-sentiment-files.py
```

### 选项 3: 单独处理特定年份

```bash
# 例如只处理2014年
sbatch 0.1.6-recompute-sentiment-slurm.sh 2014

# 等待完成后验证
python 0.1.8-verify-and-copy-sentiment.py --year 2014 --dry_run
python 0.1.8-verify-and-copy-sentiment.py --year 2014 --copy
```

## 📊 预估资源和时间

### 计算资源需求

每个年份的任务：
- **GPU**: 1x A100 或 V100
- **CPU**: 8 cores
- **内存**: 64GB
- **时间**: 最多12小时

### 预估处理时间

| 年份 | 文件数 | 预估时间 | GPU时间成本 |
|------|--------|---------|------------|
| 2014 | 3,986 | 8-10小时 | ~40 GPU-hours |
| 2017 | 513 | 1-2小时 | ~8 GPU-hours |
| 2023 | 510 | 1-2小时 | ~8 GPU-hours |
| 2020 | 1 | 1分钟 | ~0.02 GPU-hours |
| **总计** | **5,010** | **10-14小时** | **~56 GPU-hours** |

如果并行处理所有年份，总墙钟时间约为 **8-10小时**。

### 磁盘空间需求

- **临时目录** (`sentiment_computing_path`): ~50-100 GB
- **最终目录** (`sentiment_file_base_path`): ~50-100 GB
- **总计**: ~100-200 GB

## 🔧 技术细节

### 计算方法

使用与原始sentiment数据相同的BERT模型：
- **模型**: `/n/holylfs/LABS/cga/data/geo-tweets/geotweet-sentiment-geography/training_model/`
- **方法**: `emb.pkl` (BERT embeddings) + `clf.pkl` (classifier)
- **代码库**: `/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography`

### 处理流程

```
Tweet文件 (.csv.gz)
    ↓
BERT Embedding生成
    ↓
Sentiment分类器
    ↓
Sentiment分数 (0-1 范围)
    ↓
输出文件 (bert_sentiment_*.csv.gz)
```

### 输出格式

生成的文件格式与原始sentiment文件一致：
- 文件名: `bert_sentiment_{原tweet文件名}.csv.gz`
- 格式: TSV (tab分隔)
- 列: `message_id`, `score`
- 压缩: gzip

## 📁 目录结构

```
US-Census-TGSI/
├── 0.1.5-find-missing-sentiment-files.py       # 检测脚本
├── 0.1.6-recompute-missing-sentiment.py        # 重计算脚本
├── 0.1.6-recompute-sentiment-slurm.sh          # SLURM提交脚本
├── 0.1.7-submit-recompute-sentiment-jobs.sh    # 批量提交
├── 0.1.8-verify-and-copy-sentiment.py          # 验证复制
├── 0.1.9-test-sentiment-pipeline.sh            # 测试脚本
├── README-recompute-sentiment.md               # 使用指南
├── SUMMARY-sentiment-recomputation.md          # 本文件
├── setting.json                                # 配置文件
├── Snakefile                                   # 更新的Snakefile
├── data-pipeline-flowchart.txt                 # 更新的流程图
└── outputs/
    ├── missing_sentiment_files.csv             # 缺失文件列表
    ├── existing_sentiment_files.csv            # 现有文件列表
    ├── sentiment_files_statistics.csv          # 统计数据
    ├── missing_sentiment_summary.txt           # 摘要报告
    └── logs/
        └── recompute_sentiment_*.out/err       # 任务日志

workspace/sentiment_computing_path/             # 临时计算目录
├── input/
│   ├── 2014/                                   # 输入tweet文件
│   ├── 2017/
│   ├── 2020/
│   └── 2023/
└── output/
    ├── 2014/                                   # 计算结果
    ├── 2017/
    ├── 2020/
    └── 2023/
```

## ⚠️ 重要注意事项

### 运行前检查

✅ 必须完成：
1. [ ] 确认 `geo` conda环境可用
2. [ ] 检查GPU分区配额
3. [ ] 验证磁盘空间充足（100-200GB）
4. [ ] 确认BERT模型文件存在
5. [ ] 运行 `0.1.5-find-missing-sentiment-files.py` 获取缺失文件列表

### 运行时监控

📊 定期检查：
1. GPU利用率: `nvidia-smi`
2. 任务状态: `squeue -u $USER`
3. 日志输出: `tail -f outputs/logs/recompute_sentiment_*.out`
4. 磁盘空间: `df -h /n/netscratch/cga/Lab/xiaokang/`

### 完成后验证

✓ 验证步骤：
1. [ ] 运行验证脚本检查文件有效性
2. [ ] 随机抽查2-3个文件内容
3. [ ] 重新运行缺失文件检测，确认数量减少
4. [ ] 比较新旧文件的统计特征（如果有重叠）

## 🐛 常见问题

### Q1: 如果任务中途失败怎么办？

**A**: 脚本支持断点续算。重新运行相同的命令，它会跳过已完成的文件。

### Q2: 如何优先处理2014年？

**A**:
```bash
sbatch 0.1.6-recompute-sentiment-slurm.sh 2014
```

### Q3: 计算结果存在哪里？

**A**:
- 临时位置: `{sentiment_computing_path}/output/{year}/`
- 验证后复制到: `{sentiment_file_base_path}/{year}/`

### Q4: 如何判断文件是否正确？

**A**: 使用验证脚本：
```bash
python 0.1.8-verify-and-copy-sentiment.py --year 2014 --dry_run
```
会检查：
- 文件格式是否正确
- 是否包含必需列
- sentiment分数是否在合理范围
- 文件行数统计

### Q5: 可以在CPU上运行吗？

**A**: 可以但非常慢（约慢10-100倍）。不建议用于大规模处理。

## 📈 进度追踪

### 2025-11-24 当前状态

- [x] 创建检测脚本 (0.1.5)
- [x] 创建重计算脚本 (0.1.6)
- [x] 创建SLURM提交脚本 (0.1.6-slurm)
- [x] 创建批量提交脚本 (0.1.7)
- [x] 创建验证脚本 (0.1.8)
- [x] 创建测试脚本 (0.1.9)
- [x] 更新配置文件
- [x] 更新流程图
- [x] 编写完整文档
- [ ] 运行测试验证（2020年）
- [ ] 提交所有年份的任务
- [ ] 验证并复制结果
- [ ] 最终确认所有文件完整

## 📞 下一步行动

### 立即可做

1. **测试运行**（5-10分钟）
   ```bash
   bash 0.1.9-test-sentiment-pipeline.sh
   ```

2. **提交批量任务**（如果测试通过）
   ```bash
   bash 0.1.7-submit-recompute-sentiment-jobs.sh
   ```

### 任务完成后

3. **验证结果**
   ```bash
   python 0.1.8-verify-and-copy-sentiment.py --dry_run
   ```

4. **复制到正式目录**
   ```bash
   python 0.1.8-verify-and-copy-sentiment.py --copy
   ```

5. **最终确认**
   ```bash
   python 0.1.5-find-missing-sentiment-files.py
   ```

## 📚 相关文档

- **详细使用指南**: `README-recompute-sentiment.md`
- **原始sentiment代码**: `/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/`
- **项目总览**: `CLAUDE.md`
- **流程图**: `data-pipeline-flowchart.txt`

---

**创建日期**: 2025-11-24
**作者**: Claude Code
**版本**: 1.0
