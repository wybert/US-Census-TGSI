# Missing Sentiment Files Check

## 概述 (Overview)

这个脚本用于在执行推文-情感合并之前，检查所有缺失的BERT情感分析文件。

This script checks for missing BERT sentiment files before running the tweet-sentiment merge process.

## 使用方法 (Usage)

### 方法1: 直接运行 Python 脚本

```bash
# 激活 geo conda 环境
conda activate geo

# 运行脚本
python 0.1.5-find-missing-sentiment-files.py
```

### 方法2: 使用 Snakemake

```bash
# 仅检查数据质量（不运行其他步骤）
snakemake check_data_quality -j 1

# 或者作为完整流程的一部分
snakemake -j 1
```

## 输出文件 (Output Files)

脚本会在 `outputs/` 目录下生成以下文件：

1. **missing_sentiment_files.csv**
   - 所有缺失情感文件的详细列表
   - 包含：年份、推文文件名、期望的情感文件名、文件路径、缺失原因

2. **existing_sentiment_files.csv**
   - 所有存在的情感文件列表（用于参考）
   - 包含：年份、推文文件名、情感文件名、文件路径

3. **sentiment_files_statistics.csv**
   - 按年份统计的摘要数据
   - 包含：每年的总文件数、存在的文件数、缺失的文件数、覆盖率

4. **missing_sentiment_summary.txt**
   - 人类可读的摘要报告
   - 包含：总体统计、年度细分、缺失原因分析、前10个缺失文件示例

## 输出示例 (Example Output)

```
================================================================================
SUMMARY
================================================================================
Total tweet files scanned: 93,574
Sentiment files exist: 85,432 (91.30%)
Sentiment files missing: 8,142 (8.70%)

⚠️  WARNING: Missing sentiment files detected!
   Please review: /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/missing_sentiment_files.csv
```

## 在流程中的位置 (Pipeline Position)

```
0.1 下载 Census 数据
  ↓
0.1.5 检查缺失的情感文件 ← 你在这里
  ↓
0.2 合并推文和情感数据
  ↓
0.3 空间连接
  ↓
...
```

## 注意事项 (Notes)

- 此脚本是**只读**的，不会修改任何文件
- 运行时间：通常 < 30分钟（取决于文件系统性能）
- 内存需求：~4GB
- 推荐在数据合并前运行，以识别数据完整性问题

## 数据路径配置 (Data Paths)

所有路径在 `setting.json` 中配置：
- `geo_tweets_archive_base_path`: 原始推文目录
- `sentiment_file_base_path`: BERT情感分析文件目录
- `outputs_dir`: 输出报告目录

## 文件命名规则 (File Naming Convention)

- 推文文件: `{year}/{filename}.csv.gz`
- 情感文件: `{year}/bert_sentiment_{filename}.csv.gz`

例如：
- 推文: `2020/2020_01_01_00.csv.gz`
- 对应情感: `2020/bert_sentiment_2020_01_01_00.csv.gz`

## 故障排查 (Troubleshooting)

**问题**: 脚本报告大量缺失文件

**可能原因**:
1. 情感分析尚未完成所有文件
2. 文件路径配置错误（检查 `setting.json`）
3. 文件系统挂载问题

**解决方案**:
1. 检查 `missing_sentiment_summary.txt` 中的 "MISSING FILES BY REASON"
2. 验证 `setting.json` 中的路径是否正确
3. 手动检查几个缺失文件是否真的不存在

## 与其他脚本的关系 (Related Scripts)

- **0.2.1-combine-geo-tweets-archive-and-sentiment.py**: 使用此验证结果来合并数据
- **Snakefile**: 自动在合并之前运行此验证

## 更新日志 (Changelog)

- 2025-11-24: 初始版本创建
