# 创建Sentiment计算专用环境

## 推荐配置

**Python版本**: 3.8 (2022年模型兼容)

## 方法1: 使用conda环境文件（推荐）

```bash
# 1. 创建环境
conda env create -f sentiment-env-2022.yml

# 2. 激活环境
conda activate sentiment2022

# 3. 验证安装
python -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}')"
python -c "import sentence_transformers; print(f'sentence-transformers: {sentence_transformers.__version__}')"

# 4. 测试模型加载
python -c "import torch; model = torch.load('/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/emb.pkl', weights_only=False); print('✓ Model loaded successfully')"
```

## 方法2: 手动安装（如果方法1失败）

```bash
# 1. 创建新环境
conda create -n sentiment2022 python=3.8 -y

# 2. 激活环境
conda activate sentiment2022

# 3. 安装PyTorch (CUDA 11.3版本)
pip install torch==1.11.0+cu113 torchvision==0.12.0+cu113 --extra-index-url https://download.pytorch.org/whl/cu113

# 4. 安装其他依赖
pip install sentence-transformers==2.2.0
pip install transformers==4.19.0
pip install huggingface-hub==0.5.1
pip install scikit-learn==1.0.2
pip install pandas==1.4.2
pip install numpy==1.22.3
pip install emoji==1.7.0
pip install tqdm nltk

# 5. 验证安装
python -c "import torch; print(f'PyTorch: {torch.__version__}')"
python -c "import sentence_transformers; print(f'sentence-transformers: {sentence_transformers.__version__}')"
```

## 验证环境是否正确

创建测试脚本：

```python
# test_sentiment_env.py
import torch
import sentence_transformers
from transformers import AutoModel

print("=" * 60)
print("Testing Sentiment Environment")
print("=" * 60)

print(f"\n✓ Python version: {__import__('sys').version}")
print(f"✓ PyTorch version: {torch.__version__}")
print(f"✓ CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"✓ CUDA version: {torch.version.cuda}")
print(f"✓ sentence-transformers: {sentence_transformers.__version__}")

# Test model loading
try:
    print("\nTesting model loading...")
    emb_model = torch.load(
        '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/emb.pkl',
        weights_only=False
    )
    clf_model = torch.load(
        '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/clf.pkl',
        weights_only=False
    )
    print("✓ BERT embedding model loaded successfully")
    print("✓ Classifier model loaded successfully")
    print(f"  Embedding model type: {type(emb_model)}")
    print(f"  Classifier model type: {type(clf_model)}")
    print("\n✅ All tests passed! Environment is ready.")
except Exception as e:
    print(f"✗ Error loading models: {e}")
    import traceback
    traceback.print_exc()
```

运行测试：
```bash
conda activate sentiment2022
python test_sentiment_env.py
```

## 更新SLURM脚本使用新环境

修改 `0.1.9-test-sentiment-gpu.sh`，将以下行：

```bash
conda activate geo
```

改为：

```bash
conda activate sentiment2022
```

## 常见问题

### Q: CUDA版本不匹配？

如果遇到CUDA版本问题，可以尝试：
```bash
pip install torch==1.11.0+cu115 torchvision==0.12.0+cu115 --extra-index-url https://download.pytorch.org/whl/cu115
```

### Q: 包冲突？

如果遇到包冲突，删除环境重新创建：
```bash
conda env remove -n sentiment2022
conda env create -f sentiment-env-2022.yml
```

### Q: 模型加载失败？

确保使用 `weights_only=False`：
```python
torch.load(model_path, weights_only=False)
```

## 下一步

环境创建成功后：

1. 运行GPU测试：
   ```bash
   sbatch 0.1.9-test-sentiment-gpu.sh
   ```

2. 如果测试成功，批量处理所有年份：
   ```bash
   bash 0.1.7-submit-recompute-sentiment-jobs.sh
   ```

## 环境清理

如果不再需要，可以删除环境：
```bash
conda env remove -n sentiment2022
```
