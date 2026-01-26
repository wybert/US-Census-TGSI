#!/bin/bash
# Install PyTorch and dependencies in geo conda environment

echo "=========================================="
echo "Installing PyTorch in geo environment"
echo "=========================================="

# Activate geo environment
source ~/.bashrc
conda activate geo

echo "Current environment: $CONDA_DEFAULT_ENV"
echo ""

# Install PyTorch with CUDA support
echo "Installing PyTorch with CUDA 11.8 support..."
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install sentence-transformers (required for BERT sentiment)
echo ""
echo "Installing sentence-transformers..."
pip install sentence-transformers

# Install other dependencies from the README
echo ""
echo "Installing other dependencies..."
pip install emoji

echo ""
echo "=========================================="
echo "Installation complete!"
echo "=========================================="

# Verify installation
echo ""
echo "Verifying PyTorch installation..."
python -c "import torch; print(f'PyTorch version: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}'); print(f'CUDA version: {torch.version.cuda}' if torch.cuda.is_available() else 'No CUDA detected (this is normal on login node)')"

echo ""
echo "Verifying sentence-transformers..."
python -c "import sentence_transformers; print(f'sentence-transformers version: {sentence_transformers.__version__}')"

echo ""
echo "✓ All packages installed successfully!"
echo ""
echo "You can now run the GPU test again:"
echo "  sbatch 0.1.9-test-sentiment-gpu.sh"
