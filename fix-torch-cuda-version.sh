#!/bin/bash
# Fix PyTorch CUDA version mismatch

echo "Fixing PyTorch CUDA version..."
echo ""

# Activate environment
conda activate sentiment2022

# Uninstall current pytorch
echo "Removing current PyTorch..."
pip uninstall torch torchvision -y

# Install PyTorch with CUDA 11.3 support
echo ""
echo "Installing PyTorch 1.11.0 with CUDA 11.3 support..."
pip install torch==1.11.0+cu113 torchvision==0.12.0+cu113 --extra-index-url https://download.pytorch.org/whl/cu113

# Verify
echo ""
echo "Verifying installation..."
python -c "import torch; print(f'PyTorch version: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}')"

echo ""
echo "Done! PyTorch is now configured for CUDA 11.3"
