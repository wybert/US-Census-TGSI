#!/bin/bash
#SBATCH -J merge_tweets_sentiment
#SBATCH -c 110
#SBATCH -t 00-12:00
#SBATCH -p sapphire
#SBATCH --mem=100000
#SBATCH -o /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/combine_tweets_sentiment_%j.out
#SBATCH -e /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/combine_tweets_sentiment_%j.err

cd /n/home11/xiaokangfu/xiaokang/US-Census-TGSI
/n/home11/xiaokangfu/.conda/envs/geo/bin/python src/02_merging/0.2.1-combine-geo-tweets-archive-and-sentiment.py
