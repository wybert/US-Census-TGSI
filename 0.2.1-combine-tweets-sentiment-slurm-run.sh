#!/bin/bash
#SBATCH -c 110                # Number of cores
#SBATCH -t 00-12:00          # Runtime in D-HH:MM, minimum of 10 minutes
#SBATCH -p sapphire           # Partition to submit to
#SBATCH --mem=100000          # Memory pool for all cores (see also --mem-per-cpu)
#SBATCH -o outputs/logs/combine_tweets_sentiment_%j.out    # File to which STDOUT will be written, %j inserts jobid
#SBATCH -e outputs/logs/combine_tweets_sentiment_%j.err     # File to which STDERR will be written, %j inserts jobid


/n/home11/xiaokangfu/.conda/envs/geo/bin/python '/n/home11/xiaokangfu/xiaokang/US-Census-TGSI/0.2.1-combine-geo-tweets-archive-and-sentiment.py'
