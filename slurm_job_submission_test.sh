#!/bin/bash

# echo "Submitting SLURM job"

#SBATCH --job-name=hello_world
#SBATCH --output=hello_%j.out
#SBATCH --ntasks=1
#SBATCH --time=00:01:00

echo "Hello World from $(hostname) at $(date)"
