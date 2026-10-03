#!/bin/bash
# Build the MeshFlow paper PDF
# Requires: docker (for first build), or pdflatex locally

set -e

PAPER_DIR="$(cd "$(dirname "$0")" && pwd)"

if command -v pdflatex &> /dev/null; then
    echo "Using local pdflatex..."
    cd "$PAPER_DIR"
    pdflatex -interaction=nonstopmode paper.tex
    bibtex paper
    pdflatex -interaction=nonstopmode paper.tex
    pdflatex -interaction=nonstopmode paper.tex
    echo "Done: paper/paper.pdf"
elif command -v docker &> /dev/null; then
    echo "Building via Docker..."
    docker run --rm -v "$PAPER_DIR":/paper -w /paper \
        texlive/texlive:latest \
        sh -c '
            pdflatex -interaction=nonstopmode paper.tex && \
            bibtex paper && \
            pdflatex -interaction=nonstopmode paper.tex && \
            pdflatex -interaction=nonstopmode paper.tex
        '
    echo "Done: paper/paper.pdf"
else
    echo "Neither pdflatex nor docker found."
    echo "Upload the paper/ directory to Overleaf: https://www.overleaf.com"
    exit 1
fi
