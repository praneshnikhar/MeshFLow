FROM texlive/texlive:latest

WORKDIR /paper
RUN tlmgr install acmart booktabs listings 2>/dev/null; exit 0

ENTRYPOINT ["pdflatex", "-interaction=nonstopmode", "paper.tex"]
