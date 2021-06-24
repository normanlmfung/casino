FROM normanfung/anaconda3:1.0.0
WORKDIR /
COPY . .
RUN pip install -r requirements.txt
ENV PATH /opt/conda/bin:$PATH
ENV PYTHONPATH "${PYTHONPATH}:/src"
ENTRYPOINT ["python", "/src/strategies/hsi_hedge.py"]