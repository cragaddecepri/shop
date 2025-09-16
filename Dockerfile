FROM python 3.11-slim
WORKDIR /appp
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirments.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p databases/log databases/orders databases/users databases/data/points databases/data/payment
CMD ["python", "bot.py"]
