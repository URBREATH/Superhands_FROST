# Usa un'immagine Python 3.10 slim come base
FROM python:3.10-slim-bullseye

# Imposta la timezone per Tallinn (Estonia)
ENV TZ=Europe/Tallinn

# Imposta la directory di lavoro all'interno del container
WORKDIR /app

# Copia tutto il contenuto della directory corrente (dove si trova il Dockerfile)
# nella directory /app del container.
COPY . /app

# Aggiorna apt e installa gcc, libc-dev e tzdata per la gestione dei fusi orari.
RUN apt-get update && \
    apt-get install --no-install-recommends --assume-yes gcc libc-dev tzdata && \
    # Configura il timezone a livello di sistema
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    # Pulisci la cache di apt per ridurre le dimensioni dell'immagine
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installa le librerie Python necessarie
RUN pip install --no-cache-dir flask requests boto3

# Comando da eseguire all'avvio del container
CMD ["python", "superhands_frost.py"]