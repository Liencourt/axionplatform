FROM python:3.12-slim

# Evita que o Python grave arquivos .pyc e força a exibição dos logs no terminal
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


# Define a pasta de trabalho dentro do container
WORKDIR /app

# Instala as dependências do sistema necessárias para o banco de dados (PostgreSQL)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de bibliotecas e instala
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Garante que o servidor de produção do Python (Gunicorn) seja instalado
RUN pip install gunicorn

# Copia todo o resto do código da Axiom Platform para dentro do container
COPY . /app/

# Expõe a porta padrão que o Cloud Run utiliza
EXPOSE 8080

CMD python manage.py migrate && gunicorn --bind 0.0.0.0:8080 config.wsgi:application



