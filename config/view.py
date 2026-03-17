from google.cloud import storage
from datetime import timedelta
from django.http import JsonResponse
import os
import uuid

def gerar_url_upload(request):
    # Pega o nome e o tipo do arquivo que o JavaScript vai mandar
    file_name = request.GET.get('file_name')
    content_type = request.GET.get('content_type') # Ex: text/csv

    # Gera um nome único para o arquivo para evitar que um sobrescreva o outro
    nome_unico = f"{uuid.uuid4()}_{file_name}"
    
    # O nome do bucket que você acabou de criar no GCP
    bucket_name = 'axiom-platform-datasets' 

    # Conecta no Storage do Google
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"datasets/{nome_unico}")

    # Gera a URL com o passe livre válido por 15 minutos
    url_assinada = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type=content_type,
    )

    # Devolve a URL mágica e o nome do arquivo salvo para o frontend
    return JsonResponse({
        'url': url_assinada, 
        'file_path': f"datasets/{nome_unico}"
    })