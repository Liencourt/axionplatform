import uuid
import google.auth
from google.auth.transport import requests
from google.cloud import storage
from datetime import timedelta
from django.http import JsonResponse
import os



BUCKET_NAME = os.getenv('BUCKET_NAME', 'axiom-platform-datasets')




def gerar_url_upload(request):
    file_name = request.GET.get('file_name')
    content_type = request.GET.get('content_type')

    nome_unico = f"{uuid.uuid4()}_{file_name}"
    bucket_name = BUCKET_NAME

    # 1. Pega as credenciais (seja local ou na nuvem)
    credentials, project_id = google.auth.default()

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"datasets/{nome_unico}")

    # 2. Preparamos os parâmetros padrão (que funcionam localmente)
    params = {
        "version": "v4",
        "expiration": timedelta(minutes=15),
        "method": "PUT",
        "content_type": content_type,
    }

    # 3. A Inteligência: O código pergunta "Eu tenho uma chave privada (signer)?"
    # Se NÃO tiver (estamos no Cloud Run), ele pega o Token e adiciona nos parâmetros
    if not hasattr(credentials, 'signer') and hasattr(credentials, 'service_account_email'):
        auth_request = requests.Request()
        credentials.refresh(auth_request)
        params["service_account_email"] = credentials.service_account_email
        params["access_token"] = credentials.token

    # 4. Gera a URL desempacotando os parâmetros certos para cada ambiente
    url_assinada = blob.generate_signed_url(**params)

    return JsonResponse({
        'url': url_assinada, 
        'file_path': f"datasets/{nome_unico}"
    })