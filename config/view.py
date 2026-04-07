import uuid
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from django.http import JsonResponse, HttpResponse
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt

BUCKET_NAME = os.getenv('BUCKET_NAME', 'axiom-platform-datasets')


def _gerar_url_local(file_path: str, content_type: str) -> str:
    """
    Em modo DEBUG sem GCS, retorna uma URL local que aponta para o endpoint
    de upload direto no próprio servidor Django.
    O frontend fará PUT para esta URL e o Django salvará o arquivo em MEDIA_ROOT.
    """
    return f"/api/upload-local/?file_path={file_path}&content_type={content_type}"


def gerar_url_upload(request):
    file_name = request.GET.get('file_name')
    content_type = request.GET.get('content_type', 'application/octet-stream')

    nome_unico = f"{uuid.uuid4()}_{file_name}"
    file_path = f"datasets/{nome_unico}"

    # ── Modo local (DEBUG sem credenciais GCS) ───────────────────────────────
    # Ativado quando DEBUG=True E a variável USE_GCS não está setada como "true"
    usar_gcs = os.getenv('USE_GCS', 'false').lower() == 'true' or not settings.DEBUG

    if not usar_gcs:
        url = request.build_absolute_uri(
            f"/api/upload-local/?file_path={file_path}&content_type={content_type}"
        )
        return JsonResponse({'url': url, 'file_path': file_path})

    # ── Modo produção: Google Cloud Storage Signed URL ───────────────────────
    import google.auth
    from google.auth.transport import requests as google_requests
    from google.cloud import storage

    credentials, project_id = google.auth.default()

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_path)

    params = {
        "version": "v4",
        "expiration": timedelta(minutes=15),
        "method": "PUT",
        "content_type": content_type,
    }

    # No Cloud Run sem chave privada local, usa IAM Credentials via access token
    if not hasattr(credentials, 'signer') and hasattr(credentials, 'service_account_email'):
        auth_request = google_requests.Request()
        credentials.refresh(auth_request)
        params["service_account_email"] = credentials.service_account_email
        params["access_token"] = credentials.token

    url_assinada = blob.generate_signed_url(**params)

    return JsonResponse({'url': url_assinada, 'file_path': file_path})


@csrf_exempt
def upload_local(request):
    """
    Endpoint de fallback para uploads locais (apenas DEBUG=True).
    Recebe PUT com o arquivo binário e salva em um diretório temporário,
    simulando o mesmo fluxo do GCS Signed URL para o frontend.
    """
    if not settings.DEBUG:
        return HttpResponse("Not available in production.", status=403)

    if request.method != "PUT":
        return HttpResponse("Method not allowed.", status=405)

    file_path = request.GET.get('file_path', '')
    if not file_path:
        return HttpResponse("Missing file_path.", status=400)

    # Salva em MEDIA_ROOT/datasets/ (cria se não existir)
    destino = Path(settings.BASE_DIR) / "media" / file_path
    destino.parent.mkdir(parents=True, exist_ok=True)

    # Lê em streaming diretamente do wsgi.input para não estourar
    # DATA_UPLOAD_MAX_MEMORY_SIZE — evita carregar o CSV inteiro em memória.
    content_length = int(request.META.get('CONTENT_LENGTH', 0))
    wsgi_input = request.META.get('wsgi.input')

    with open(destino, 'wb') as f:
        restante = content_length
        while restante > 0:
            chunk = wsgi_input.read(min(65536, restante))  # lê em blocos de 64 KB
            if not chunk:
                break
            f.write(chunk)
            restante -= len(chunk)

    return HttpResponse(status=200)