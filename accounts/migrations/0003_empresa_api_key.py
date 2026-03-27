import uuid
from django.db import migrations, models


def populate_api_keys(apps, schema_editor):
    """Gera um UUID único para cada empresa já existente no banco."""
    Empresa = apps.get_model('accounts', 'Empresa')
    for empresa in Empresa.objects.all():
        empresa.api_key = uuid.uuid4()
        empresa.save(update_fields=['api_key'])


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0002_empresa_is_active_subscriber_and_more'),
    ]

    operations = [
        # Passo 1: Adiciona o campo sem a constraint UNIQUE (nullable)
        migrations.AddField(
            model_name='empresa',
            name='api_key',
            field=models.UUIDField(null=True, blank=True, editable=False),
        ),
        # Passo 2: Popula UUIDs únicos para todos os registros existentes
        migrations.RunPython(populate_api_keys, migrations.RunPython.noop),
        # Passo 3: Aplica UNIQUE + default para novos registros futuros
        migrations.AlterField(
            model_name='empresa',
            name='api_key',
            field=models.UUIDField(
                default=uuid.uuid4,
                editable=False,
                unique=True,
                help_text='Chave de autenticação para integração via API REST (B2B). Enviar no header X-Axiom-API-Key.'
            ),
        ),
    ]
