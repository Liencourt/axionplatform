from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0005_alter_empresa_id_alter_usuarioempresa_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='empresa',
            name='numero',
            field=models.CharField(
                blank=True,
                help_text='Número do endereço (ex: 2701). Melhora a precisão do geocoding.',
                max_length=20,
                null=True,
            ),
        ),
    ]
