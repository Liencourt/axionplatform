"""
Migration: adiciona campos de geolocalização à Empresa
Campos: cep, municipio, uf, codigo_ibge, lat, lon
Usados pelo módulo de Data Enrichment para resolver coordenadas automaticamente.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("accounts", "0003_empresa_api_key"),
    ]

    operations = [
        migrations.AddField(
            model_name="empresa",
            name="cep",
            field=models.CharField(
                blank=True,
                help_text="CEP da sede principal da empresa (ex: 01310-100). Usado para enriquecer dados com clima e IBGE.",
                max_length=9,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="empresa",
            name="municipio",
            field=models.CharField(
                blank=True,
                help_text="Preenchido automaticamente via ViaCEP",
                max_length=150,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="empresa",
            name="uf",
            field=models.CharField(blank=True, max_length=2, null=True),
        ),
        migrations.AddField(
            model_name="empresa",
            name="codigo_ibge",
            field=models.CharField(
                blank=True,
                help_text="Código IBGE do município",
                max_length=10,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="empresa",
            name="lat",
            field=models.FloatField(
                blank=True,
                help_text="Latitude (geocoding automático)",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="empresa",
            name="lon",
            field=models.FloatField(
                blank=True,
                help_text="Longitude (geocoding automático)",
                null=True,
            ),
        ),
    ]
