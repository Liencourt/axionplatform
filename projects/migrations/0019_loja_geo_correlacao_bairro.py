from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('projects', '0018_tendencia_reputacao_projeto_fk'),
    ]

    operations = [
        # ── Campos geográficos na Loja ───────────────────────────────────────
        migrations.AddField(
            model_name='loja',
            name='cep',
            field=models.CharField(blank=True, max_length=9, null=True),
        ),
        migrations.AddField(
            model_name='loja',
            name='numero',
            field=models.CharField(
                blank=True,
                help_text='Número do endereço da filial (ex: 2701). Melhora a precisão do geocoding.',
                max_length=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='loja',
            name='bairro',
            field=models.CharField(
                blank=True,
                help_text='Bairro conforme ViaCEP / cadastro manual',
                max_length=150,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='loja',
            name='lat',
            field=models.FloatField(
                blank=True,
                help_text='Latitude (geocoding automático)',
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='loja',
            name='lon',
            field=models.FloatField(
                blank=True,
                help_text='Longitude (geocoding automático)',
                null=True,
            ),
        ),
        migrations.AddField(
            model_name='loja',
            name='codigo_ibge',
            field=models.CharField(
                blank=True,
                help_text='Código IBGE do município da filial (sobrepõe o da Empresa quando preenchido)',
                max_length=10,
                null=True,
            ),
        ),
        # ── Campos de granularidade geográfica em CorrelacaoAnalise ─────────
        migrations.AddField(
            model_name='correlacaoanalise',
            name='ibge_bairro',
            field=models.CharField(blank=True, max_length=150, null=True),
        ),
        migrations.AddField(
            model_name='correlacaoanalise',
            name='ibge_setor_codigo',
            field=models.CharField(blank=True, max_length=15, null=True),
        ),
        migrations.AddField(
            model_name='correlacaoanalise',
            name='ibge_nivel_geo',
            field=models.CharField(
                blank=True,
                help_text="'setor', 'municipio' ou 'nenhum' — indica a precisão do enriquecimento IBGE",
                max_length=20,
                null=True,
            ),
        ),
    ]
