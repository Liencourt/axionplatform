"""
Migration 0014 — Axiom Trend Radar
Cria RadarConfig e TendenciaDetectada.
"""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0004_empresa_geo_fields'),
        ('projects', '0013_correlacaoanalise'),
    ]

    operations = [
        migrations.CreateModel(
            name='RadarConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('palavras_chave', models.JSONField(default=list, help_text="Lista de keywords customizados para monitorar")),
                ('usar_catalogo_automatico', models.BooleanField(default=True)),
                ('fontes_ativas', models.JSONField(default=list, help_text="Fontes ativas: google_trends, newsapi, rss, reddit")),
                ('limiar_aceleracao', models.FloatField(default=50.0, help_text="Aceleração mínima (%) para registrar uma tendência")),
                ('newsapi_key', models.CharField(blank=True, max_length=100, null=True)),
                ('reddit_client_id', models.CharField(blank=True, max_length=100, null=True)),
                ('reddit_client_secret', models.CharField(blank=True, max_length=200, null=True)),
                ('ativo', models.BooleanField(default=True)),
                ('atualizado_em', models.DateTimeField(auto_now=True)),
                ('empresa', models.OneToOneField(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='radar_config',
                    to='accounts.empresa',
                )),
            ],
            options={
                'verbose_name': 'Configuração do Radar',
                'verbose_name_plural': 'Configurações do Radar',
            },
        ),
        migrations.CreateModel(
            name='TendenciaDetectada',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('palavra_chave', models.CharField(max_length=200)),
                ('nivel', models.CharField(
                    choices=[('baixo', 'Baixo'), ('moderado', 'Moderado'), ('alto', 'Alto'), ('viral', 'Viral')],
                    max_length=20,
                )),
                ('aceleracao_pct', models.FloatField(help_text="Aceleração composta (%) das menções")),
                ('mencoes_recentes', models.FloatField(default=0)),
                ('mencoes_baseline', models.FloatField(default=0)),
                ('aceleracao_por_fonte', models.JSONField(default=dict)),
                ('classificacao', models.CharField(
                    choices=[('positivo', 'Positivo'), ('negativo', 'Negativo'), ('neutro', 'Neutro')],
                    max_length=20,
                )),
                ('confianca', models.FloatField(default=0.5)),
                ('fontes', models.JSONField(default=list)),
                ('skus_relacionados', models.JSONField(default=list)),
                ('recomendacao', models.JSONField(default=dict)),
                ('visualizado', models.BooleanField(default=False)),
                ('arquivado', models.BooleanField(default=False)),
                ('criado_em', models.DateTimeField(auto_now_add=True)),
                ('empresa', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='tendencias',
                    to='accounts.empresa',
                )),
            ],
            options={
                'verbose_name': 'Tendência Detectada',
                'verbose_name_plural': 'Tendências Detectadas',
                'ordering': ['-criado_em', '-aceleracao_pct'],
            },
        ),
        migrations.AddIndex(
            model_name='tendenciadetectada',
            index=models.Index(fields=['empresa', 'arquivado', '-criado_em'], name='projects_te_empresa_arq_idx'),
        ),
    ]
