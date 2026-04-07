"""Migration 0015 — Axiom Reputation: ReputacaoConfig + AnaliseReputacao"""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0004_empresa_geo_fields'),
        ('projects', '0014_trend_radar'),
    ]

    operations = [
        migrations.CreateModel(
            name='ReputacaoConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('nome_busca', models.CharField(max_length=255, help_text='Nome digitado pelo usuário na busca')),
                ('google_place_id', models.CharField(max_length=255, unique=True)),
                ('google_place_nome', models.CharField(max_length=255)),
                ('google_place_endereco', models.CharField(max_length=500, blank=True)),
                ('google_place_url', models.URLField(blank=True)),
                ('google_place_foto', models.URLField(blank=True)),
                ('configurado_em', models.DateTimeField(auto_now_add=True)),
                ('atualizado_em', models.DateTimeField(auto_now=True)),
                ('empresa', models.OneToOneField(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='reputacao_config',
                    to='accounts.empresa',
                )),
            ],
            options={
                'verbose_name': 'Configuração de Reputação',
                'verbose_name_plural': 'Configurações de Reputação',
            },
        ),
        migrations.CreateModel(
            name='AnaliseReputacao',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('criado_em', models.DateTimeField(auto_now_add=True)),
                ('status', models.CharField(
                    choices=[('concluido', 'Concluído'), ('sem_reviews', 'Sem avaliações textuais'), ('erro', 'Erro')],
                    default='concluido', max_length=20,
                )),
                ('rating_geral', models.FloatField(null=True, blank=True)),
                ('total_avaliacoes', models.IntegerField(default=0)),
                ('sentimento_geral', models.CharField(
                    choices=[('positivo', 'Positivo'), ('negativo', 'Negativo'), ('neutro', 'Neutro')],
                    max_length=20, blank=True,
                )),
                ('score_sentimento', models.IntegerField(default=0)),
                ('temas_positivos', models.JSONField(default=list)),
                ('temas_negativos', models.JSONField(default=list)),
                ('resumo_executivo', models.TextField(blank=True)),
                ('reviews', models.JSONField(default=list)),
                ('tokens_input', models.IntegerField(default=0)),
                ('tokens_output', models.IntegerField(default=0)),
                ('empresa', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='analises_reputacao',
                    to='accounts.empresa',
                )),
                ('config', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='analises',
                    to='projects.reputacaoconfig',
                )),
            ],
            options={
                'verbose_name': 'Análise de Reputação',
                'verbose_name_plural': 'Análises de Reputação',
                'ordering': ['-criado_em'],
            },
        ),
        migrations.AddIndex(
            model_name='analisereputacao',
            index=models.Index(fields=['empresa', '-criado_em'], name='projects_ar_empresa_dt_idx'),
        ),
    ]
