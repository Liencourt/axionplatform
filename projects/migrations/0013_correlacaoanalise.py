import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0004_empresa_geo_fields'),
        ('projects', '0012_resultadoprecificacao_elasticidade_ic_lower_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='CorrelacaoAnalise',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('criado_em', models.DateTimeField(auto_now_add=True)),
                ('status', models.CharField(
                    choices=[
                        ('concluido', 'Concluído'),
                        ('sem_dados', 'Sem dados suficientes'),
                        ('sem_localizacao', 'Empresa sem localização cadastrada'),
                        ('erro', 'Erro durante análise'),
                    ],
                    default='concluido',
                    max_length=20,
                )),
                ('correlacoes', models.JSONField(default=list)),
                ('insights', models.JSONField(default=list)),
                ('resumo_executivo', models.TextField(blank=True)),
                ('n_registros', models.IntegerField(default=0)),
                ('estacao_codigo', models.CharField(blank=True, max_length=20, null=True)),
                ('estacao_nome', models.CharField(blank=True, max_length=100, null=True)),
                ('distancia_estacao_km', models.FloatField(blank=True, null=True)),
                ('ibge_municipio', models.CharField(blank=True, max_length=150, null=True)),
                ('ibge_classe', models.CharField(blank=True, max_length=2, null=True)),
                ('empresa', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='correlacoes',
                    to='accounts.empresa',
                )),
                ('projeto', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='correlacoes',
                    to='projects.projetoprecificacao',
                )),
            ],
            options={
                'verbose_name': 'Análise de Correlações',
                'verbose_name_plural': 'Análises de Correlações',
                'ordering': ['-criado_em'],
            },
        ),
    ]
