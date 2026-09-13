# Databricks notebook source
# ═══════════════════════════════════════════════════════════
# LIMPAR TUDO E RECOMEÇAR DO ZERO
# ═══════════════════════════════════════════════════════════

print("🧹 INICIANDO LIMPEZA COMPLETA...\n")

# 1. Remover caminhos antigos do Delta Lake
caminhos_para_limpar = [
    "/tmp/internacoes_delta",
    "/tmp/internacoes_delta_v2",
    "/tmp/internacoes_delta_novo"
]

for caminho in caminhos_para_limpar:
    try:
        dbutils.fs.rm(caminho, recurse=True)
        print(f"✅ Removido: {caminho}")
    except:
        print(f"ℹ️ Não existia: {caminho}")

# 2. Remover tabela antiga
try:
    spark.sql("DROP TABLE IF EXISTS internacoes_hospitalares")
    print("✅ Tabela antiga removida")
except:
    print("ℹ️ Tabela não existia")

print("\n🎉 LIMPEZA CONCLUÍDA! Criando tudo do zero...\n")

# ═══════════════════════════════════════════════════════════
# CRIAR TABELA BASE DO ZERO
# ═══════════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id_paciente", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("diagnostico", StringType(), True),
    StructField("dias_internacao", IntegerType(), True),
    StructField("data_entrada", StringType(), True),
    StructField("custo", DoubleType(), True)
])

dados = [
    (1, "Maria Silva", "Rio de Janeiro", 72, "Pneumonia", 5, "2026-01-10", 3500.00),
    (2, "João Santos", "São Paulo", 45, "Fratura", 3, "2026-01-12", 2800.00),
    (3, "Ana Costa", "Rio de Janeiro", 68, "Diabetes", 7, "2026-01-15", 4200.00),
    (4, "Carlos Lima", "Belo Horizonte", 30, "Apendicite", 2, "2026-01-18", 2200.00),
    (5, "Paula Souza", "Rio de Janeiro", 80, "AVC", 10, "2026-01-20", 8500)]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# 1. Definir a estrutura
schema = StructType([
    StructField("id_paciente", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("diagnostico", StringType(), True),
    StructField("dias_internacao", IntegerType(), True),
    StructField("data_entrada", StringType(), True),
    StructField("custo", DoubleType(), True)
])

# 2. Criar os dados (5 registros para ser rápido e garantir que funcione)
dados = [
    (1, "Maria Silva", "Rio de Janeiro", 72, "Pneumonia", 5, "2026-01-10", 3500.00),
    (2, "Joao Santos", "Sao Paulo", 45, "Fratura", 3, "2026-01-12", 2800.00),
    (3, "Ana Costa", "Rio de Janeiro", 68, "Diabetes", 7, "2026-01-15", 4200.00),
    (4, "Carlos Lima", "Belo Horizonte", 30, "Apendicite", 2, "2026-01-18", 2200.00),
    (5, "Paula Souza", "Rio de Janeiro", 80, "AVC", 10, "2026-01-20", 8500.00)
]

# 3. Criar o DataFrame e salvar como tabela
df = spark.createDataFrame(dados, schema)
df.write.mode("overwrite").saveAsTable("internacoes_hospitalares")

print("Tabela criada com sucesso!")
print("Total de registros:", df.count())
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, when

# 1. Ler a tabela que criamos
df = spark.table("internacoes_hospitalares")

# 2. Criar a coluna de faixa etária
df_transformado = df.withColumn(
    "faixa_etaria",
    when(col("idade") < 18, "Menor de 18")
    .when((col("idade") >= 18) & (col("idade") < 60), "Adulto (18-59)")
    .otherwise("Idoso (60+)")
)

# 3. Salvar como TABELA DELTA GERENCIADA (não precisa de caminho!)
df_transformado.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("internacoes_delta")

print("Dados salvos como tabela Delta com sucesso!")

# 4. Validar lendo de volta
df_delta = spark.table("internacoes_delta")
print("Registros na tabela Delta:", df_delta.count())
display(df_delta)

# COMMAND ----------

# Criar uma view temporária para usar SQL
df_delta.createOrReplaceTempView("internacoes")

query_faixa = """
SELECT 
    faixa_etaria,
    COUNT(*) as total_internacoes,
    ROUND(AVG(custo), 2) as custo_medio,
    ROUND(AVG(dias_internacao), 1) as dias_medios,
    SUM(custo) as custo_total
FROM internacoes
GROUP BY faixa_etaria
ORDER BY custo_medio DESC
"""
display(spark.sql(query_faixa))

# COMMAND ----------

query_diag = """
SELECT 
    diagnostico,
    COUNT(*) as ocorrencias,
    ROUND(AVG(custo), 2) as custo_medio,
    MAX(custo) as custo_maximo
FROM internacoes
GROUP BY diagnostico
ORDER BY custo_medio DESC
"""
display(spark.sql(query_diag))

# COMMAND ----------

query_mensal = """
SELECT 
    DATE_FORMAT(data_entrada, 'yyyy-MM') as mes,
    COUNT(*) as total_internacoes,
    SUM(custo) as receita_total
FROM internacoes
GROUP BY DATE_FORMAT(data_entrada, 'yyyy-MM')
ORDER BY mes
"""
display(spark.sql(query_mensal))

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════╗
║   🔒 BOAS PRÁTICAS DE SEGURANÇA E GOVERNANÇA APLICADAS     ║
╚══════════════════════════════════════════════════════════════╝

1. DELTA LAKE (Integridade dos Dados):
   ✓ Transações ACID garantem que os dados de saúde não sejam corrompidos.
   ✓ Versionamento automático permite rollback em caso de erro no pipeline.

2. ARQUITETURA MEDALLION (Bronze/Silver/Gold):
   🥉 Bronze: Dados brutos (tabela 'internacoes_hospitalares')
   🥈 Silver: Dados transformados e limpos (tabela 'internacoes_delta')
   🥇 Gold: Camada pronta para dashboards e análise de negócio.

3. PRINCÍPIO DO MENOR PRIVILÉGIO (Conceito):
   → Em produção, o Unity Catalog seria usado para mascarar a coluna 'nome' 
     e 'diagnostico' para analistas externos, cumprindo a LGPD.

4. SEGURANÇA EM NUVEM:
   → Nenhuma credencial ou senha está hardcoded neste notebook.
   → Em produção, conexões usariam AWS Secrets Manager ou Azure Key Vault.
""")

# COMMAND ----------

import matplotlib.pyplot as plt

# Query com cálculo da faixa etária
query = """
SELECT 
    CASE 
        WHEN idade < 18 THEN 'Menor 18'
        WHEN idade BETWEEN 18 AND 59 THEN 'Adulto'
        ELSE 'Idoso 60+'
    END AS faixa_etaria,
    ROUND(AVG(custo), 2) as custo_medio
FROM internacoes_hospitalares
GROUP BY 
    CASE 
        WHEN idade < 18 THEN 'Menor 18'
        WHEN idade BETWEEN 18 AND 59 THEN 'Adulto'
        ELSE 'Idoso 60+'
    END
ORDER BY custo_medio DESC
"""

resultado = spark.sql(query).toPandas()

# Gráfico menor e compacto
plt.figure(figsize=(6, 4))  # Tamanho reduzido
plt.bar(resultado['faixa_etaria'], resultado['custo_medio'], 
        color=['#FF6B6B', '#4ECDC4', '#45B7D1'], 
        edgecolor='black', linewidth=1)
plt.xlabel('Faixa Etária', fontsize=10)
plt.ylabel('Custo Médio (R$)', fontsize=10)
plt.title('Custo por Faixa Etária', fontsize=12, fontweight='bold')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

query_diag = """
SELECT 
    diagnostico,
    ROUND(AVG(custo), 2) as custo_medio
FROM internacoes_hospitalares
GROUP BY diagnostico
ORDER BY custo_medio DESC
LIMIT 5  -- Top 5 diagnósticos
"""

resultado = spark.sql(query_diag).toPandas()

# Gráfico de barras horizontal (mais compacto)
plt.figure(figsize=(6, 4))  # Tamanho reduzido
plt.barh(resultado['diagnostico'], resultado['custo_medio'], 
         color='#9B59B6', edgecolor='black', linewidth=1)
plt.xlabel('Custo Médio (R$)', fontsize=10)
plt.ylabel('Diagnóstico', fontsize=10)
plt.title('Top 5 Diagnósticos Mais Caros', fontsize=12, fontweight='bold')
plt.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.show()