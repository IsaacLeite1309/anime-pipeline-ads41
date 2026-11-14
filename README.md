# Pipeline de animes — ADS41 N3

Grupo: Isaac Leite, Klaudenilson Alves e Higor Pessoa.

Implementa a especificação N2: CSV → bruto imutável com SHA-256 → PySpark → quarentena e Parquet → MongoDB. O dataset é o Anime Recommendations Database (CooperUnion, Kaggle), versão 1. É um snapshot histórico de animes; não contém mangás ou datas das avaliações.

## Execução recomendada com Docker

Requisitos: Docker Engine com Compose (ou Docker Desktop com containers Linux), conexão com a internet no primeiro build e memória disponível para Spark (recomendados 6 GB para os containers).

Após obter o código como indicado abaixo, baixe o [Anime Recommendations Database no Kaggle](https://www.kaggle.com/datasets/CooperUnion/anime-recommendations-database) e extraia `anime.csv` e `rating.csv` em `data/input/`, dentro da pasta do projeto. Crie essa pasta se necessário e não renomeie as colunas. O dataset, as dependências instaladas e o banco local não acompanham o repositório.

Para obter o código:

```powershell
git clone https://github.com/IsaacLeite1309/anime-pipeline-ads41.git
cd anime-pipeline-ads41
```

Na pasta do projeto:

```powershell
docker compose build
docker compose up -d mongo
docker compose run --rm pipeline
docker compose run --rm pipeline --report
```

O primeiro processamento baixa o conector MongoDB do Maven Central. As próximas execuções podem reutilizar dependências em cache somente se o ambiente as preservar. O MongoDB fica disponível em `mongodb://127.0.0.1:27018`, banco `anime_analytics`. A porta é restrita à máquina local. Use Compass se desejar inspecionar os documentos. O volume Docker preserva o banco.

## Execução local

Requisitos: Python 3.10–3.12, Java 17, MongoDB 8.0 e as dependências de `requirements.txt`. Spark 3.5 usa Scala 2.12 e conector MongoDB 10.7.0. Linux é o ambiente recomendado; a escrita Parquet no Windows pode exigir as bibliotecas nativas do Hadoop compatíveis com o Spark.

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
# JAVA_HOME deve apontar para o JDK 17; MongoDB deve estar iniciado.
.venv\Scripts\python pipeline.py --uri mongodb://127.0.0.1:27018
.venv\Scripts\python pipeline.py --report
```

Em Linux, substitua `.venv\Scripts\python` por `.venv/bin/python`. Para outra fonte local use `--input CAMINHO`; para outro destino, `--uri` e `--database`. Também é possível definir `MONGO_URI` no ambiente, sem gravar credenciais no código. Não publique arquivos de dados, ambientes ou credenciais no GitHub; `.gitignore` já os exclui.

Na instalação local usada pelo grupo, foram preparadas dependências portáteis em `.runtime` e um ambiente Python em `.venv`. Essas pastas não são versionadas. Os scripts abaixo atendem a essa instalação já preparada; para um clone novo, use o fluxo Docker acima ou configure os requisitos da execução local:

```powershell
.\executar.ps1
.\executar.ps1 -Relatorio
```

O script define Java e Hadoop apenas no processo atual. Java foi obtido da Microsoft; MongoDB, do site oficial; bibliotecas Python, do PyPI; o conector é resolvido no Maven Central. Os binários Hadoop para Windows foram obtidos de `https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin`, destino indicado pelo repositório histórico dos mantenedores de winutils. Esses binários são uma dependência adicional exclusiva desta execução Windows; a imagem Docker usa Linux.

O script inicia automaticamente o MongoDB portátil quando o destino padrão local está indisponível e aguarda uma resposta ao comando `ping` antes de continuar. Se ele já estiver funcionando, reutiliza o processo. Após reiniciar o computador, basta executar o mesmo comando novamente. Destinos personalizados por `--uri` ou `MONGO_URI` devem estar disponíveis por conta própria. Também é possível iniciar apenas o banco com `.\iniciar-mongo.ps1`.

A versão da fonte pode ser informada com `-Argumentos @('--version','1')` para o dataset real utilizado neste projeto. Quando não informada, `versao_fonte` fica `null`, conforme a N2; a versão das regras continua identificada separadamente.

## Saídas

- `data/raw/<lote>/`: os dois CSVs originais e manifesto de captura.
- `data/work/<lote>/`: envelope CSV de auditoria, com conteúdo original e linha física inicial. Leitura sequencial, sem carregar o arquivo inteiro na memória.
- `data/quarantine/<lote>/<arquivo>/`: Parquet com registro original, posição e motivo. Referências ausentes são classificadas como valor inválido.
- `data/curated/<lote>/{animes,usuarios,interacoes}/`: Parquet tratado.
- `data/reports/<lote>/`: métricas reconciliadas, manifesto final ou registro de falha.
- `data/reports/saida_mongodb.json`: contagens reais, rankings de popularidade e notas (mínimo 50 avaliações), distribuição de gêneros e exemplos BSON exportados como JSON.
- MongoDB: `animes`, `usuarios`, `interacoes`, `lotes_ingestao`; coleções auxiliares `controle` e `locks` coordenam execução e consultas.

IDs são positivos; nomes têm espaços externos removidos; gêneros viram listas sem repetição; desconhecidos viram null. Nota -1 preserva interação, com `nota=null` e `nota_atribuida=false`. A média usa só notas 1–10. Animes sem interações permanecem com contagens zero. Duplicatas idênticas são removidas; todas as linhas com mesma chave e conteúdo divergente vão para quarentena. Usuários derivam das interações aceitas.

Por arquivo: `lidas = aceitas + rejeitadas + duplicatas`. Antes de publicar, a pipeline valida os contratos BSON completos, as referências, as métricas dos dois arquivos e as contagens e médias de cada anime. O relatório repete a conferência dos totais publicados. Índices únicos impedem repetir o par usuário/anime; o pipeline verifica referências antes de publicar.

## Reexecução e falhas

Execute o mesmo comando duas vezes. Na segunda execução, o lote ativo com os mesmos checksums e versão de regras não é regravado. O relatório é consultado novamente no MongoDB.

Para demonstrar falha isoladamente, use outro banco:

```powershell
docker compose run --rm pipeline --database anime_demo_falha --fail-before-publish
docker compose run --rm pipeline --database anime_demo_falha
```

A primeira execução termina com erro intencional depois de validar as coleções temporárias; a segunda reconstrói e publica. Para testar preservação de uma versão anterior, carregue nesse banco uma entrada diferente e então execute outra entrada com `--fail-before-publish`.

O parâmetro `--fail-during-publish` provoca uma exceção logo após a troca da primeira coleção para exercitar a restauração automática. Use sempre um banco de demonstração e uma entrada diferente da versão ativa (um lote já ativo é ignorado). `examples/` contém dois animes fictícios e duas interações para demonstrações rápidas. Os dados desse exemplo não são resultados do Kaggle.

Se o tratamento terminou e somente a carga falhou, `--resume-curated` permite reutilizar o Parquet local do mesmo lote. A opção confere o identificador do lote e as contagens contra as métricas intermediárias, e reconstrói as coleções temporárias. Use-a apenas com a camada tratada produzida pelo próprio projeto e preservada sem alterações. Exemplo local: `.\executar.ps1 -Argumentos @('--resume-curated')`. A escrita no MongoDB usa oito partições, operações de inserção e lotes de até 5.000 documentos; a deduplicação é feita antes da carga.

A publicação registra um diário durável no MongoDB antes da primeira troca. As consultas gerenciadas pelo projeto e as cargas usam exclusão mútua, impedindo leitura de coleções de versões diferentes. Clientes externos, inclusive Compass, devem consultar após o término da carga e respeitar `controle.publicacao.bloqueado`. MongoDB não fornece troca atômica de três coleções.

Exceções durante a publicação restauram automaticamente os backups antes de liberar consultas. Se o processo ou computador for encerrado abruptamente, o bloqueio do sistema operacional é liberado pelo próprio sistema, enquanto o diário permanece no MongoDB. O próximo comando da mesma instalação recupera o diário e restaura integralmente a versão anterior antes de consultar ou carregar dados. A identidade do coordenador persiste em `data/.locks/installation`, inclusive no volume Docker. Não apague essa pasta. Um bloqueio de outra instalação é recusado para evitar dois escritores.

Para executar apenas a recuperação:

```powershell
.\executar.ps1 -Argumentos @('--recover')
```

`--crash-during-publish` encerra o processo intencionalmente após a primeira troca, sem executar os blocos de limpeza, para demonstrar a recuperação automática no próximo comando. Use exclusivamente um banco de demonstração, com uma versão anterior publicada e uma entrada diferente. Se o MongoDB estiver indisponível ou faltar um backup necessário, o consumo permanece bloqueado; a solução não libera consultas com recuperação incompleta.

Rótulos de formato desconhecidos são preservados nos dados e listados em `tipos_para_revisao` no relatório. Não há treinamento de recomendações nem análise temporal. Gêneros são multivalorados e suas contagens não são aditivas.

## Análises implementadas

O relatório contém dois rankings separados de popularidade (`membros` e quantidade de interações), ranking de médias com pelo menos 50 notas, quantidade de obras e média das notas por gênero, comparação dos formatos com amostras e ausências, e afinidades de um usuário por gênero. As médias por gênero/formato são ponderadas pela quantidade de notas válidas; gêneros são multivalorados e seus totais não são aditivos. Observações sem nota são contabilizadas separadamente na afinidade.

```powershell
.\executar.ps1 -Relatorio -Argumentos @('--user','101')
```

A versão das regras é `2.0.0`. Mudanças de regras geram outro identificador mesmo quando os CSVs são iguais. A base antiga fica preservada como backup da publicação.

## Exemplo rápido

Para processar os dados fictícios de `examples/`, sem baixar o dataset completo:

```powershell
docker compose run --rm pipeline --input examples --data data/demo --database anime_demo
docker compose run --rm pipeline --data data/demo --database anime_demo --report
```

Esse exemplo contém dois animes, dois usuários e duas interações. A primeira obra possui uma nota válida de 8 e uma interação sem nota; a segunda não possui interações. É um exemplo de funcionamento, não uma amostra do dataset real.

## Testes

Os testes usam `unittest`. `test_pipeline.py` confere captura e integridade; `test_transform.py` usa Spark e Parquet; `test_contracts.py` usa o MongoDB local na porta 27018 e bancos temporários exclusivos dos testes.

Com o ambiente local configurado e o MongoDB em execução:

```powershell
.venv\Scripts\python -c "from pathlib import Path; Path('data').mkdir(exist_ok=True)"
.venv\Scripts\python -m unittest test_pipeline test_transform test_contracts -v
```

## Fontes técnicas

Dataset: https://www.kaggle.com/datasets/CooperUnion/anime-recommendations-database

Compatibilidade do conector: https://www.mongodb.com/docs/spark-connector/v10.x/

Requisitos do Spark: https://spark.apache.org/docs/3.5.3/
