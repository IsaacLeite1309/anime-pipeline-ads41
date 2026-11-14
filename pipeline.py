"""ADS41 N3: snapshot CSV -> raw -> Spark -> Parquet -> MongoDB.

Execute `python pipeline.py --help`. Consultas devem respeitar o bloqueio
controle/publicacao; renomear três coleções não é uma transação MongoDB.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
from datetime import datetime, timezone
from coordinator import exclusive, swap
from contracts import schema, manifest_schema, validate_destination

VERSION = '2.0.0'
SOURCE = 'https://www.kaggle.com/datasets/CooperUnion/anime-recommendations-database'
HEADERS = {'anime.csv': ['anime_id','name','genre','type','episodes','rating','members'],
           'rating.csv': ['user_id','anime_id','rating']}
DOMAIN = ('animes', 'usuarios', 'interacoes')

def now():
    return datetime.now(timezone.utc)

def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def dump(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    temp.replace(path)

def capture(root, source, version):
    files = []
    for name, header in HEADERS.items():
        p = source / name
        with p.open(encoding='utf-8-sig', newline='') as f:
            if next(csv.reader(f)) != header:
                raise ValueError(f'Cabeçalho inválido: {name}')
        files.append(dict(nome=name, tamanho_bytes=p.stat().st_size, sha256=checksum(p)))
    batch = hashlib.sha256((VERSION + ''.join(x['sha256'] for x in files)).encode()).hexdigest()
    raw = root / 'raw' / batch
    raw.mkdir(parents=True, exist_ok=True)
    for meta in files:
        dst = raw / meta['nome']
        if dst.exists():
            if checksum(dst) != meta['sha256']:
                raise ValueError('Área bruta alterada: ' + str(dst))
        else:
            temporary = dst.with_suffix('.captura.tmp')
            try:
                shutil.copyfile(source / meta['nome'], temporary)
                if checksum(temporary) != meta['sha256']:
                    raise ValueError('Arquivo de entrada mudou durante a captura: '+meta['nome'])
                temporary.replace(dst)
            finally:
                temporary.unlink(missing_ok=True)
    manifest = dict(_id=batch, origem=SOURCE, versao_fonte=version,
                    versao_regras=VERSION, capturado_em=now(), arquivos=files,
                    inicio=now(), fim=None, status='recebido', metricas=[])
    if not (raw / 'manifesto.json').exists():
        dump(raw / 'manifesto.json', manifest)
    else:
        original = json.loads((raw/'manifesto.json').read_text(encoding='utf-8'))
        manifest['capturado_em'] = datetime.fromisoformat(original['capturado_em'])
    return raw, manifest

def preprocess(raw, dest, partitioned=False):
    """Audita CSV sequencialmente e prepara blocos para leitura Spark particionada."""
    dest.mkdir(parents=True, exist_ok=True)
    for name, header in HEADERS.items():
        folder = dest/(name+'.parts')
        if partitioned:
            folder.mkdir(parents=True,exist_ok=True)
            for old in folder.glob('part-*.csv'):
                old.unlink()
        with (raw/name).open(encoding='utf-8-sig', newline='') as src:
            buffer = []
            def tracked():
                for line in src:
                    buffer.append(line)
                    yield line
            reader = csv.reader(tracked(), strict=True)
            next(reader)
            buffer.clear()
            out = None
            records = 0
            try:
                while True:
                    if out is None or (partitioned and records and records % 250000 == 0):
                        if out is not None:
                            out.close()
                        target = folder/f'part-{records//250000:05d}.csv' if partitioned else dest/name
                        out = target.open('w',encoding='utf-8',newline='')
                        writer = csv.writer(out)
                        writer.writerow(header + ['_original','_posicao','_malformada','_conteudo'])
                    start = reader.line_num
                    try:
                        row = next(reader)
                        malformed = len(row) != len(header)
                    except StopIteration:
                        break
                    except csv.Error:
                        row, malformed = [], True
                    original = ''.join(buffer)
                    buffer.clear()
                    values = (row + ['']*len(header))[:len(header)]
                    writer.writerow(values + [original,str(start+1),str(malformed).lower(),json.dumps(row,ensure_ascii=False)])
                    records += 1
            finally:
                if out is not None:
                    out.close()


def transform(spark, raw, root, batch):
    from pyspark.sql import functions as F, types as T
    work = root/'work'/batch
    preprocess(raw, work, partitioned=True)
    metrics = []
    def read(name):
        schema = T.StructType([T.StructField(x,T.StringType()) for x in HEADERS[name]+['_original','_posicao','_malformada','_conteudo']])
        return spark.read.schema(schema).option('header',True).option('multiLine',True).option('escape','"').csv(str(work/(name+'.parts'))).repartition(8)
    def integer(c):
        return F.when(F.trim(F.col(c)).rlike(r'^-?\d+$'), F.trim(F.col(c)).cast('long'))
    def optional(c, unknown=False):
        absent = F.col(c).isNull() | (F.trim(F.col(c)) == '')
        return absent | (F.lower(F.trim(F.col(c))) == 'unknown') if unknown else absent
    def finish(df, name, keys, fields, valid):
        df = df.withColumn('_motivo',F.when(F.col('_malformada')=='true',F.lit('csv_malformado')).when(~F.coalesce(valid,F.lit(False)),F.lit('valor_invalido'))).cache()
        # Compara conteúdo de entrada inclusive quando uma linha da chave é inválida.
        conflicts = df.groupBy(*keys).agg(F.countDistinct('_conteudo').alias('_variantes')).filter('_variantes > 1').select(*keys)
        marked = df.join(conflicts.withColumn('_conflito',F.lit(True)),keys,'left').withColumn('_motivo',F.when(F.col('_conflito')==True,F.lit('chave_conflitante')).otherwise(F.col('_motivo')))
        bad = marked.filter('_motivo is not null')
        candidates = marked.filter('_motivo is null').drop('_conflito')
        accepted = candidates.dropDuplicates(keys).select(*fields).cache()
        m = dict(arquivo=name,lidas=df.count(),aceitas=accepted.count(),rejeitadas=bad.count(),duplicatas=candidates.count()-accepted.count())
        if m['lidas']!=m['aceitas']+m['rejeitadas']+m['duplicatas']:
            raise ValueError('Métricas não reconciliadas: '+name)
        bad.select(F.lit(name).alias('arquivo'),F.col('_posicao').cast('long').alias('posicao'),F.col('_original').alias('original'),F.col('_motivo').alias('motivo'),F.lit(batch).alias('lote_id')).write.mode('overwrite').parquet(str(root/'quarantine'/batch/name))
        metrics.append(m)
        df.unpersist()
        return accepted
    a = read('anime.csv').withColumn('_id',integer('anime_id')).withColumn('nome',F.trim('name'))
    a = a.withColumn('episodios',integer('episodes')).withColumn('membros',integer('members')).withColumn('nota_fonte',F.col('rating').cast('double'))
    a = a.withColumn('generos',F.expr("array_distinct(filter(transform(split(coalesce(genre, ''), ','), x -> trim(x)), x -> x <> ''))")).withColumn('tipo',F.when(~optional('type'),F.trim('type')))
    valid = (F.col('_id')>0)&(F.length('nome')>0)&(optional('episodes',True)|(F.col('episodios')>0))&(optional('members',True)|(F.col('membros')>=0))&(optional('rating')|F.col('nota_fonte').between(0,10))
    a = finish(a,'anime.csv',['_id'],['_id','nome','generos','tipo','episodios','nota_fonte','membros'], valid)
    i = read('rating.csv').withColumn('_user',integer('user_id')).withColumn('_anime',integer('anime_id')).withColumn('_rating',integer('rating'))
    i = i.drop('user_id','anime_id').withColumnRenamed('_user','user_id').withColumnRenamed('_anime','anime_id')
    i = i.join(a.select(F.col('_id').alias('_catalogo')),F.col('anime_id')==F.col('_catalogo'),'left')
    valid = (F.col('user_id')>0)&(F.col('anime_id')>0)&((F.col('_rating') == -1)|F.col('_rating').between(1,10))&F.col('_catalogo').isNotNull()
    i = i.withColumn('_id',F.concat_ws(':','user_id','anime_id')).withColumn('nota',F.when(F.col('_rating')!=-1,F.col('_rating'))).withColumn('nota_atribuida',F.col('_rating')!=-1)
    i = finish(i,'rating.csv',['user_id','anime_id'],['_id','user_id','anime_id','nota','nota_atribuida'],valid)
    stats = i.groupBy('anime_id').agg(F.count('*').alias('interacoes'),F.count('nota').alias('avaliacoes'),F.avg('nota').alias('media_calculada'))
    a = a.join(stats,a['_id']==stats['anime_id'],'left').drop('anime_id').withColumn('estatisticas',F.struct(F.coalesce('interacoes',F.lit(0)).alias('interacoes'),F.coalesce('avaliacoes',F.lit(0)).alias('avaliacoes'),F.col('media_calculada'))).drop('interacoes','avaliacoes','media_calculada')
    u = i.select(F.col('user_id').alias('_id')).distinct()
    outputs = {}
    for name, df in zip(DOMAIN,(a,u,i)):
        df = df.withColumn('lote_id',F.lit(batch))
        df.write.mode('overwrite').parquet(str(root/'curated'/batch/name))
        outputs[name] = spark.read.parquet(str(root/'curated'/batch/name))
    if outputs['interacoes'].count() != metrics[1]['aceitas']:
        raise ValueError('Parquet diverge das métricas de interações')
    dump(root/'reports'/batch/'metricas.json',metrics)
    return outputs, metrics

def publish(spark, db, frames, manifest, fail=False, fail_during=False, crash_during=False):
    batch = manifest['_id']
    for name, df in frames.items():
        temp = f'{name}__{batch[:16]}'
        db.drop_collection(temp)
        db.create_collection(temp,validator=schema(name,batch))
        # Spark connector: DataFrames Parquet -> BSON, incluindo campos null.
        df.repartition(8).write.format('mongodb').mode('append').option('database',db.name).option('collection',temp).option('operationType','insert').option('maxBatchSize','5000').option('ignoreNullValues','false').save()
        if db[temp].count_documents({}) != df.count():
            raise ValueError('Contagem divergente: '+name)
    ai, ui, ii = [db[f'{n}__{batch[:16]}'] for n in DOMAIN]
    ai.create_index([('generos',1)])
    ai.create_index([('membros',-1)])
    ii.create_index([('user_id',1),('anime_id',1)], unique=True)
    ii.create_index([('anime_id',1)])
    names = {n:f'{n}__{batch[:16]}' for n in DOMAIN}
    checks = validate_destination(db,names,manifest)
    manifest['validacao'] = checks
    manifest['status']='validado'
    db.lotes_ingestao.replace_one({'_id':batch},manifest,upsert=True)
    if fail:
        raise RuntimeError('Falha de demonstração antes da publicação')
    swap(db,manifest,fail_during,crash_during)


def report(db, root, user=101):
    state = db.controle.find_one({'_id':'publicacao'})
    if not state or not state.get('lote_id') or state.get('bloqueado'):
        raise RuntimeError('Consultas suspensas: publicação ausente ou em recuperação')
    result = {'lote_id':state['lote_id'],'contagens':{n:db[n].count_documents({}) for n in DOMAIN},
              'populares':list(db.animes.find({}, {'nome':1,'membros':1,'estatisticas':1}).sort('membros',-1).limit(10)),
              'melhores':list(db.animes.find({'estatisticas.avaliacoes':{'$gte':50}},{'nome':1,'estatisticas':1}).sort('estatisticas.media_calculada',-1).limit(10)),
              'exemplo_sem_nota':db.interacoes.find_one({'nota_atribuida':False}),
              'exemplo_anime':db.animes.find_one()}
    sums = next(db.animes.aggregate([{'$group':{'_id':None,'interacoes':{'$sum':'$estatisticas.interacoes'},'avaliacoes':{'$sum':'$estatisticas.avaliacoes'}}}]),{'interacoes':0,'avaliacoes':0})
    if sums['interacoes'] != result['contagens']['interacoes'] or sums['avaliacoes'] != db.interacoes.count_documents({'nota_atribuida':True}):
        raise ValueError('Estatísticas publicadas inconsistentes')
    result['verificacoes'] = {'interacoes_reconciliadas':True,'avaliacoes_reconciliadas':True}
    result['tipos_para_revisao'] = [t for t in db.animes.distinct('tipo') if t is not None and t not in {'TV','Movie','OVA','ONA','Special','Music'}]
    result['populares_por_interacoes'] = list(db.animes.find({}, {'nome':1,'estatisticas':1}).sort('estatisticas.interacoes',-1).limit(10))
    result['generos'] = list(db.animes.aggregate([
        {'$unwind':'$generos'}, {'$group':{'_id':'$generos','obras':{'$sum':1},
            'avaliacoes':{'$sum':'$estatisticas.avaliacoes'},
            'soma_notas':{'$sum':{'$multiply':['$estatisticas.media_calculada','$estatisticas.avaliacoes']}}}},
        {'$project':{'obras':1,'avaliacoes':1,'media_calculada':{'$cond':[{'$gt':['$avaliacoes',0]},{'$divide':['$soma_notas','$avaliacoes']},None]}}},
        {'$sort':{'obras':-1}}]))
    result['formatos'] = list(db.animes.aggregate([
        {'$group':{'_id':'$tipo','obras':{'$sum':1},'membros':{'$sum':'$membros'},
            'membros_ausentes':{'$sum':{'$cond':[{'$eq':['$membros',None]},1,0]}},
            'notas_fonte_ausentes':{'$sum':{'$cond':[{'$eq':['$nota_fonte',None]},1,0]}},
            'media_fonte':{'$avg':'$nota_fonte'},'avaliacoes':{'$sum':'$estatisticas.avaliacoes'},
            'soma_notas':{'$sum':{'$multiply':['$estatisticas.media_calculada','$estatisticas.avaliacoes']}}}},
        {'$set':{'media_calculada':{'$cond':[{'$gt':['$avaliacoes',0]},{'$divide':['$soma_notas','$avaliacoes']},None]}}},
        {'$unset':'soma_notas'},{'$sort':{'obras':-1}}]))
    result['afinidades_usuario'] = {'user_id':user,'generos':list(db.interacoes.aggregate([
        {'$match':{'user_id':user}},
        {'$lookup':{'from':'animes','localField':'anime_id','foreignField':'_id','as':'anime'}},
        {'$unwind':'$anime'},{'$unwind':'$anime.generos'},
        {'$group':{'_id':'$anime.generos','interacoes':{'$sum':1},
            'avaliacoes':{'$sum':{'$cond':['$nota_atribuida',1,0]}},
            'sem_nota':{'$sum':{'$cond':['$nota_atribuida',0,1]}},'media_notas':{'$avg':'$nota'}}},
        {'$sort':{'interacoes':-1}}]))}
    if db.controle.find_one({'_id':'publicacao'}) != state:
        raise RuntimeError('Publicação mudou durante a consulta; repita o relatório')
    dump(root/'reports'/'saida_mongodb.json',result)
    print(json.dumps(result,ensure_ascii=False,indent=2,default=str))

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,default=Path('data/input'))
    p.add_argument('--data',type=Path,default=Path('data'))
    p.add_argument('--uri',default=os.environ.get('MONGO_URI','mongodb://127.0.0.1:27018'))
    p.add_argument('--database',default='anime_analytics')
    p.add_argument('--version',default=None,help='Versão da fonte; fica null quando não informada')
    p.add_argument('--master',default='local[2]')
    p.add_argument('--report',action='store_true')
    p.add_argument('--fail-before-publish',action='store_true')
    p.add_argument('--fail-during-publish',action='store_true')
    p.add_argument('--crash-during-publish',action='store_true')
    p.add_argument('--recover',action='store_true',help='Recupera publicação interrompida sem iniciar nova carga')
    p.add_argument('--user',type=int,default=101,help='Usuário para análise de afinidades no relatório')
    p.add_argument('--resume-curated',action='store_true',help='Reutiliza Parquet e métricas locais do mesmo lote; reconstrói coleções temporárias')
    args = p.parse_args()
    from pymongo import MongoClient
    from pymongo.write_concern import WriteConcern
    client = MongoClient(args.uri,serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    db = client[args.database].with_options(write_concern=WriteConcern(w=1,j=True))
    try:
        with exclusive(db):
            if args.recover:
                print('Recuperação concluída; nenhuma publicação pendente.')
            elif args.report:
                report(db,args.data,args.user)
            else:
                execute(args,db)
    finally:
        client.close()


def execute(args,db):
    spark = None
    manifest = None
    try:
        state = db.controle.find_one({'_id':'publicacao'})
        if state and state.get('bloqueado'):
            raise RuntimeError('Publicação interrompida; consumo permanece bloqueado')
        raw, manifest = capture(args.data,args.input,args.version)
        db.locks.update_one({'_id':'pipeline'},{'$set':{'lote_id':manifest['_id']}})
        if 'lotes_ingestao' in db.list_collection_names():
            db.command({'collMod':'lotes_ingestao','validator':manifest_schema()})
        else:
            db.create_collection('lotes_ingestao',validator=manifest_schema())
        previous = db.lotes_ingestao.find_one({'_id':manifest['_id'],'status':'publicado'})
        if previous and state and state.get('lote_id')==manifest['_id']:
            manifest = previous
            print('Lote já publicado: nenhuma alteração.')
            report(db,args.data,args.user)
            return
        db.lotes_ingestao.replace_one({'_id':manifest['_id']},manifest,upsert=True)
        manifest['status']='processando'
        db.lotes_ingestao.replace_one({'_id':manifest['_id']},manifest,upsert=True)
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.master(args.master).appName('ADS41-Anime-N3')
                 .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:10.7.0')
                 .config('spark.mongodb.write.connection.uri',args.uri)
                 .config('spark.sql.shuffle.partitions','8').config('spark.sql.ansi.enabled','false')
                 .config('spark.sql.jsonGenerator.ignoreNullFields','false').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        if args.resume_curated:
            manifest['metricas'] = json.loads((args.data/'reports'/manifest['_id']/'metricas.json').read_text(encoding='utf-8'))
            frames = {n:spark.read.parquet(str(args.data/'curated'/manifest['_id']/n)) for n in DOMAIN}
            for metric in manifest['metricas']:
                if metric['lidas'] != metric['aceitas']+metric['rejeitadas']+metric['duplicatas']:
                    raise ValueError('Métricas intermediárias inconsistentes')
            expected = {m['arquivo']:m['aceitas'] for m in manifest['metricas']}
            if frames['animes'].count()!=expected['anime.csv'] or frames['interacoes'].count()!=expected['rating.csv']:
                raise ValueError('Parquet não corresponde às métricas do lote')
            if any(df.filter(df.lote_id != manifest['_id']).limit(1).count() for df in frames.values()):
                raise ValueError('Parquet contém documentos de outro lote')
        else:
            frames, manifest['metricas'] = transform(spark,raw,args.data,manifest['_id'])
        publish(spark,db,frames,manifest,args.fail_before_publish,args.fail_during_publish,args.crash_during_publish)
        dump(args.data/'reports'/manifest['_id']/'manifesto_final.json',manifest)
        report(db,args.data,args.user)
    except Exception as exc:
        if manifest and manifest['status'] != 'publicado':
            manifest.update(status='falhou',fim=now(),erro=str(exc))
            db.lotes_ingestao.replace_one({'_id':manifest['_id']},manifest,upsert=True)
            dump(args.data/'reports'/manifest['_id']/'falha.json',manifest)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    main()
