"""Contrato BSON e validação das coleções temporárias antes da publicação."""
def manifest_schema():
    number = {'bsonType':['int','long'],'minimum':0}
    file = {'bsonType':'object','required':['nome','tamanho_bytes','sha256'],
        'properties':{'nome':{'enum':['anime.csv','rating.csv']},'tamanho_bytes':number,
                      'sha256':{'bsonType':'string','pattern':'^[a-f0-9]{64}$'}}}
    metric = {'bsonType':'object','required':['arquivo','lidas','aceitas','rejeitadas','duplicatas'],
        'properties':{'arquivo':{'enum':['anime.csv','rating.csv']},
                      **{n:number for n in ('lidas','aceitas','rejeitadas','duplicatas')}}}
    props = {'_id':{'bsonType':'string'},'origem':{'bsonType':'string'},
        'versao_fonte':{'bsonType':['string','null']},'versao_regras':{'bsonType':'string'},
        'capturado_em':{'bsonType':'date'},'inicio':{'bsonType':'date'},
        'fim':{'bsonType':['date','null']},
        'status':{'enum':['recebido','processando','validado','publicado','falhou']},
        'arquivos':{'bsonType':'array','minItems':2,'maxItems':2,'items':file},
        'metricas':{'bsonType':'array','maxItems':2,'items':metric}}
    return {'$and':[{'$jsonSchema':{'bsonType':'object','required':list(props),'properties':props}},
        {'$or':[{'status':{'$nin':['validado','publicado']}},{'metricas':{'$size':2}}]}]}

def schema(name, batch):
    positive = {'bsonType':['int','long'],'minimum':1}
    count = {'bsonType':['int','long'],'minimum':0}
    props = {'_id':({'bsonType':'string'} if name=='interacoes' else positive),
             'lote_id':{'bsonType':'string','enum':[batch]}}
    if name=='animes':
        props.update(nome={'bsonType':'string','minLength':1},
            generos={'bsonType':'array','uniqueItems':True,'items':{'bsonType':'string','minLength':1}},
            tipo={'bsonType':['string','null']},episodios={'bsonType':['int','long','null'],'minimum':1},
            membros={'bsonType':['int','long','null'],'minimum':0},
            nota_fonte={'bsonType':['double','null'],'minimum':0,'maximum':10},
            estatisticas={'bsonType':'object','additionalProperties':False,
                'required':['interacoes','avaliacoes','media_calculada'],
                'properties':{'interacoes':count,'avaliacoes':count,
                    'media_calculada':{'bsonType':['double','null'],'minimum':1,'maximum':10}}})
    if name=='interacoes':
        props.update(user_id=positive,anime_id=positive,
            nota={'bsonType':['int','long','null'],'minimum':1,'maximum':10},
            nota_atribuida={'bsonType':'bool'})
    validator = {'$jsonSchema':{'bsonType':'object','required':list(props),
        'additionalProperties':False,'properties':props}}
    if name=='interacoes':
        validator = {'$and':[validator,{'$or':[{'nota':None,'nota_atribuida':False},
            {'nota':{'$gte':1,'$lte':10},'nota_atribuida':True}]},
            {'$expr':{'$eq':['$_id',{'$concat':[{'$toString':'$user_id'},':',{'$toString':'$anime_id'}]}]}}]}
    if name=='animes':
        validator = {'$and':[validator,{'$expr':{'$and':[
            {'$lte':['$estatisticas.avaliacoes','$estatisticas.interacoes']},
            {'$or':[{'$and':[{'$eq':['$estatisticas.avaliacoes',0]},{'$eq':['$estatisticas.media_calculada',None]}]},
                    {'$and':[{'$gt':['$estatisticas.avaliacoes',0]},{'$ne':['$estatisticas.media_calculada',None]}]}]}]}}]}
    return validator

def validate_destination(db, names, manifest):
    a,u,i = [db[names[n]] for n in ('animes','usuarios','interacoes')]
    batch = manifest['_id']
    metrics = manifest['metricas']
    if len(metrics)!=2 or {m['arquivo'] for m in metrics}!={'anime.csv','rating.csv'}:
        raise ValueError('São obrigatórias métricas dos dois arquivos')
    for m in metrics:
        if any(not isinstance(m[k],int) or m[k]<0 for k in ('lidas','aceitas','rejeitadas','duplicatas')) or m['lidas']!=m['aceitas']+m['rejeitadas']+m['duplicatas']:
            raise ValueError('Métricas não reconciliadas')
    totals = {n:db[c].count_documents({}) for n,c in names.items()}
    expected = {m['arquivo']:m['aceitas'] for m in metrics}
    if totals['animes']!=expected['anime.csv'] or totals['interacoes']!=expected['rating.csv']:
        raise ValueError('Contagem no destino diverge das métricas')
    for n,c in names.items():
        if db[c].find_one({'$nor':[schema(n,batch)]},{'_id':1}):
            raise ValueError('Documento viola contrato BSON: '+n)
    # Agrupa antes dos lookups: compara cada anime/usuário, sem milhões de buscas repetidas.
    invalid = list(i.aggregate([
        {'$group':{'_id':'$anime_id','interacoes':{'$sum':1},'avaliacoes':{'$sum':{'$cond':['$nota_atribuida',1,0]}},'media':{'$avg':'$nota'}}},
        {'$lookup':{'from':a.name,'localField':'_id','foreignField':'_id','as':'catalogo'}},
        {'$unwind':{'path':'$catalogo','preserveNullAndEmptyArrays':True}},
        {'$match':{'$expr':{'$or':[
            {'$ne':[{'$ifNull':['$catalogo.lote_id',None]},batch]},
            {'$ne':['$interacoes','$catalogo.estatisticas.interacoes']},
            {'$ne':['$avaliacoes','$catalogo.estatisticas.avaliacoes']},
            {'$ne':[{'$eq':['$media',None]},{'$eq':['$catalogo.estatisticas.media_calculada',None]}]},
            {'$gt':[{'$abs':{'$subtract':['$media','$catalogo.estatisticas.media_calculada']}},1e-10]}
        ]}}},{'$limit':1}],allowDiskUse=True))
    if invalid:
        raise ValueError('Referência ou estatísticas por anime inconsistentes')
    users = list(i.aggregate([{'$group':{'_id':'$user_id'}},
        {'$lookup':{'from':u.name,'localField':'_id','foreignField':'_id','as':'usuario'}},
        {'$match':{'$or':[{'usuario':{'$size':0}},{'usuario.lote_id':{'$ne':batch}}]}},{'$limit':1}],allowDiskUse=True))
    if users:
        raise ValueError('Referência a usuário inconsistente')
    distinct_users = list(i.aggregate([{'$group':{'_id':'$user_id'}},{'$count':'n'}],allowDiskUse=True))
    if totals['usuarios'] != (distinct_users[0]['n'] if distinct_users else 0):
        raise ValueError('Usuários não correspondem às interações aceitas')
    sums = next(a.aggregate([{'$group':{'_id':None,'interacoes':{'$sum':'$estatisticas.interacoes'},'avaliacoes':{'$sum':'$estatisticas.avaliacoes'}}}]),{'interacoes':0,'avaliacoes':0})
    if sums['interacoes']!=totals['interacoes'] or sums['avaliacoes']!=i.count_documents({'nota_atribuida':True}):
        raise ValueError('Totais analíticos não reconciliados')
    return {'contagens':totals,'referencias_validas':True,'estatisticas_reconciliadas':True,'metricas_reconciliadas':True}
