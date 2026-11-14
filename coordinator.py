"""Exclusão de leitores/escritores e recuperação durável da publicação."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import uuid

DOMAIN = ('animes', 'usuarios', 'interacoes')

@contextmanager
def exclusive(db):
    # Arquivo compartilhado pelos comandos desta instalação, mesmo com --data distinto.
    db.client.admin.command('ping')
    # localhost/IP/opções diferentes de URI não podem criar dois coordenadores.
    key = hashlib.sha256(db.name.encode()).hexdigest()
    folder = Path(__file__).parent/'data'/'.locks'
    folder.mkdir(parents=True, exist_ok=True)
    with (folder/key).open('a+b') as handle:
        handle.seek(0); handle.write(b'0'); handle.flush(); handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError('Outra carga ou consulta está em andamento; aguarde e repita.') from exc
        token = uuid.uuid4().hex
        acquired = False
        try:
            identity_file = folder/'installation'
            # Criado sob bloqueio: persiste no volume e sobrevive a novos containers.
            if not identity_file.exists():
                identity_file.write_text(uuid.uuid4().hex,encoding='ascii')
            identity = identity_file.read_text(encoding='ascii')
            old = db.locks.find_one({'_id':'pipeline'})
            if old:
                if old.get('coordinator') != identity:
                    raise RuntimeError('Há um coordenador de outra instalação; não é seguro liberar seu bloqueio.')
                # Só esta instalação possui o bloqueio do SO: o processo anterior morreu.
                if old.get('lote_id'):
                    db.lotes_ingestao.update_one({'_id':old['lote_id'],
                        'status':{'$in':['recebido','processando','validado']}},{'$set':{
                        'status':'falhou','fim':datetime.now(timezone.utc),
                        'erro':'Processo interrompido; bloqueio abandonado recuperado'}})
                db.locks.delete_one({'_id':'pipeline','owner':old['owner']})
            db.locks.insert_one({'_id':'pipeline','owner':token,'coordinator':identity})
            acquired = True
            recover(db)
            yield
        finally:
            if acquired:
                db.locks.delete_one({'_id':'pipeline','owner':token})
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

def recover(db):
    state = db.controle.find_one({'_id':'publicacao'})
    if not state or not state.get('bloqueado'):
        return False
    journal = state.get('journal')
    if not journal:
        raise RuntimeError('Bloqueio sem diário de recuperação; consumo permanece suspenso.')
    for name in reversed(DOMAIN):
        collections = {c['name']:c.get('info',{}).get('uuid') for c in db.list_collections()}
        names = set(collections)
        backup = journal['backups'][name]
        failed = name+'__recuperado_'+journal['attempt']
        original_uuid = journal.get('current_uuid',{}).get(name)
        if backup in names:
            if original_uuid is not None and collections[backup] != original_uuid:
                raise RuntimeError('Backup não corresponde à coleção anterior; consumo permanece suspenso: '+name)
            if name in names:
                db[name].rename(failed, dropTarget=True)
            db[backup].rename(name)
        elif journal['had_current'][name]:
            if name not in names:
                raise RuntimeError('Coleção anterior indisponível; consultas continuam suspensas: '+name)
            if original_uuid is not None:
                if collections[name] != original_uuid:
                    raise RuntimeError('Backup necessário ausente; consultas continuam suspensas: '+name)
            else:
                # Compatibilidade com diários antigos sem UUID: exige prova pelo lote.
                previous_batch = (journal.get('previous') or {}).get('lote_id')
                if not previous_batch or not db[name].find_one({'lote_id':previous_batch}) or db[name].find_one({'lote_id':{'$ne':previous_batch}}):
                    raise RuntimeError('Não foi possível confirmar a coleção anterior: '+name)
        elif name in names:
            db[name].rename(failed, dropTarget=True)
    db.lotes_ingestao.update_one({'_id':journal['batch']},{'$set':{
        'status':'falhou','fim':datetime.now(timezone.utc),'erro':'Publicação interrompida; versão anterior restaurada'}})
    # O diário só é removido depois da restauração integral.
    previous = journal['previous'] or {'_id':'publicacao','lote_id':None}
    previous['bloqueado'] = False
    db.controle.replace_one({'_id':'publicacao'},previous,upsert=True)
    return True

def swap(db, manifest, fail_during=False, crash_during=False):
    batch = manifest['_id']
    attempt = uuid.uuid4().hex[:16]
    previous = db.controle.find_one({'_id':'publicacao'})
    collections = {c['name']:c.get('info',{}).get('uuid') for c in db.list_collections()}
    names = set(collections)
    journal = {'batch':batch,'attempt':attempt,'previous':previous,
               'had_current':{n:n in names for n in DOMAIN},
               'current_uuid':{n:collections.get(n) for n in DOMAIN},
               'backups':{n:n+'__anterior_'+attempt for n in DOMAIN}}
    db.controle.replace_one({'_id':'publicacao'},
        {'_id':'publicacao','bloqueado':True,'journal':journal},upsert=True)
    try:
        for position,name in enumerate(DOMAIN):
            if journal['had_current'][name]:
                db[name].rename(journal['backups'][name])
            db[f'{name}__{batch[:16]}'].rename(name)
            if position == 0:
                if crash_during:
                    os._exit(86)  # Exercita recuperação sem finally/except.
                if fail_during:
                    raise RuntimeError('Falha de demonstração durante a troca de coleções')
        manifest.update(status='publicado',fim=datetime.now(timezone.utc))
        db.lotes_ingestao.replace_one({'_id':batch},manifest)
        db.controle.replace_one({'_id':'publicacao'},
            {'_id':'publicacao','lote_id':batch,'bloqueado':False})
    except Exception:
        restored = recover(db)
        if not restored:
            state = db.controle.find_one({'_id':'publicacao'})
            if state and not state.get('bloqueado') and state.get('lote_id')==batch:
                manifest['status']='publicado'
                return
        manifest['status']='falhou'
        raise
