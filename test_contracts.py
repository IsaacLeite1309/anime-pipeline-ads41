"""Testes no MongoDB real, com bancos efêmeros próprios."""
import copy
from contextlib import redirect_stdout
import io
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from pymongo import MongoClient
from pymongo.errors import WriteError
from contracts import schema, validate_destination
from coordinator import DOMAIN, exclusive, recover, swap

URI='mongodb://127.0.0.1:27018'

class ContractTest(unittest.TestCase):
    def setUp(self):
        self.client=MongoClient(URI,serverSelectionTimeoutMS=2000)
        self.db=self.client['anime_contract_test_'+uuid.uuid4().hex]
        self.batch='b'*64
        self.names={n:n+'__'+self.batch[:16] for n in DOMAIN}
        self.manifest={'_id':self.batch,'status':'validado','metricas':[
            dict(arquivo='anime.csv',lidas=2,aceitas=2,rejeitadas=0,duplicatas=0),
            dict(arquivo='rating.csv',lidas=2,aceitas=2,rejeitadas=0,duplicatas=0)]}
        for n in DOMAIN:self.db.create_collection(self.names[n],validator=schema(n,self.batch))
        self.anime=dict(_id=1,nome='Teste',generos=['Action'],tipo='TV',episodios=None,
            membros=10,nota_fonte=8.0,estatisticas=dict(interacoes=2,avaliacoes=1,media_calculada=8.0),lote_id=self.batch)
        empty=copy.deepcopy(self.anime);empty.update(_id=2,estatisticas=dict(interacoes=0,avaliacoes=0,media_calculada=None))
        self.db[self.names['animes']].insert_many([self.anime,empty])
        self.db[self.names['usuarios']].insert_many([dict(_id=1,lote_id=self.batch),dict(_id=2,lote_id=self.batch)])
        self.db[self.names['interacoes']].insert_many([
            dict(_id='1:1',user_id=1,anime_id=1,nota=8,nota_atribuida=True,lote_id=self.batch),
            dict(_id='2:1',user_id=2,anime_id=1,nota=None,nota_atribuida=False,lote_id=self.batch)])

    def tearDown(self):
        # Banco aleatório criado exclusivamente pelo teste; nenhuma base do projeto é removida.
        self.client.drop_database(self.db.name)
        self.client.close()

    def test_schema_and_prepublication_statistics(self):
        self.assertTrue(validate_destination(self.db,self.names,self.manifest)['estatisticas_reconciliadas'])
        c=self.db[self.names['animes']]
        for stats in [dict(interacoes=2),dict(interacoes='2',avaliacoes=1,media_calculada=8.0),
                      dict(interacoes=2,avaliacoes=3,media_calculada=8.0),
                      dict(interacoes=2,avaliacoes=0,media_calculada=8.0)]:
            with self.assertRaises(WriteError):c.update_one({'_id':1},{'$set':{'estatisticas':stats}})
        c.update_one({'_id':1},{'$set':{'estatisticas.media_calculada':9.0}})
        with self.assertRaises(ValueError):validate_destination(self.db,self.names,self.manifest)
        self.assertIsNone(self.db.controle.find_one({'_id':'publicacao'}))

    def test_references_metrics_and_identity(self):
        c=self.db[self.names['interacoes']]
        with self.assertRaises(WriteError):c.update_one({'_id':'1:1'},{'$set':{'nota_atribuida':False}})
        with self.assertRaises(WriteError):c.update_one({'_id':'1:1'},{'$set':{'lote_id':'outro'}})
        self.db[self.names['usuarios']].delete_one({'_id':2})
        with self.assertRaises(ValueError):validate_destination(self.db,self.names,self.manifest)

    def prepare_previous(self):
        for name in DOMAIN:self.db[name].insert_one({'_id':'anterior'})
        self.db.controle.insert_one({'_id':'publicacao','lote_id':'anterior','bloqueado':False})
        self.db.lotes_ingestao.insert_one(self.manifest)

    def assert_previous(self):
        for name in DOMAIN:self.assertEqual(list(self.db[name].find()),[{'_id':'anterior'}])
        self.assertEqual(self.db.controle.find_one({'_id':'publicacao'})['lote_id'],'anterior')
        self.assertFalse(self.db.controle.find_one({'_id':'publicacao'})['bloqueado'])

    def test_exception_rollback(self):
        self.prepare_previous()
        with exclusive(self.db):
            with self.assertRaises(RuntimeError):swap(self.db,self.manifest,fail_during=True)
        self.assert_previous()

    def test_abrupt_exit_recovers_before_next_reader(self):
        self.prepare_previous()
        code="""from pymongo import MongoClient
from coordinator import exclusive,swap
import sys
d=MongoClient(sys.argv[1])[sys.argv[2]]
with exclusive(d):swap(d,d.lotes_ingestao.find_one(),crash_during=True)
"""
        p=subprocess.run([sys.executable,'-c',code,URI,self.db.name],cwd=Path(__file__).parent)
        self.assertEqual(p.returncode,86)
        self.assertTrue(self.db.controle.find_one()['bloqueado'])
        with exclusive(self.db):self.assert_previous()
        self.assertEqual(self.db.locks.count_documents({}),0)

    def test_parallel_reader_is_blocked(self):
        code="""from pymongo import MongoClient
from coordinator import exclusive
import sys
try:
 with exclusive(MongoClient(sys.argv[1])[sys.argv[2]]):pass
except RuntimeError:sys.exit(7)
"""
        with exclusive(self.db):
            for uri in (URI,URI.replace('127.0.0.1','localhost')):
                p=subprocess.run([sys.executable,'-c',code,uri,self.db.name],cwd=Path(__file__).parent)
                self.assertEqual(p.returncode,7)

    def test_abrupt_exit_before_publication_marks_batch_failed(self):
        self.prepare_previous()
        code="""from pymongo import MongoClient
from coordinator import exclusive
import sys,os
d=MongoClient(sys.argv[1])[sys.argv[2]]
with exclusive(d):
 b=d.lotes_ingestao.find_one()['_id']
 d.locks.update_one({'_id':'pipeline'},{'$set':{'lote_id':b}})
 d.lotes_ingestao.update_one({'_id':b},{'$set':{'status':'processando'}})
 os._exit(86)
"""
        result=subprocess.run([sys.executable,'-c',code,URI,self.db.name],cwd=Path(__file__).parent)
        self.assertEqual(result.returncode,86)
        with exclusive(self.db):self.assert_previous()
        self.assertEqual(self.db.lotes_ingestao.find_one()['status'],'falhou')

    def test_recovery_between_backup_and_install(self):
        self.prepare_previous()
        previous=self.db.controle.find_one()
        uuids={c['name']:c['info']['uuid'] for c in self.db.list_collections()}
        journal=dict(batch=self.batch,attempt='intervalo',previous=previous,
            had_current={n:True for n in DOMAIN},current_uuid={n:uuids[n] for n in DOMAIN},
            backups={n:n+'__backup_intervalo' for n in DOMAIN})
        self.db.controle.replace_one({'_id':'publicacao'},dict(_id='publicacao',bloqueado=True,journal=journal))
        self.db.animes.rename(journal['backups']['animes'])
        with exclusive(self.db):self.assert_previous()

    def test_missing_required_backup_keeps_consumption_blocked(self):
        self.prepare_previous()
        code="""from pymongo import MongoClient
from coordinator import exclusive,swap
import sys
d=MongoClient(sys.argv[1])[sys.argv[2]]
with exclusive(d):swap(d,d.lotes_ingestao.find_one(),crash_during=True)
"""
        result=subprocess.run([sys.executable,'-c',code,URI,self.db.name],cwd=Path(__file__).parent)
        self.assertEqual(result.returncode,86)
        journal=self.db.controle.find_one()['journal']
        # Remove apenas o backup criado neste banco efêmero para simular perda de dados.
        self.db.drop_collection(journal['backups']['animes'])
        with self.assertRaises(RuntimeError):
            with exclusive(self.db):pass
        self.assertTrue(self.db.controle.find_one()['bloqueado'])

    def test_successful_publication_and_all_analyses(self):
        from pipeline import report
        with exclusive(self.db):
            validate_destination(self.db,self.names,self.manifest)
            self.db.lotes_ingestao.insert_one(self.manifest)
            swap(self.db,self.manifest)
            with tempfile.TemporaryDirectory(dir=Path(__file__).parent/'data') as folder:
                with redirect_stdout(io.StringIO()):report(self.db,Path(folder),user=2)
                result=json.loads((Path(folder)/'reports'/'saida_mongodb.json').read_text(encoding='utf-8'))
        self.assertEqual(result['contagens'],dict(animes=2,usuarios=2,interacoes=2))
        self.assertEqual(result['generos'][0]['media_calculada'],8.0)
        self.assertEqual(result['formatos'][0]['avaliacoes'],1)
        affinity=result['afinidades_usuario']['generos'][0]
        self.assertEqual(affinity['sem_nota'],1)
        self.assertIsNone(affinity['media_notas'])
        self.assertEqual(result['melhores'],[])  # A amostra é inferior a 50 notas.
        self.assertEqual(self.db.lotes_ingestao.find_one()['status'],'publicado')

if __name__=='__main__':unittest.main()
