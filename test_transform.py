"""Teste de integração Spark/Parquet com dados sintéticos e casos de qualidade."""
import csv
import tempfile
import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from pipeline import capture, transform

class TransformTest(unittest.TestCase):
    def test_quality_and_statistics(self):
        spark=SparkSession.builder.master('local[2]').appName('quality-test').config('spark.sql.shuffle.partitions','2').getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        try:
            with tempfile.TemporaryDirectory(dir='data') as d:
                root=Path(d); source=root/'input';source.mkdir()
                with (source/'anime.csv').open('w',newline='',encoding='utf-8') as f:
                    w=csv.writer(f);w.writerow(['anime_id','name','genre','type','episodes','rating','members'])
                    w.writerows([[1,' A ','Action, Fantasy, Action','TV','Unknown',8.2,100],[2,'B','','Movie',1,'',0],[3,'X','','TV',1,8,10],[3,'Y','','TV',1,8,10],
                        [4,'Valido','','TV',1,8,0],[4,'Valido','','TV',0,8,0]])
                with (source/'rating.csv').open('w',newline='',encoding='utf-8') as f:
                    w=csv.writer(f);w.writerow(['user_id','anime_id','rating'])
                    w.writerows([[1,1,8],[1,1,8],[2,1,-1],[3,1,7],[3,1,9],[4,999,10],[5,1,0],[6,1,8],[6,1,11],
                        [7,4,9],[8,1,-1],[8,1,0]])
                raw,m=capture(root,source,'test')
                frames,metrics=transform(spark,raw,root,m['_id'])
                self.assertEqual([x['aceitas'] for x in metrics],[2,2])
                self.assertEqual(metrics[0]['rejeitadas'],4)
                self.assertEqual(metrics[1]['rejeitadas'],9)
                self.assertEqual(metrics[1]['duplicatas'],1)
                rows={r['_id']:r.asDict(recursive=True) for r in frames['animes'].collect()}
                self.assertEqual(rows[1]['generos'],['Action','Fantasy'])
                self.assertEqual(rows[1]['estatisticas'],{'interacoes':2,'avaliacoes':1,'media_calculada':8.0})
                self.assertEqual(rows[2]['estatisticas'],{'interacoes':0,'avaliacoes':0,'media_calculada':None})
                self.assertIsNone(rows[1]['episodios'])
                self.assertEqual(frames['usuarios'].count(),2)
                rejected=spark.read.parquet(str(root/'quarantine'/m['_id']/'rating.csv'))
                self.assertTrue({'arquivo','posicao','original','motivo','lote_id'} <= set(rejected.columns))
                self.assertEqual(rejected.filter("motivo = 'chave_conflitante'").count(),6)
        finally:spark.stop()

if __name__=='__main__': unittest.main()
