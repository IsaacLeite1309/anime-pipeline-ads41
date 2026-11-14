import csv
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from pipeline import capture, preprocess

class CaptureTest(unittest.TestCase):
    def test_source_changed_during_copy_is_not_published_as_raw(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'input';source.mkdir()
            (source/'anime.csv').write_text('anime_id,name,genre,type,episodes,rating,members\n1,A,,TV,1,8,0\n',encoding='utf-8')
            (source/'rating.csv').write_text('user_id,anime_id,rating\n1,1,8\n',encoding='utf-8')
            def changed_copy(src,dst):Path(dst).write_bytes(b'arquivo modificado durante captura')
            with patch('pipeline.shutil.copyfile',side_effect=changed_copy):
                with self.assertRaises(ValueError):capture(root,source,'1')
            self.assertEqual(list((root/'raw').glob('*/anime.csv')),[])
            self.assertEqual(list((root/'raw').glob('*/*.tmp')),[])

    def test_capture_preserves_bytes_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); source=root/'input'; source.mkdir()
            a='anime_id,name,genre,type,episodes,rating,members\n1,Teste,"Action, Fantasy",TV,12,8.2,10\n'
            (source/'anime.csv').write_text(a,encoding='utf-8')
            (source/'rating.csv').write_text('user_id,anime_id,rating\n1,1,-1\n',encoding='utf-8')
            raw,m=capture(root,source,'1')
            self.assertEqual((raw/'anime.csv').read_bytes(),(source/'anime.csv').read_bytes())
            self.assertEqual(capture(root,source,'1')[1]['_id'],m['_id'])
            preprocess(raw,root/'work')
            with (root/'work'/'anime.csv').open(encoding='utf-8',newline='') as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(rows[0]['genre'],'Action, Fantasy')
            self.assertEqual(rows[0]['_posicao'],'2')
            (raw/'anime.csv').write_text('alterado',encoding='utf-8')
            with self.assertRaises(ValueError): capture(root,source,'1')

    def test_malformed_row_is_audited(self):
        with tempfile.TemporaryDirectory() as d:
            raw=Path(d)/'raw'; raw.mkdir()
            (raw/'anime.csv').write_text('anime_id,name,genre,type,episodes,rating,members\n1,Nome\n',encoding='utf-8')
            (raw/'rating.csv').write_text('user_id,anime_id,rating\n1,2,-1\n',encoding='utf-8')
            preprocess(raw,Path(d)/'work')
            with (Path(d)/'work'/'anime.csv').open(encoding='utf-8',newline='') as f:
                row=next(csv.DictReader(f))
            self.assertEqual(row['_malformada'],'true')
            self.assertIn('1,Nome',row['_original'])

if __name__=='__main__': unittest.main()
