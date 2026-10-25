import csv
import sys
import logging
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402
from main import (ArgentineDetector, LiveContentDetector, ContentCategorizer, DataManager,  # noqa: E402
                  SearchStrategies, CAMPOS_CSV)


def canal(title='', description='', country=''):
    return {'snippet': {'title': title, 'description': description, 'country': country}}


class TestArgentinidad(unittest.TestCase):
    def setUp(self):
        self.det = ArgentineDetector()

    def test_pais_declarado_ar_conserva_provincia(self):
        r = self.det.analyze_channel(canal('Radio Cuyo', 'Desde Mendoza para todo el país', 'AR'))
        self.assertTrue(r['is_argentine'])
        self.assertEqual(r['province'], 'Mendoza')

    def test_codigo_local_define_provincia(self):
        r = self.det.analyze_channel(canal('El Bunker MZA', 'Charlas en vivo, sumate al chat. Argentina'))
        self.assertTrue(r['is_argentine'])
        self.assertEqual(r['province'], 'Mendoza')
        self.assertEqual(r['method'], 'explicit_country')
        self.assertIn('codigo_MZA', r['indicators'])

    def test_codigo_en_minusculas_no_cuenta(self):
        r = self.det.analyze_channel(canal('ros', 'cor y mza'))
        self.assertFalse(any(i.startswith('codigo_') for i in r['indicators']))

    def test_salta_verbo_no_es_provincia(self):
        r = self.det.analyze_channel(canal('Saltar', 'El que salta gana, argentina en vivo'))
        self.assertNotEqual(r['province'], 'Salta')

    def test_provincia_ambigua_sola_no_ubica(self):
        r = self.det.analyze_channel(canal('Misiones', 'Misiones y aventuras en el juego'))
        self.assertNotEqual(r['province'], 'Misiones')

    def test_ciudad_sin_tildes(self):
        r = self.det.analyze_channel(canal('Stream', 'Desde Bahia Blanca, Argentina', ''))
        self.assertEqual(r['province'], 'Buenos Aires')
        self.assertEqual(r['city'], 'Bahía Blanca')

    def test_boludo_no_excluye_por_uruguay(self):
        # Antes 'bo' y 'ta' como substrings sumaban como Uruguay.
        r = self.det.analyze_channel(canal('Che boludo', 'Uruguay vs Argentina, lo vemos en vivo, sumate'))
        self.assertTrue(r['is_argentine'])

    def test_canal_mexicano_excluido(self):
        r = self.det.analyze_channel(canal('Gaming MX', 'Soy de México, streams en CDMX'))
        self.assertFalse(r['is_argentine'])
        self.assertEqual(r['country'], 'México')

    def test_tuteo_no_es_voseo(self):
        r = self.det.analyze_channel(canal('Canal', '¿Sabes que puedes suscribirte? Síguenos'))
        self.assertFalse(any(i.startswith('voseo_') for i in r['indicators']))

    def test_voseo_detectado(self):
        r = self.det.analyze_channel(canal('Canal', 'Si vos querés aprender, suscribite y seguinos'))
        self.assertTrue(any(i.startswith('voseo_') for i in r['indicators']))


class TestEnVivo(unittest.TestCase):
    def test_requiere_videos_en_vivo_reales(self):
        det = LiveContentDetector()
        sin_vivos = [{'snippet': {'title': 'EN VIVO'}}] * 5
        con_vivos = sin_vivos + [{'snippet': {}, 'liveStreamingDetails': {'actualStartTime': '2026-01-01T00:00:00Z'}}]
        self.assertFalse(det.analyze_channel(canal(description='en vivo'), sin_vivos)['is_live_content'])
        r = det.analyze_channel(canal(), con_vivos)
        self.assertTrue(r['is_live_content'])
        self.assertEqual(r['live_video_count'], 1)


class TestCategorias(unittest.TestCase):
    def test_palabra_completa(self):
        cat = ContentCategorizer()
        self.assertEqual(cat.categorize(canal('Radio FM', 'noticias en vivo')), 'Noticias/Radio')
        # 'lol' no debe aparecer dentro de 'lolita'
        self.assertNotEqual(cat.categorize(canal('Lolita', 'recetas de cocina')), 'Gaming')


class TestFases(unittest.TestCase):
    def test_fase_por_dia(self):
        self.assertEqual([SearchStrategies.fase_del_dia(d) for d in (1, 30, 31, 61, 91, 120)], [1, 1, 2, 3, 4, 4])

    def test_remotas_con_mas_profundidad(self):
        terminos = dict(SearchStrategies.fase_2())
        self.assertGreater(terminos['streaming Catamarca'], terminos['streaming Córdoba'])

    def test_ciclo_se_reinicia(self):
        with tempfile.TemporaryDirectory() as d:
            dm = DataManager(Path(d) / 's.csv', Path(d) / 'e.json')
            dm.estado['fecha_inicio'] = (date.today() - timedelta(days=125)).isoformat()
            dm.estado['progreso'] = {'x': {'completo': True}}
            self.assertEqual(dm.dia_del_ciclo(), 1)
            self.assertEqual(dm.estado['progreso'], {})


class TestCsv(unittest.TestCase):
    def test_migra_csv_viejo_y_deduplica(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 's.csv'
            viejos = CAMPOS_CSV[:-1]
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=viejos)
                w.writeheader()
                w.writerow({k: 'A' if k == 'canal_id' else '' for k in viejos})
                w.writerow({k: 'A' if k == 'canal_id' else '' for k in viejos})
            dm = DataManager(path, Path(d) / 'e.json')
            self.assertEqual(dm.ids_guardados, {'A'})
            with open(path, encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(list(rows[0].keys()), CAMPOS_CSV)


class TestSimulacion(unittest.TestCase):
    def test_corrida_completa_y_reanudacion(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            for attr, sub in (('DATA_DIR', 'data'), ('LOGS_DIR', 'logs'), ('CACHE_DIR', 'cache')):
                setattr(main.Config, attr, base / sub)
            engine = main.StreamingArgentinaEngine(simulacion=True)
            encontrados = engine.execute_daily_search(fase=1, max_terminos=2)
            self.assertGreater(encontrados, 0)
            with open(main.Config.STREAMERS_CSV, encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), encontrados)
            self.assertEqual(len({r['canal_id'] for r in rows}), len(rows))
            self.assertTrue(all(int(r['videos_en_vivo']) > 0 for r in rows))
            self.assertFalse(any(r['provincia'] == '' for r in rows))
            # Los canales mexicanos y chicos quedan rechazados.
            motivos = set(engine.data_manager.estado['procesados'].values())
            self.assertTrue({'otro_pais', 'pocos_suscriptores', 'sin_contenido_vivo'} <= motivos)
            # Segunda corrida: sigue con los términos siguientes y no repite canales.
            engine2 = main.StreamingArgentinaEngine(simulacion=True)
            engine2.execute_daily_search(fase=1, max_terminos=2)
            completos = [t for t, p in engine2.data_manager.estado['progreso'].items() if p['completo']]
            self.assertEqual(len(completos), 4)
            with open(main.Config.STREAMERS_CSV, encoding='utf-8') as f:
                rows2 = list(csv.DictReader(f))
            self.assertGreater(len(rows2), len(rows))
            self.assertEqual(len({r['canal_id'] for r in rows2}), len(rows2))
            for handler in logging.getLogger().handlers:
                handler.close()


if __name__ == '__main__':
    unittest.main()
