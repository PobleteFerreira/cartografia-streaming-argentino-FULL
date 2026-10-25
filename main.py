#!/usr/bin/env python3
"""
CARTOGRAFÍA COMPLETA DEL STREAMING ARGENTINO
Busca canales argentinos de YouTube que hacen transmisiones EN VIVO, con una
estrategia anti-sesgo en cuatro fases (términos generales, provincias, códigos
locales y cultura regional).

Uso:
    python main.py                 # corrida diaria con la fase que corresponde
    python main.py --fase 3        # forzar una fase
    python main.py --simulacion    # probar sin API key (datos inventados)

Los resultados quedan en data/ y el estado en cache/. Ninguna de las dos
carpetas se sube a git.
"""

import os
import re
import csv
import json
import time
import zlib
import random
import logging
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from dataclasses import dataclass, asdict, field
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_YOUTUBE_API = True
except ImportError:
    HAS_YOUTUBE_API = False

    class HttpError(Exception):
        pass


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


# =============================================================================
# CONFIGURACIÓN
# =============================================================================

class Config:
    API_KEYS = [k for k in (os.getenv('YOUTUBE_API_KEY', ''), os.getenv('API_KEY_2', '')) if k]

    # Cuota de YouTube Data API v3: 10.000 unidades por día y por proyecto.
    CUOTA_POR_CLAVE = _env_int('CUOTA_DIARIA_LIMITE', 10000)
    CUOTA_BUFFER = _env_int('CUOTA_BUFFER_SEGURIDAD', 500)
    COSTO_BUSQUEDA = 100      # search.list
    COSTO_LISTA = 1           # channels.list / playlistItems.list / videos.list

    RESULTADOS_POR_PAGINA = 50   # máximo que admite search.list
    VIDEOS_A_REVISAR = 50        # últimas subidas que se miran para detectar vivos

    MIN_SUSCRIPTORES = _env_int('MIN_SUSCRIPTORES', 500)
    MIN_CERTEZA_ARGENTINA = _env_int('MIN_CERTEZA_ARGENTINA', 70)
    MIN_VIDEOS_EN_VIVO = 1
    DIAS_POR_FASE = 30

    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / 'data'
    LOGS_DIR = BASE_DIR / 'logs'
    CACHE_DIR = BASE_DIR / 'cache'
    STREAMERS_CSV = DATA_DIR / 'streamers_argentinos.csv'
    ESTADO = CACHE_DIR / 'estado.json'
    CUOTA = CACHE_DIR / 'cuota.json'

    PROVINCIAS = [
        'Buenos Aires', 'Ciudad Autónoma de Buenos Aires', 'Córdoba', 'Santa Fe',
        'Mendoza', 'Tucumán', 'Salta', 'Entre Ríos', 'Misiones', 'Chaco',
        'Corrientes', 'Santiago del Estero', 'Jujuy', 'Neuquén', 'Río Negro',
        'Formosa', 'Chubut', 'San Luis', 'Catamarca', 'La Rioja', 'San Juan',
        'Santa Cruz', 'Tierra del Fuego', 'La Pampa',
    ]

    # Nombres alternativos de provincia que aparecen en descripciones.
    ALIAS_PROVINCIA = {
        'caba': 'Ciudad Autónoma de Buenos Aires',
        'capital federal': 'Ciudad Autónoma de Buenos Aires',
        'ciudad de buenos aires': 'Ciudad Autónoma de Buenos Aires',
    }

    # Nombres que también son palabras comunes o lugares de otros países: suman
    # poco y sólo definen la provincia si hay otra señal argentina.
    PROVINCIAS_AMBIGUAS = {
        'Córdoba', 'Salta', 'Misiones', 'Corrientes', 'San Juan', 'Santa Cruz',
        'La Rioja', 'Formosa', 'Santa Fe', 'Chaco',
    }

    CIUDADES = {
        'Rosario': 'Santa Fe', 'La Plata': 'Buenos Aires', 'Mar del Plata': 'Buenos Aires',
        'Bahía Blanca': 'Buenos Aires', 'Quilmes': 'Buenos Aires', 'Lomas de Zamora': 'Buenos Aires',
        'Posadas': 'Misiones', 'Resistencia': 'Chaco', 'Paraná': 'Entre Ríos',
        'San Miguel de Tucumán': 'Tucumán', 'San Salvador de Jujuy': 'Jujuy',
        'Bariloche': 'Río Negro', 'San Carlos de Bariloche': 'Río Negro',
        'Comodoro Rivadavia': 'Chubut', 'Trelew': 'Chubut', 'Puerto Madryn': 'Chubut',
        'Río Gallegos': 'Santa Cruz', 'Ushuaia': 'Tierra del Fuego', 'Río Grande': 'Tierra del Fuego',
        'Santa Rosa': 'La Pampa', 'San Rafael': 'Mendoza', 'Godoy Cruz': 'Mendoza',
        'Villa Mercedes': 'San Luis', 'Río Cuarto': 'Córdoba', 'Villa Carlos Paz': 'Córdoba',
        'Rafaela': 'Santa Fe', 'Concordia': 'Entre Ríos', 'Gualeguaychú': 'Entre Ríos',
    }
    CIUDADES_AMBIGUAS = {'Paraná', 'Santa Rosa', 'Río Grande', 'Concordia', 'San Rafael', 'Resistencia', 'Posadas'}

    # Códigos locales. Se buscan en MAYÚSCULAS y como palabra completa en el
    # texto original para no confundirlos con palabras comunes.
    CODIGOS_LOCALES = {
        'CABA': 'Ciudad Autónoma de Buenos Aires', 'BSAS': 'Buenos Aires',
        'MZA': 'Mendoza', 'MDZ': 'Mendoza',
        'CBA': 'Córdoba', 'COR': 'Córdoba',
        'ROS': 'Santa Fe', 'SFE': 'Santa Fe',
        'SLA': 'Salta', 'TUC': 'Tucumán', 'NQN': 'Neuquén',
        'MDQ': 'Buenos Aires', 'BRC': 'Río Negro', 'JUJ': 'Jujuy',
        'SDE': 'Santiago del Estero', 'CTES': 'Corrientes', 'USH': 'Tierra del Fuego',
    }

    @classmethod
    def setup_directories(cls):
        for directory in (cls.DATA_DIR, cls.LOGS_DIR, cls.CACHE_DIR):
            directory.mkdir(parents=True, exist_ok=True)


CAMPOS_CSV = [
    'canal_id', 'nombre_canal', 'categoria', 'provincia', 'ciudad',
    'suscriptores', 'certeza', 'metodo_deteccion', 'indicadores_argentinidad',
    'url', 'fecha_deteccion', 'ultima_actividad', 'tiene_streaming',
    'descripcion', 'pais_detectado', 'videos_analizados', 'videos_en_vivo',
]


@dataclass
class StreamerData:
    canal_id: str
    nombre_canal: str
    categoria: str
    provincia: str
    ciudad: str
    suscriptores: int
    certeza: float
    metodo_deteccion: str
    indicadores_argentinidad: List[str]
    url: str
    fecha_deteccion: str
    ultima_actividad: str
    tiene_streaming: bool
    descripcion: str
    pais_detectado: str
    videos_analizados: int
    videos_en_vivo: int = 0


def normalizar(texto: str) -> str:
    """Minúsculas y sin tildes, para comparar 'Córdoba' con 'cordoba'."""
    texto = unicodedata.normalize('NFD', texto.lower())
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')


def contiene_palabra(texto_normalizado: str, termino: str) -> bool:
    return re.search(rf'(?<!\w){re.escape(normalizar(termino))}(?!\w)', texto_normalizado) is not None


def primera_posicion(texto_normalizado: str, termino: str) -> int:
    m = re.search(rf'(?<!\w){re.escape(normalizar(termino))}(?!\w)', texto_normalizado)
    return m.start() if m else -1


# =============================================================================
# LOGGING Y ESTADÍSTICAS
# =============================================================================

class Logger:
    def __init__(self, nivel=logging.INFO):
        log_file = Config.LOGS_DIR / f'streaming_{datetime.now():%Y%m%d_%H%M%S}.log'
        logging.basicConfig(
            level=nivel,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()],
            force=True,
        )
        logging.getLogger('googleapiclient').setLevel(logging.WARNING)
        self.logger = logging.getLogger('StreamingArgentina')
        self.stats = defaultdict(int)
        self.start_time = time.time()

    def log_channel_found(self, data: StreamerData):
        self.logger.info(
            f"✅ ENCONTRADO: {data.nombre_canal} | {data.provincia} | {data.suscriptores:,} subs | "
            f"Certeza: {data.certeza:.0f}% | Método: {data.metodo_deteccion} | "
            f"Vivos: {data.videos_en_vivo}/{data.videos_analizados} | Categoría: {data.categoria}"
        )
        self.stats['channels_found'] += 1
        self.stats[f'cat_{data.categoria}'] += 1
        self.stats[f'prov_{data.provincia}'] += 1

    def log_channel_rejected(self, channel_id: str, reason: str, details: str = ""):
        self.logger.debug(f"❌ {channel_id}: {reason} {details}")
        self.stats['channels_rejected'] += 1
        self.stats[f'reject_{reason}'] += 1

    def print_summary(self, cuota_usada: int):
        duration = (time.time() - self.start_time) / 60
        print("\n" + "=" * 80)
        print("📊 RESUMEN DE EJECUCIÓN")
        print("=" * 80)
        print(f"⏱️  Duración: {duration:.1f} minutos")
        print(f"🔋 Cuota usada en esta corrida: {cuota_usada:,} unidades")
        print(f"✅ Canales encontrados: {self.stats['channels_found']}")
        print(f"❌ Canales rechazados: {self.stats['channels_rejected']}")
        for titulo, prefijo, limite in (("📉 Razones de rechazo", 'reject_', None),
                                        ("📂 Categorías", 'cat_', None),
                                        ("🗺️  Provincias", 'prov_', 10)):
            items = [(k[len(prefijo):], v) for k, v in self.stats.items() if k.startswith(prefijo)]
            if items:
                print(f"\n{titulo}:")
                for nombre, count in sorted(items, key=lambda x: x[1], reverse=True)[:limite]:
                    print(f"   - {nombre}: {count}")


# =============================================================================
# DETECTOR DE CONTENIDO EN VIVO
# =============================================================================

class LiveContentDetector:
    """
    La señal principal es objetiva: cuántas de las últimas subidas del canal
    fueron transmisiones en vivo según la API (liveStreamingDetails). Las
    palabras clave de la descripción sólo se guardan como indicadores.
    """

    live_keywords = [
        'en vivo', 'en directo', 'streaming', 'stream', 'transmision', 'transmisiones',
        'transmito', 'livestream', 'live', 'twitch', 'kick', 'streamyard', 'obs studio',
        'streamlabs', 'superchat', 'super chat',
    ]
    schedule_patterns = [
        r'\b(?:lunes|martes|miercoles|jueves|viernes|sabados?|domingos?)\b',
        r'\b\d{1,2}\s*(?:hs|hrs|horas)\b',
        r'\btodos los dias\b',
        r'\bde lunes a\b',
        r'\bhorarios?\b',
    ]

    def analyze_channel(self, channel_data: dict, videos_data: List[dict]) -> Dict:
        description = normalizar(channel_data.get('snippet', {}).get('description', ''))
        indicators = [f"keyword_{k.replace(' ', '_')}" for k in self.live_keywords
                      if contiene_palabra(description, k)]
        if any(re.search(p, description) for p in self.schedule_patterns):
            indicators.append('horario_publicado')
        if re.search(r'(?:twitch\.tv|kick\.com)/\w+', description):
            indicators.append('url_plataforma_streaming')

        live_videos = [v for v in videos_data if v.get('liveStreamingDetails', {}).get('actualStartTime')]
        live_count = len(live_videos)
        total = len(videos_data)
        if live_count:
            indicators.append(f'videos_en_vivo_{live_count}')
        # Certeza de streaming = proporción de subidas recientes que fueron vivos.
        certainty = round(100 * live_count / total) if total else 0
        return {
            'is_live_content': live_count >= Config.MIN_VIDEOS_EN_VIVO,
            'certainty': certainty,
            'indicators': indicators,
            'live_video_count': live_count,
            'videos_analyzed': total,
        }


# =============================================================================
# DETECTOR DE ARGENTINIDAD
# =============================================================================

class ArgentineDetector:
    # Se aplican sobre el texto en minúsculas CON tildes: sin ellas "sabés"
    # (voseo) sería igual a "sabes" (tuteo).
    voseo_patterns = [
        r'\bvos\s+(?:tenés|sabés|querés|podés|andás|venís|hacés|decís|sos|estás)\b',
        r'\b(?:tenés|sabés|querés|podés|andás|venís|hacés|decís)\b',
        r'\b(?:suscribite|suscribíte|seguinos|seguínos|sumate|sumáte|bancanos|unite|dejanos|contanos)\b',
        r'\bche\s+(?:vos|boludo|loco|flaco)\b',
        r'\bdale\s+che\b',
    ]
    argentine_expressions = {
        'che': 10, 'boludo': 12, 'boluda': 12, 'chabon': 10, 'flaco': 8,
        'pibe': 10, 'piba': 10, 'pibes': 10, 'wacho': 10, 'copado': 10, 'piola': 10,
        'zarpado': 12, 'groso': 10, 'grosso': 10, 'al pedo': 12, 'una banda': 10,
        'barbaro': 8, 'flashear': 10, 'chamuyar': 12, 'laburar': 10, 'laburo': 10,
        'guita': 10, 'bondi': 12, 'birra': 8, 'escabio': 10, 'pucho': 10,
        'mate': 8, 'asado': 8, 'fernet': 10, 'cuarteto': 8,
    }
    MAX_PUNTOS_MODISMOS = 40
    argentina_mentions = ['argentina', 'argentino', 'argentinos', 'argentinas']

    # País -> términos que, como palabra completa, indican ese país. Hacen
    # falta 2 términos distintos (o una frase "soy de X") para excluir.
    country_exclusions = {
        'España': ['españa', 'madrid', 'barcelona', 'valencia', 'sevilla', 'mola', 'vale tio'],
        'México': ['mexico', 'mexicano', 'mexicana', 'cdmx', 'guadalajara', 'monterrey', 'chilango', 'wey', 'neta'],
        'Colombia': ['colombia', 'colombiano', 'colombiana', 'bogota', 'medellin', 'parcero', 'parce'],
        'Chile': ['chile', 'chileno', 'chilena', 'santiago de chile', 'weon', 'cachai', 'valparaiso'],
        'Perú': ['peru', 'peruano', 'peruana', 'lima', 'cusco', 'arequipa'],
        'Uruguay': ['uruguay', 'uruguayo', 'uruguaya', 'montevideo', 'botija'],
        'Venezuela': ['venezuela', 'venezolano', 'venezolana', 'caracas', 'maracaibo', 'chamo'],
        'Paraguay': ['paraguay', 'paraguayo', 'paraguaya', 'asuncion'],
        'Bolivia': ['bolivia', 'boliviano', 'boliviana', 'la paz bolivia', 'cochabamba'],
    }

    def analyze_channel(self, channel_data: dict, videos_data: List[dict] = None) -> Dict:
        snippet = channel_data.get('snippet', {})
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        country = snippet.get('country', '')
        original = f"{title} {description}"
        text = normalizar(original)

        province, city, location_indicators, location_score = self._detect_location(original, text)

        # País declarado en la configuración del canal: es la señal más fuerte.
        if country == 'AR':
            return {
                'is_argentine': True, 'certainty': 100, 'province': province or 'Argentina',
                'city': city, 'indicators': ['metadata_AR'] + location_indicators,
                'score': 100, 'method': 'explicit_country',
            }

        other_country = self._check_other_countries(text)
        if other_country:
            return {'is_argentine': False, 'certainty': 0, 'country': other_country,
                    'reason': 'otro_pais', 'indicators': [], 'province': '', 'city': ''}

        score = 0
        indicators = []
        method = 'cultural_analysis'

        if country:
            score -= 50
            indicators.append(f'metadata_{country}')

        if any(contiene_palabra(text, m) for m in self.argentina_mentions) or '🇦🇷' in original:
            score += 40
            indicators.append('menciona_argentina')
            method = 'explicit_country'

        if location_indicators:
            score += location_score
            indicators.extend(location_indicators)
            if method == 'cultural_analysis':
                method = 'local_code' if any(i.startswith('codigo_') for i in location_indicators) else 'province_mention'

        voseo_count = sum(len(re.findall(p, original.lower())) for p in self.voseo_patterns)
        if voseo_count:
            score += min(voseo_count * 15, 45)
            indicators.append(f'voseo_{voseo_count}')
            if method == 'cultural_analysis' and voseo_count >= 2:
                method = 'voseo_patterns'

        modismos = [m for m in self.argentine_expressions if contiene_palabra(text, m)]
        if modismos:
            score += min(sum(self.argentine_expressions[m] for m in modismos), self.MAX_PUNTOS_MODISMOS)
            indicators.append(f'modismos_{len(modismos)}')

        if videos_data:
            video_score = self._analyze_video_content(videos_data)
            if video_score:
                score += video_score
                indicators.append(f'video_content_{video_score}')

        # Una provincia ambigua sola no alcanza para ubicar el canal.
        strong_signal = score >= Config.MIN_CERTEZA_ARGENTINA
        if province in Config.PROVINCIAS_AMBIGUAS and not strong_signal:
            province, city = '', ''

        certainty = max(0, min(100, score))
        return {
            'is_argentine': certainty >= Config.MIN_CERTEZA_ARGENTINA,
            'certainty': certainty,
            'province': province or 'Argentina',
            'city': city,
            'indicators': indicators,
            'score': score,
            'method': method,
            'reason': 'no_argentino',
        }

    def _detect_location(self, original: str, text: str) -> Tuple[str, str, List[str], int]:
        """Devuelve (provincia, ciudad, indicadores, puntaje). Prioridad: código
        local, provincia no ambigua, ciudad, provincia ambigua."""
        indicators = []
        score = 0
        candidates = []  # (prioridad, posición, provincia, ciudad)

        for code, prov in Config.CODIGOS_LOCALES.items():
            m = re.search(rf'(?<![A-Za-z]){code}(?![A-Za-z])', original)
            if m:
                indicators.append(f'codigo_{code}')
                score += 20
                candidates.append((0, m.start(), prov, ''))

        for alias, prov in Config.ALIAS_PROVINCIA.items():
            pos = primera_posicion(text, alias)
            if pos >= 0:
                indicators.append(f'provincia_{prov}')
                score += 15
                candidates.append((1, pos, prov, ''))

        for prov in Config.PROVINCIAS:
            pos = primera_posicion(text, prov)
            if pos < 0:
                continue
            ambigua = prov in Config.PROVINCIAS_AMBIGUAS
            indicators.append(f'provincia_{prov}')
            score += 5 if ambigua else 15
            candidates.append((3 if ambigua else 1, pos, prov, ''))

        for ciudad, prov in Config.CIUDADES.items():
            pos = primera_posicion(text, ciudad)
            if pos < 0:
                continue
            ambigua = ciudad in Config.CIUDADES_AMBIGUAS
            indicators.append(f'ciudad_{ciudad}')
            score += 5 if ambigua else 12
            candidates.append((4 if ambigua else 2, pos, prov, ciudad))

        if not candidates:
            return '', '', [], 0
        _, _, province, city = min(candidates)
        if not city:
            # Si además se nombra una ciudad de esa provincia, la guardamos.
            city = next((c for prio, _, p, c in sorted(candidates) if c and p == province), '')
        return province, city, indicators, min(score, 45)

    def _check_other_countries(self, text: str) -> Optional[str]:
        for country, keywords in self.country_exclusions.items():
            name = keywords[0]
            for frase in (f'soy de {name}', f'desde {name}', f'vivo en {name}', f'hecho en {name}'):
                if contiene_palabra(text, frase):
                    return country
            if sum(1 for kw in keywords if contiene_palabra(text, kw)) >= 2:
                return country
        return None

    def _analyze_video_content(self, videos: List[dict]) -> int:
        score = 0
        for video in videos[:10]:
            snippet = video.get('snippet', {})
            crudo = f"{snippet.get('title', '')} {snippet.get('description', '')[:300]}".lower()
            video_text = normalizar(crudo)
            if any(re.search(p, crudo) for p in self.voseo_patterns):
                score += 5
            if any(contiene_palabra(video_text, m) for m in self.argentine_expressions):
                score += 3
            if any(contiene_palabra(video_text, m) for m in self.argentina_mentions):
                score += 5
        return min(score, 30)


# =============================================================================
# CLASIFICADOR DE CATEGORÍAS
# =============================================================================

class ContentCategorizer:
    categories = {
        'Gaming': (['gaming', 'gamer', 'juegos', 'gameplay', 'jugando', 'videojuegos', 'ps5', 'xbox',
                    'minecraft', 'fortnite', 'lol', 'valorant', 'warzone', 'gta', 'fifa'], 1.0),
        'Música': (['musica', 'music', 'cantante', 'cantando', 'concierto', 'banda', 'covers', 'acustico',
                    'instrumental', 'dj', 'remix', 'folklore', 'cumbia', 'cuarteto', 'rock'], 1.0),
        'Cocina': (['cocina', 'cocinando', 'recetas', 'chef', 'cooking', 'gastronomia', 'comida',
                    'parrilla', 'asado', 'empanadas'], 1.0),
        'Charlas/Podcast': (['podcast', 'charla', 'charlas', 'charlando', 'entrevista', 'entrevistas',
                             'debate', 'opinion', 'invitado', 'invitados', 'mesa', 'analisis'], 1.0),
        'Noticias/Radio': (['noticias', 'radio', 'informativo', 'periodismo', 'actualidad', 'fm'], 1.0),
        'Educativo': (['educativo', 'tutorial', 'clase', 'clases', 'curso', 'aprender', 'leccion',
                       'profesor', 'universidad', 'escuela'], 1.0),
        'Deportes': (['deporte', 'deportes', 'futbol', 'basquet', 'tenis', 'rugby', 'gym', 'fitness',
                      'partido', 'club', 'torneo'], 1.0),
        'Arte/Creatividad': (['arte', 'dibujando', 'pintura', 'diseño', 'ilustracion', 'dibujo',
                              'manualidades', 'artesania'], 1.0),
        'Tecnología': (['tecnologia', 'tech', 'programacion', 'software', 'hardware', 'gadget'], 1.0),
        'Religión': (['iglesia', 'misa', 'culto', 'parroquia', 'evangelio', 'pastor'], 1.0),
        'Estilo de Vida': (['lifestyle', 'vlog', 'rutina', 'viajes', 'tips', 'consejos', 'bienestar',
                            'belleza'], 0.8),
        'Entretenimiento': (['entretenimiento', 'show', 'humor', 'comedia', 'reacciones', 'react',
                             'challenges', 'retos', 'stand up'], 0.7),
    }

    def categorize(self, channel_data: dict, videos_data: List[dict] = None) -> str:
        snippet = channel_data.get('snippet', {})
        partes = [snippet.get('title', ''), snippet.get('description', '')]
        partes += [v.get('snippet', {}).get('title', '') for v in (videos_data or [])[:10]]
        text = normalizar(' '.join(partes))
        scores = defaultdict(float)
        for category, (keywords, weight) in self.categories.items():
            for keyword in keywords:
                if contiene_palabra(text, keyword):
                    scores[category] += weight
        if not scores:
            return 'Entretenimiento'
        return max(scores.items(), key=lambda x: x[1])[0]


# =============================================================================
# GESTIÓN DE CUOTA (por clave, con reinicio a medianoche del Pacífico)
# =============================================================================

def fecha_cuota() -> str:
    """YouTube reinicia la cuota a medianoche, hora del Pacífico."""
    try:
        from zoneinfo import ZoneInfo
        ahora = datetime.now(ZoneInfo('America/Los_Angeles'))
    except Exception:
        ahora = datetime.now(timezone(timedelta(hours=-8)))
    return ahora.strftime('%Y-%m-%d')


class QuotaTracker:
    def __init__(self, n_keys: int, archivo: Path = None):
        self.archivo = archivo or Config.CUOTA
        self.n_keys = max(1, n_keys)
        self.usada_en_corrida = 0
        self.fecha = fecha_cuota()
        self.usada = [0] * self.n_keys
        if self.archivo.exists():
            try:
                data = json.loads(self.archivo.read_text(encoding='utf-8'))
                if data.get('fecha') == self.fecha:
                    guardada = data.get('usada', [])
                    self.usada = [guardada[i] if i < len(guardada) else 0 for i in range(self.n_keys)]
            except (ValueError, OSError):
                pass

    def save(self):
        self.archivo.write_text(json.dumps({'fecha': self.fecha, 'usada': self.usada}), encoding='utf-8')

    def restante(self, key_index: int) -> int:
        return Config.CUOTA_POR_CLAVE - Config.CUOTA_BUFFER - self.usada[key_index]

    def puede(self, key_index: int, costo: int) -> bool:
        return self.restante(key_index) >= costo

    def usar(self, key_index: int, costo: int):
        self.usada[key_index] += costo
        self.usada_en_corrida += costo
        self.save()

    def agotar(self, key_index: int):
        """La API dijo quotaExceeded: la clave no sirve más por hoy."""
        self.usada[key_index] = Config.CUOTA_POR_CLAVE
        self.save()


class CuotaAgotada(Exception):
    pass


class ClaveInvalida(Exception):
    pass


ERRORES_DE_CLAVE = ('keyInvalid', 'API key not valid', 'accessNotConfigured', 'API_KEY_INVALID',
                    'has not been used in project', 'API_KEY_SERVICE_BLOCKED', 'forbidden')


def describir_error(e: Exception) -> str:
    """Mensaje corto de un HttpError, sin la URL (que incluye la API key)."""
    status = getattr(getattr(e, 'resp', None), 'status', '?')
    try:
        detalle = json.loads(e.content.decode('utf-8'))['error']['message']
    except Exception:
        detalle = type(e).__name__
    return f"HTTP {status}: {detalle}"


# =============================================================================
# CLIENTE YOUTUBE
# =============================================================================

class YouTubeClient:
    def __init__(self, api_keys: List[str]):
        if not HAS_YOUTUBE_API:
            raise RuntimeError("Falta google-api-python-client: pip install -r requirements.txt")
        if not api_keys:
            raise RuntimeError("No hay API key configurada (YOUTUBE_API_KEY)")
        self.logger = logging.getLogger('StreamingArgentina')
        self.api_keys = api_keys
        self.quota = QuotaTracker(len(api_keys))
        self.key_index = 0
        self._clients = {}
        self._elegir_clave(0)

    def _elegir_clave(self, minimo: int) -> bool:
        for i in range(len(self.api_keys)):
            if self.quota.puede(i, minimo):
                self.key_index = i
                return True
        return False

    def _youtube(self):
        if self.key_index not in self._clients:
            self._clients[self.key_index] = build('youtube', 'v3', developerKey=self.api_keys[self.key_index],
                                                  cache_discovery=False)
        return self._clients[self.key_index]

    def cuota_disponible(self, costo: int = Config.COSTO_BUSQUEDA) -> bool:
        return self._elegir_clave(costo)

    def _ejecutar(self, costo: int, hacer_request):
        """Ejecuta un request rotando de clave si una se queda sin cuota."""
        while True:
            if not self._elegir_clave(costo):
                raise CuotaAgotada()
            try:
                response = hacer_request(self._youtube()).execute()
                self.quota.usar(self.key_index, costo)
                return response
            except HttpError as e:
                razon = str(e)
                if getattr(e, 'resp', None) is not None and e.resp.status == 403 and (
                        'quotaExceeded' in razon or 'dailyLimitExceeded' in razon):
                    self.logger.warning(f"🔄 Clave #{self.key_index + 1} sin cuota, rotando")
                    self.quota.agotar(self.key_index)
                    continue
                if getattr(e, 'resp', None) is not None and e.resp.status in (400, 403) and any(
                        m in razon for m in ERRORES_DE_CLAVE):
                    raise ClaveInvalida(f"Clave #{self.key_index + 1} rechazada: {describir_error(e)}")
                raise

    def search_channels(self, query: str, page_token: Optional[str]) -> dict:
        return self._ejecutar(Config.COSTO_BUSQUEDA, lambda yt: yt.search().list(
            q=query, part='snippet', type='channel', maxResults=Config.RESULTADOS_POR_PAGINA,
            pageToken=page_token, regionCode='AR', relevanceLanguage='es'))

    def get_channels(self, channel_ids: List[str]) -> List[dict]:
        if not channel_ids:
            return []
        response = self._ejecutar(Config.COSTO_LISTA, lambda yt: yt.channels().list(
            part='snippet,statistics,contentDetails', id=','.join(channel_ids[:50]), maxResults=50))
        return response.get('items', [])

    def get_recent_videos(self, uploads_playlist: str) -> List[dict]:
        if not uploads_playlist:
            return []
        try:
            playlist = self._ejecutar(Config.COSTO_LISTA, lambda yt: yt.playlistItems().list(
                part='contentDetails', playlistId=uploads_playlist, maxResults=Config.VIDEOS_A_REVISAR))
        except HttpError as e:
            if getattr(e, 'resp', None) is not None and e.resp.status == 404:
                return []  # canal sin subidas públicas
            raise
        ids = [i['contentDetails']['videoId'] for i in playlist.get('items', [])]
        if not ids:
            return []
        videos = self._ejecutar(Config.COSTO_LISTA, lambda yt: yt.videos().list(
            part='snippet,liveStreamingDetails', id=','.join(ids)))
        return videos.get('items', [])


class SimulatedYouTubeClient:
    """Cliente falso para probar el flujo completo sin API key ni cuota."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.quota = QuotaTracker(1, archivo=Config.CACHE_DIR / 'cuota_simulacion.json')
        self.quota.usada = [0]
        self._canales = {}

    def cuota_disponible(self, costo: int = Config.COSTO_BUSQUEDA) -> bool:
        return self.quota.puede(0, costo)

    def search_channels(self, query: str, page_token: Optional[str]) -> dict:
        self.quota.usar(0, Config.COSTO_BUSQUEDA)
        pagina = int(page_token or 0)
        items = []
        for i in range(Config.RESULTADOS_POR_PAGINA):
            cid = f'SIM_{zlib.crc32(f"{query}|{pagina}|{i}".encode()):010d}'
            perfil = self.rng.choice(['ar_vivo', 'ar_vivo', 'ar_sin_vivo', 'mx', 'ar_codigo', 'chico'])
            self._canales[cid] = perfil
            items.append({'id': {'kind': 'youtube#channel', 'channelId': cid}, 'snippet': {'title': cid}})
        siguiente = str(pagina + 1) if pagina < 3 else None
        return {'items': items, 'nextPageToken': siguiente}

    def get_channels(self, channel_ids: List[str]) -> List[dict]:
        self.quota.usar(0, Config.COSTO_LISTA)
        descripciones = {
            'ar_vivo': ('Canal argentino. Transmitimos en vivo de lunes a viernes 20hs. Suscribite!', 'AR'),
            'ar_sin_vivo': ('Videos de cocina argentina, recetas caseras. Seguinos!', 'AR'),
            'mx': ('Desde México para todo el mundo, en vivo cada semana wey', 'MX'),
            'ar_codigo': ('Charlas en vivo desde MZA, mate y amigos. Sumate al chat', ''),
            'chico': ('en vivo che', 'AR'),
        }
        out = []
        for cid in channel_ids:
            perfil = self._canales.get(cid, 'ar_vivo')
            desc, pais = descripciones[perfil]
            out.append({
                'id': cid,
                'snippet': {'title': f'Canal simulado {cid[-4:]}', 'description': desc, 'country': pais},
                'statistics': {'subscriberCount': '100' if perfil == 'chico' else str(self.rng.randint(800, 90000))},
                'contentDetails': {'relatedPlaylists': {'uploads': f'UU{cid}' if perfil != 'ar_sin_vivo' else f'UUnolive{cid}'}},
            })
        return out

    def get_recent_videos(self, uploads_playlist: str) -> List[dict]:
        self.quota.usar(0, 2 * Config.COSTO_LISTA)
        en_vivo = not uploads_playlist.startswith('UUnolive')
        videos = []
        for i in range(10):
            video = {'snippet': {'title': f'Programa #{i}', 'description': 'Charlando con los pibes',
                                 'publishedAt': (datetime(2026, 1, 1) - timedelta(days=i)).isoformat() + 'Z'}}
            if en_vivo and i % 3 == 0:
                video['liveStreamingDetails'] = {'actualStartTime': video['snippet']['publishedAt']}
            videos.append(video)
        return videos


# =============================================================================
# ESTADO PERSISTENTE Y RESULTADOS
# =============================================================================

class DataManager:
    """Guarda los canales encontrados (CSV) y el estado de la búsqueda (JSON):
    canales ya analizados, motivo de rechazo y progreso de cada término."""

    def __init__(self, csv_path: Path = None, estado_path: Path = None):
        self.logger = logging.getLogger('StreamingArgentina')
        self.csv_path = csv_path or Config.STREAMERS_CSV
        self.estado_path = estado_path or Config.ESTADO
        self.estado = self._cargar_estado()
        self.ids_guardados = self._leer_ids_csv()

    def _cargar_estado(self) -> dict:
        estado = {}
        if self.estado_path.exists():
            try:
                estado = json.loads(self.estado_path.read_text(encoding='utf-8'))
            except (ValueError, OSError):
                self.logger.warning("⚠️ estado.json ilegible, se arranca de cero")
        estado.setdefault('fecha_inicio', date.today().isoformat())
        estado.setdefault('procesados', {})   # canal_id -> 'ok' | motivo de rechazo
        estado.setdefault('progreso', {})     # término -> {'paginas': n, 'token': str|None, 'completo': bool}
        return estado

    def save_state(self):
        tmp = self.estado_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(self.estado, ensure_ascii=False), encoding='utf-8')
        tmp.replace(self.estado_path)

    def _leer_ids_csv(self) -> set:
        if not self.csv_path.exists():
            return set()
        with open(self.csv_path, 'r', encoding='utf-8-sig', errors='replace', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            columnas = reader.fieldnames or []
        desconocidas = [c for c in columnas if c not in CAMPOS_CSV] + (['(sin nombre)'] if any(None in r for r in rows) else [])
        if desconocidas:
            raise SystemExit(f"❌ {self.csv_path} tiene columnas que este script no conoce ({desconocidas}). "
                             "Movelo o renombralo para no perder datos.")
        if columnas != CAMPOS_CSV:
            # CSV de una versión anterior (u otras columnas): se reescribe con el
            # encabezado actual y sin duplicados para que las filas nuevas no queden corridas.
            vistos, unicos = set(), []
            for row in rows:
                if row.get('canal_id') and row['canal_id'] not in vistos:
                    vistos.add(row['canal_id'])
                    unicos.append(row)
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(unicos)
            self.logger.info(f"🧹 CSV actualizado al formato actual ({len(unicos)} canales)")
        return {row.get('canal_id', '') for row in rows} - {''}

    # --- canales ---
    def is_processed(self, channel_id: str) -> bool:
        return channel_id in self.estado['procesados'] or channel_id in self.ids_guardados

    def mark_processed(self, channel_id: str, reason: str = 'ok'):
        self.estado['procesados'][channel_id] = reason

    def save_channel(self, data: StreamerData):
        if data.canal_id in self.ids_guardados:
            return
        nuevo = not self.csv_path.exists()
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV, extrasaction='ignore')
            if nuevo:
                writer.writeheader()
            row = asdict(data)
            row['indicadores_argentinidad'] = ', '.join(row['indicadores_argentinidad'])
            row['descripcion'] = re.sub(r'\s+', ' ', row['descripcion'])[:500]
            writer.writerow(row)
        self.ids_guardados.add(data.canal_id)

    # --- fases ---
    def dia_del_ciclo(self, hoy: date = None) -> int:
        hoy = hoy or date.today()
        inicio = date.fromisoformat(self.estado['fecha_inicio'])
        dia = (hoy - inicio).days + 1
        ciclo = SearchStrategies.DURACION_CICLO
        if dia > ciclo:
            # Termina el ciclo de 120 días: se reinicia el progreso de los términos
            # (los canales ya analizados se siguen salteando).
            self.estado['fecha_inicio'] = hoy.isoformat()
            self.estado['progreso'] = {}
            dia = 1
        return max(1, dia)

    def progreso(self, termino: str) -> dict:
        return self.estado['progreso'].setdefault(termino, {'paginas': 0, 'token': None, 'completo': False})


# =============================================================================
# PROCESADOR DE CANALES
# =============================================================================

class ChannelProcessor:
    def __init__(self, youtube_client, logger_system, data_manager: DataManager):
        self.youtube = youtube_client
        self.logger_system = logger_system
        self.logger = logging.getLogger('StreamingArgentina')
        self.argentine_detector = ArgentineDetector()
        self.live_detector = LiveContentDetector()
        self.categorizer = ContentCategorizer()
        self.data_manager = data_manager

    def _rechazar(self, channel_id: str, reason: str, details: str = ''):
        self.logger_system.log_channel_rejected(channel_id, reason, details)
        self.data_manager.mark_processed(channel_id, reason)

    def process_search_page(self, items: List[dict]) -> int:
        """Analiza los canales de una página de resultados. Devuelve cuántos se guardaron."""
        ids = []
        for item in items:
            cid = item.get('id', {}).get('channelId') if isinstance(item.get('id'), dict) else item.get('id')
            if cid and cid not in ids and not self.data_manager.is_processed(cid):
                ids.append(cid)
        if not ids:
            return 0
        encontrados = 0
        for detalle in self.youtube.get_channels(ids):
            try:
                if self.process_channel(detalle):
                    encontrados += 1
            except HttpError as e:
                # Error puntual de un canal: no se marca, se reintenta otro día.
                self.logger.error(f"Error de API con el canal {detalle.get('id')}: {describir_error(e)}")
        return encontrados

    def process_channel(self, channel_details: dict) -> Optional[StreamerData]:
        channel_id = channel_details.get('id')
        if not channel_id or self.data_manager.is_processed(channel_id):
            return None
        snippet = channel_details.get('snippet', {})
        stats = channel_details.get('statistics', {})

        # Canales que ocultan sus suscriptores no se descartan por este filtro.
        if not stats.get('hiddenSubscriberCount'):
            subscribers = int(stats.get('subscriberCount', 0) or 0)
            if subscribers < Config.MIN_SUSCRIPTORES:
                self._rechazar(channel_id, 'pocos_suscriptores', f'({subscribers})')
                return None
        else:
            subscribers = 0

        # Primera pasada sólo con el texto del canal: descarta lo claramente
        # extranjero antes de gastar cuota en videos.
        arg_analysis = self.argentine_detector.analyze_channel(channel_details)
        if arg_analysis.get('reason') == 'otro_pais' and not arg_analysis['is_argentine']:
            self._rechazar(channel_id, 'otro_pais', f"({arg_analysis.get('country')})")
            return None

        uploads = channel_details.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', '')
        videos = self.youtube.get_recent_videos(uploads)

        if not arg_analysis['is_argentine']:
            arg_analysis = self.argentine_detector.analyze_channel(channel_details, videos)
            if not arg_analysis['is_argentine']:
                self._rechazar(channel_id, 'no_argentino', f"(certeza {arg_analysis['certainty']:.0f}%)")
                return None

        live_analysis = self.live_detector.analyze_channel(channel_details, videos)
        if not live_analysis['is_live_content']:
            self._rechazar(channel_id, 'sin_contenido_vivo', f"({live_analysis['videos_analyzed']} videos)")
            return None

        fechas = [v.get('snippet', {}).get('publishedAt', '') for v in videos]
        streamer_data = StreamerData(
            canal_id=channel_id,
            nombre_canal=snippet.get('title', 'Sin nombre'),
            categoria=self.categorizer.categorize(channel_details, videos),
            provincia=arg_analysis.get('province') or 'Argentina',
            ciudad=arg_analysis.get('city', ''),
            suscriptores=subscribers,
            certeza=arg_analysis['certainty'],
            metodo_deteccion=arg_analysis.get('method', 'cultural_analysis'),
            indicadores_argentinidad=arg_analysis.get('indicators', []) + live_analysis['indicators'],
            url=f"https://www.youtube.com/channel/{channel_id}",
            fecha_deteccion=date.today().isoformat(),
            ultima_actividad=max(fechas) if fechas else '',
            tiene_streaming=True,
            descripcion=snippet.get('description', ''),
            pais_detectado='Argentina',
            videos_analizados=live_analysis['videos_analyzed'],
            videos_en_vivo=live_analysis['live_video_count'],
        )
        self.data_manager.save_channel(streamer_data)
        self.data_manager.mark_processed(channel_id, 'ok')
        self.logger_system.log_channel_found(streamer_data)
        return streamer_data


# =============================================================================
# ESTRATEGIAS DE BÚSQUEDA ANTI-SESGO (4 fases de 30 días)
# =============================================================================

class SearchStrategies:
    """
    Fase 1: términos generales.
    Fase 2: provincias; las remotas reciben más profundidad.
    Fase 3: códigos locales (MZA, CBA, SLA...) combinados con formatos.
    Fase 4: cultura regional, términos hiper-específicos para micro-streamers.

    La profundidad está en páginas de la API (50 resultados cada una). YouTube
    deja de devolver páginas siguientes alrededor de los 500 resultados por
    término, así que en la práctica la búsqueda llega hasta donde la API permite.
    """

    DURACION_CICLO = 120

    PROVINCIAS_GRANDES = ['Buenos Aires', 'Córdoba', 'Santa Fe', 'Mendoza', 'Rosario', 'La Plata', 'Mar del Plata']
    PROVINCIAS_MEDIANAS = ['Tucumán', 'Salta', 'Entre Ríos', 'Misiones', 'Chaco', 'Corrientes', 'Neuquén',
                           'San Juan', 'Jujuy', 'Río Negro', 'Santiago del Estero']
    PROVINCIAS_REMOTAS = ['Catamarca', 'La Rioja', 'Formosa', 'San Luis', 'La Pampa', 'Chubut',
                          'Santa Cruz', 'Tierra del Fuego', 'Bariloche', 'Ushuaia', 'Comodoro Rivadavia']

    @classmethod
    def fase_1(cls) -> List[Tuple[str, int]]:
        return [
            ('streaming argentina', 10), ('argentina en vivo', 10), ('stream argentino', 10),
            ('canal de streaming argentino', 10), ('en vivo desde argentina', 8),
            ('podcast argentina en vivo', 8), ('gaming argentina en vivo', 8),
            ('música argentina en vivo', 6), ('charla argentina en vivo', 6),
            ('noticias en vivo argentina', 6), ('radio en vivo argentina', 6),
            ('deportes argentina en vivo', 6), ('cocina argentina en vivo', 4),
            ('stand up argentino en vivo', 4), ('argentina twitch youtube', 4),
        ]

    @classmethod
    def fase_2(cls) -> List[Tuple[str, int]]:
        terminos = []
        for lugares, paginas in ((cls.PROVINCIAS_GRANDES, 6), (cls.PROVINCIAS_MEDIANAS, 8),
                                 (cls.PROVINCIAS_REMOTAS, 10)):
            for lugar in lugares:
                terminos += [(f'streaming {lugar}', paginas), (f'en vivo {lugar}', paginas),
                             (f'gaming {lugar}', paginas // 2), (f'podcast {lugar}', paginas // 2)]
        return terminos

    @classmethod
    def fase_3(cls) -> List[Tuple[str, int]]:
        codigos = ['MZA', 'MDZ', 'CBA', 'COR', 'ROS', 'SLA', 'TUC', 'NQN', 'MDQ', 'BRC', 'JUJ', 'SDE', 'CTES', 'USH']
        formatos = ['streaming', 'en vivo', 'charlas', 'gaming', 'bunker']
        return [(f'{formato} {codigo}', 6) for codigo in codigos for formato in formatos]

    @classmethod
    def fase_4(cls) -> List[Tuple[str, int]]:
        return [
            ('mate gaming', 6), ('folklore streaming', 8), ('folklore en vivo', 8), ('che gaming', 6),
            ('che en vivo', 6), ('chamamé en vivo', 8), ('cuarteto en vivo', 8), ('cumbia en vivo argentina', 6),
            ('peña folklórica en vivo', 8), ('asado en vivo', 4), ('fernet stream', 4), ('truco en vivo', 6),
            ('fútbol de ascenso en vivo', 6), ('radio comunitaria en vivo', 8), ('misa en vivo parroquia argentina', 4),
            ('streaming patagonia', 8), ('streaming norte argentino', 8), ('streaming litoral', 8),
            ('streaming cuyo', 8), ('mirá esto en vivo', 4), ('re en vivo', 4), ('boludo stream', 4),
        ]

    @classmethod
    def por_fase(cls, fase: int) -> List[Tuple[str, int]]:
        return [cls.fase_1, cls.fase_2, cls.fase_3, cls.fase_4][fase - 1]()

    @classmethod
    def fase_del_dia(cls, dia: int) -> int:
        return min(4, (dia - 1) // Config.DIAS_POR_FASE + 1)


# =============================================================================
# MOTOR PRINCIPAL
# =============================================================================

class StreamingArgentinaEngine:
    def __init__(self, simulacion: bool = False, verbose: bool = False):
        Config.setup_directories()
        self.logger_system = Logger(logging.DEBUG if verbose else logging.INFO)
        self.logger = self.logger_system.logger
        if simulacion:
            Config.STREAMERS_CSV = Config.DATA_DIR / 'simulacion_streamers.csv'
            Config.ESTADO = Config.CACHE_DIR / 'estado_simulacion.json'
            self.youtube = SimulatedYouTubeClient()
        else:
            self.youtube = YouTubeClient(Config.API_KEYS)
        self.data_manager = DataManager(Config.STREAMERS_CSV, Config.ESTADO)
        self.processor = ChannelProcessor(self.youtube, self.logger_system, self.data_manager)

    def _cola_de_terminos(self, fase: int) -> List[Tuple[int, str, int]]:
        """Términos pendientes de la fase actual y, si se terminaron, de las siguientes."""
        cola = []
        for f in [fase] + [x for x in (1, 2, 3, 4) if x != fase]:
            for termino, paginas in SearchStrategies.por_fase(f):
                if not self.data_manager.progreso(termino)['completo']:
                    cola.append((f, termino, paginas))
        return cola

    def execute_daily_search(self, fase: int = None, max_terminos: int = None) -> int:
        dia = self.data_manager.dia_del_ciclo()
        fase = fase or SearchStrategies.fase_del_dia(dia)
        cola = self._cola_de_terminos(fase)
        if max_terminos:
            cola = cola[:max_terminos]
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 Día {dia}/{SearchStrategies.DURACION_CICLO} del ciclo | Fase {fase} | "
                         f"{len(cola)} términos pendientes")
        self.logger.info("=" * 80)

        encontrados = 0
        try:
            for fase_termino, termino, max_paginas in cola:
                prog = self.data_manager.progreso(termino)
                self.logger.info(f"🔍 [F{fase_termino}] '{termino}' (página {prog['paginas'] + 1} de {max_paginas})")
                fallo = False
                while prog['paginas'] < max_paginas:
                    if not self.youtube.cuota_disponible(Config.COSTO_BUSQUEDA):
                        raise CuotaAgotada()
                    try:
                        response = self.youtube.search_channels(termino, prog['token'])
                        items = response.get('items', [])
                        encontrados += self.processor.process_search_page(items)
                    except HttpError as e:
                        if prog['token'] and getattr(e, 'resp', None) is not None and e.resp.status == 400:
                            self.logger.warning("   Token de página vencido, se reinicia el término")
                            prog.update(paginas=0, token=None)
                            continue
                        self.logger.error(f"   Error de API, se saltea el término por hoy: {describir_error(e)}")
                        fallo = True
                        break
                    prog['paginas'] += 1
                    prog['token'] = response.get('nextPageToken')
                    self.data_manager.save_state()
                    if not prog['token'] or not items:
                        break
                if not fallo:
                    prog['completo'] = True
                    prog['token'] = None
                self.data_manager.save_state()
        except CuotaAgotada:
            self.logger.warning("⏸️ Cuota del día agotada. La próxima corrida sigue desde acá.")
        except ClaveInvalida as e:
            self.logger.error(f"❌ {e}. Revisá la clave y que YouTube Data API v3 esté habilitada.")
        except KeyboardInterrupt:
            self.logger.warning("⏸️ Interrumpido. El progreso quedó guardado.")
        finally:
            self.data_manager.save_state()
            self.logger_system.print_summary(self.youtube.quota.usada_en_corrida)
        return encontrados


def main():
    parser = argparse.ArgumentParser(description='Cartografía del streaming argentino en YouTube')
    parser.add_argument('--fase', type=int, choices=[1, 2, 3, 4], help='forzar una fase de búsqueda')
    parser.add_argument('--max-terminos', type=int, help='procesar como máximo N términos en esta corrida')
    parser.add_argument('--simulacion', action='store_true', help='usar datos inventados, sin API key')
    parser.add_argument('-v', '--verbose', action='store_true', help='mostrar también los rechazos')
    args = parser.parse_args()

    if not args.simulacion and not Config.API_KEYS:
        print("❌ No hay API key configurada.")
        print("   Copiá .env.example a .env y completá YOUTUBE_API_KEY, o probá con --simulacion")
        raise SystemExit(1)

    try:
        engine = StreamingArgentinaEngine(simulacion=args.simulacion, verbose=args.verbose)
    except RuntimeError as e:
        print(f"❌ {e}")
        raise SystemExit(1)
    encontrados = engine.execute_daily_search(fase=args.fase, max_terminos=args.max_terminos)
    print(f"\n✅ {encontrados} canales nuevos | Datos en: {Config.STREAMERS_CSV}")


if __name__ == "__main__":
    main()
