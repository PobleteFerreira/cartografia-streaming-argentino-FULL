#!/usr/bin/env python3
"""
CARTOGRAFÍA COMPLETA DEL STREAMING ARGENTINO - VERSIÓN OPTIMIZADA
Detecta y mapea TODOS los canales argentinos que hacen contenido EN VIVO en YouTube.
Optimizado para no exceder cuota diaria y máxima precisión.
"""

import os
import json
import csv
import re
import time
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from collections import defaultdict

# =======================
# FUNCIÓN: Limpiar duplicados al inicio
# =======================
def limpiar_duplicados_csv(path_csv):
    try:
        import pandas as pd
        if not Path(path_csv).exists():
            return
        df = pd.read_csv(path_csv)
        antes = len(df)
        df = df.drop_duplicates(subset='canal_id', keep='first')
        df.to_csv(path_csv, index=False, quoting=csv.QUOTE_ALL)
        print(f"✔️ Duplicados eliminados en {path_csv}: {antes-len(df)} eliminados, {len(df)} filas finales")
    except Exception as e:
        print(f"Error al limpiar duplicados: {e}")

# =======================
# Dependencias de Google API
# =======================
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_YOUTUBE_API = True
except ImportError:
    HAS_YOUTUBE_API = False
    print("⚠️  googleapiclient no instalado. Ejecutando en modo simulación.")

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

class Config:
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', '')
    API_KEY_2 = os.getenv('API_KEY_2', '')
    MAX_DAILY_QUOTA = 20000
    SAFETY_BUFFER = 4000
    QUOTA_WARNING_THRESHOLD = 14000
    COST_SEARCH = 100
    COST_CHANNEL_DETAILS = 3
    COST_VIDEO_LIST = 3
    MAX_PAGES_PER_SEARCH = 8
    MAX_RESULTS_PER_PAGE = 100
    MAX_CHANNELS_PER_DAY = 800
    MIN_SUBSCRIBERS = 500
    MIN_CERTAINTY_ARGENTINA = 65
    MIN_CERTAINTY_STREAMING = 70
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / 'data'
    LOGS_DIR = BASE_DIR / 'logs'
    CACHE_DIR = BASE_DIR / 'cache'
    STREAMERS_CSV = DATA_DIR / 'streamers_argentinos.csv'
    PROCESSED_CHANNELS = CACHE_DIR / 'processed_channels.pkl'
    REJECTED_CHANNELS = CACHE_DIR / 'rejected_channels.pkl'
    API_CACHE = CACHE_DIR / 'api_cache.json'
    QUOTA_TRACKER = CACHE_DIR / 'quota_tracker.json'
    PROVINCIAS_ARGENTINAS = {
        "Buenos Aires", "CABA", "Ciudad de Buenos Aires", "Capital Federal",
        "Córdoba", "Santa Fe", "Mendoza", "Tucumán", "Salta", "Entre Ríos",
        "Misiones", "Chaco", "Corrientes", "Santiago del Estero", "Jujuy",
        "Neuquén", "Río Negro", "Formosa", "Chubut", "San Luis", "Catamarca",
        "La Rioja", "San Juan", "Santa Cruz", "Tierra del Fuego", "La Pampa"
    }
    CIUDADES_ARGENTINAS = {
        "Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata", "Tucumán",
        "Mar del Plata", "Salta", "Santa Fe", "San Juan", "Resistencia",
        "Neuquén", "Corrientes", "Posadas", "Bahía Blanca", "Paraná"
    }
    CODIGOS_ARGENTINOS = {
        'CABA': 'Ciudad Autónoma de Buenos Aires',
        'BSAS': 'Buenos Aires', 'BA': 'Buenos Aires',
        'CORDOBA': 'Córdoba', 'CBA': 'Córdoba',
        'MENDOZA': 'Mendoza', 'MDZ': 'Mendoza',
        'ROSARIO': 'Santa Fe', 'SANTA FE': 'Santa Fe',
        'TUCUMAN': 'Tucumán', 'SALTA': 'Salta', 'MISIONES': 'Misiones',
        'CHACO': 'Chaco', 'CORRIENTES': 'Corrientes', 'ENTRE RIOS': 'Entre Ríos',
        'FORMOSA': 'Formosa', 'JUJUY': 'Jujuy', 'LA PAMPA': 'La Pampa',
        'LA RIOJA': 'La Rioja', 'NEUQUEN': 'Neuquén', 'RIO NEGRO': 'Río Negro',
        'SAN JUAN': 'San Juan', 'SAN LUIS': 'San Luis', 'SANTA CRUZ': 'Santa Cruz',
        'SANTIAGO DEL ESTERO': 'Santiago del Estero', 'TIERRA DEL FUEGO': 'Tierra del Fuego',
        'CHUBUT': 'Chubut', 'ARGENTINA': 'Argentina', 'ARG': 'Argentina', 'AR': 'Argentina'
    }
    @classmethod
    def setup_directories(cls):
        for directory in [cls.DATA_DIR, cls.LOGS_DIR, cls.CACHE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

class Logger:
    def __init__(self):
        self.setup_logging()
        self.stats = defaultdict(int)
        self.start_time = time.time()
    def setup_logging(self):
        log_file = Config.LOGS_DIR / f'streaming_{datetime.now():%Y%m%d_%H%M%S}.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('StreamingArgentina')
    def log_channel_found(self, data: StreamerData):
        self.logger.info(
            f"✅ ENCONTRADO: {data.nombre_canal} | "
            f"{data.provincia} | {data.suscriptores:,} subs | "
            f"Certeza: {data.certeza:.0f}% | "
            f"Método: {data.metodo_deteccion} | "
            f"Categoría: {data.categoria}"
        )
        self.stats['channels_found'] += 1
        self.stats[f'cat_{data.categoria}'] += 1
        self.stats[f'prov_{data.provincia}'] += 1
    def log_channel_rejected(self, channel_id: str, reason: str, details: str = ""):
        self.logger.debug(f"❌ {channel_id}: {reason} {details}")
        self.stats['channels_rejected'] += 1
        self.stats[f'reject_{reason}'] += 1
    def log_quota_status(self, used: int, total: int):
        percentage = (used / total) * 100
        remaining = total - used
        emoji = "🟢" if percentage < 50 else "🟡" if percentage < 80 else "🔴"
        self.logger.info(
            f"{emoji} CUOTA: {used:,}/{total:,} ({percentage:.1f}%) | "
            f"Restante: {remaining:,}"
        )
    def print_summary(self):
        duration = (time.time() - self.start_time) / 60
        print("\n" + "="*80)
        print("📊 RESUMEN DE EJECUCIÓN")
        print("="*80)
        print(f"⏱️  Duración: {duration:.1f} minutos")
        print(f"✅ Canales encontrados: {self.stats['channels_found']}")
        print(f"❌ Canales rechazados: {self.stats['channels_rejected']}")
        print(f"🔍 Total analizados: {self.stats['channels_found'] + self.stats['channels_rejected']}")
        print("\n📉 Razones de rechazo:")
        reject_reasons = [(k.replace('reject_', ''), v) for k, v in self.stats.items() if k.startswith('reject_')]
        for reason, count in sorted(reject_reasons, key=lambda x: x[1], reverse=True):
            print(f"   - {reason}: {count}")
        print("\n📂 Categorías de contenido:")
        categories = [(k.replace('cat_', ''), v) for k, v in self.stats.items() if k.startswith('cat_')]
        for cat, count in sorted(categories, key=lambda x: x[1], reverse=True):
            print(f"   - {cat}: {count}")
        print("\n🗺️  Distribución por provincia:")
        provinces = [(k.replace('prov_', ''), v) for k, v in self.stats.items() if k.startswith('prov_')]
        for prov, count in sorted(provinces, key=lambda x: x[1], reverse=True)[:10]:
            print(f"   - {prov}: {count}")

# SIGUE EN LA PARTE 2...
# =============================================================================
# DETECTOR DE CONTENIDO EN VIVO OPTIMIZADO
# =============================================================================

class LiveContentDetector:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self._setup_patterns()
    def _setup_patterns(self):
        self.live_keywords = {
            'en vivo': 10, 'directo': 10, 'stream': 8, 'streaming': 8,
            'transmisión': 8, 'transmito': 8, 'transmitiendo': 8,
            'vivo': 6, 'envivo': 8, 'endirecto': 8,
            'live': 8, 'livestream': 10, 'live stream': 10,
            'broadcast': 6, 'broadcasting': 6,
            'charlando': 4, 'cocinando': 4, 'reaccionando': 4,
            'jugando': 4, 'cantando': 4, 'dibujando': 4,
            'enseñando': 4, 'practicando': 4, 'mostrando': 4,
            'chat': 5, 'superchat': 6, 'super chat': 6,
            'donaciones': 5, 'miembros': 4, 'suscriptores': 3,
            'saludos': 3, 'preguntas': 3, 'respondo': 4
        }
        self.streaming_platforms = {
            'twitch': 8, 'youtube live': 8, 'facebook gaming': 6,
            'instagram live': 6, 'tiktok live': 6, 'kick': 5,
            'discord': 4, 'telegram': 3
        }
        self.streaming_software = {
            'obs': 8, 'obs studio': 8, 'streamlabs': 8,
            'xsplit': 6, 'nvidia broadcast': 5, 'streamyard': 6,
            'restream': 6, 'wirecast': 5
        }
        self.schedule_patterns = [
            r'(?:lunes|martes|miércoles|jueves|viernes|sábado|domingo)',
            r'\d+\s*(?:hs|hrs|horas|pm|am)',
            r'\d+:\d+',
            r'todos los días',
            r'de lunes a',
            r'horario(?:s)?',
            r'schedule'
        ]
    def analyze_channel(self, channel_data: dict, videos_data: List[dict] = None) -> Dict:
        snippet = channel_data.get('snippet', {})
        title = snippet.get('title', '').lower()
        description = snippet.get('description', '').lower()
        score = 0
        indicators = []
        title_score = self._analyze_title(title)
        score += title_score
        if title_score > 0:
            indicators.append(f"título_indicativo_{title_score}")
        desc_score, desc_indicators = self._analyze_description(description)
        score += desc_score
        indicators.extend(desc_indicators)
        if videos_data:
            video_score, video_indicators = self._analyze_videos(videos_data)
            score += video_score
            indicators.extend(video_indicators)
        certainty = min(100, score * 2)
        return {
            'is_live_content': certainty >= Config.MIN_CERTAINTY_STREAMING,
            'certainty': certainty,
            'score': score,
            'indicators': indicators,
            'live_video_count': len([i for i in indicators if 'video_live' in i])
        }
    def _analyze_title(self, title: str) -> int:
        score = 0
        for keyword, points in self.live_keywords.items():
            if keyword in title:
                score += points // 2
        return min(score, 15)
    def _analyze_description(self, description: str) -> Tuple[int, List[str]]:
        score = 0
        indicators = []
        for keyword, points in self.live_keywords.items():
            if keyword in description:
                score += points
                indicators.append(f"keyword_{keyword}")
        for platform, points in self.streaming_platforms.items():
            if platform in description:
                score += points
                indicators.append(f"platform_{platform}")
        for software, points in self.streaming_software.items():
            if software in description:
                score += points
                indicators.append(f"software_{software}")
        schedule_count = 0
        for pattern in self.schedule_patterns:
            if re.search(pattern, description):
                schedule_count += 1
        if schedule_count > 0:
            score += schedule_count * 5
            indicators.append(f"schedule_found_{schedule_count}")
        if re.search(r'twitch\.tv/\w+', description):
            score += 15
            indicators.append("twitch_url")
        return score, indicators
    def _analyze_videos(self, videos: List[dict]) -> Tuple[int, List[str]]:
        score = 0
        indicators = []
        live_count = 0
        for video in videos[:5]:
            video_title = video.get('snippet', {}).get('title', '').lower()
            video_score = 0
            for keyword, points in self.live_keywords.items():
                if keyword in video_title:
                    video_score += points
            if video_score > 10:
                live_count += 1
                indicators.append(f"video_live_{live_count}")
            score += min(video_score, 20)
        if live_count >= 3:
            score += 20
            indicators.append("multiple_live_videos")
        return score, indicators

# =============================================================================
# DETECTOR DE ARGENTINIDAD ESTRICTO
# =============================================================================

class ArgentineDetector:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self._setup_patterns()
    def _setup_patterns(self):
        self.voseo_patterns = [
            r'\bvos\s+(?:tenés|sabés|querés|podés|andás|venís|hacés|decís|sos|estás)\b',
            r'\b(?:tenés|sabés|querés|podés|andás|venís|hacés|decís)\s+(?:que|vos)\b',
            r'\bche\s+(?:vos|boludo|loco|flaco)\b',
            r'\bdale\s+che\b',
            r'\bno\s+te\s+hagás\b'
        ]
        self.argentine_expressions = {
            'che': 10, 'boludo': 12, 'boluda': 12, 'chabón': 10,
            'flaco': 8, 'loco': 6, 'capo': 8, 'genio': 6,
            'gil': 8, 'pibe': 10, 'piba': 10, 'wacho': 10,
            're': 8, 'copado': 10, 'piola': 10, 'zarpado': 12,
            'groso': 10, 'grosso': 10, 'crack': 6, 'ídolo': 6,
            'al pedo': 12, 'de una': 10, 'una banda': 10,
            'un montón': 8, 'bárbaro': 8, 'mortal': 8,
            'flashear': 10, 'chamuyar': 12, 'joder': 8,
            'laburar': 10, 'laburo': 10, 'guita': 10,
            'bondi': 12, 'cole': 8, 'facu': 8,
            'birra': 8, 'escabio': 10, 'pucho': 10,
            'mate': 10, 'asado': 10, 'empanadas': 8
        }
        self.location_keywords = {}
        for prov in Config.PROVINCIAS_ARGENTINAS:
            self.location_keywords[prov.lower()] = 15
        for city in Config.CIUDADES_ARGENTINAS:
            self.location_keywords[city.lower()] = 12
        self.country_exclusions = {
            'españa': ['españa', 'español de españa', 'madrid', 'barcelona', 'valencia', 'sevilla'],
            'méxico': ['méxico', 'mexicano', 'cdmx', 'guadalajara', 'monterrey', 'chilango'],
            'colombia': ['colombia', 'colombiano', 'bogotá', 'medellín', 'cali', 'parcero'],
            'chile': ['chile', 'chileno', 'santiago de chile', 'weon', 'weón', 'po'],
            'perú': ['perú', 'peruano', 'lima', 'cusco', 'causa', 'pe'],
            'uruguay': ['uruguay', 'uruguayo', 'montevideo', 'bo', 'ta'],
            'venezuela': ['venezuela', 'venezolano', 'caracas', 'maracaibo', 'pana']
        }
    def analyze_channel(self, channel_data: dict, videos_data: List[dict] = None) -> Dict:
        snippet = channel_data.get('snippet', {})
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        country = snippet.get('country', '')
        full_text = f"{title} {description}".lower()
        other_country = self._check_other_countries(full_text)
        if other_country:
            return {
                'is_argentine': False,
                'certainty': 0,
                'country': other_country,
                'reason': f'Canal de {other_country}',
                'indicators': []
            }
        score = 0
        indicators = []
        method = 'cultural_analysis'
        if country == 'AR':
            score += 30
            indicators.append('metadata_AR')
            method = 'explicit_country'
        elif country and country != 'AR':
            score -= 50
            indicators.append(f'metadata_{country}')
        for code, prov in Config.CODIGOS_ARGENTINOS.items():
            if re.search(rf'\b{code}\b', full_text.upper()):
                score += 20
                indicators.append(f'codigo_{code}')
                province = prov
                if method == 'cultural_analysis':
                    method = 'local_code'
                break
        argentina_mentions = ['argentina', 'argentino', 'argentinos', 'argentinas', 'arg', '🇦🇷']
        for mention in argentina_mentions:
            if mention in full_text:
                score += 25
                indicators.append(f'menciona_{mention}')
                if method == 'cultural_analysis':
                    method = 'explicit_country'
        voseo_count = 0
        for pattern in self.voseo_patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            voseo_count += len(matches)
        if voseo_count > 0:
            score += min(voseo_count * 15, 60)
            indicators.append(f'voseo_{voseo_count}')
            if method == 'cultural_analysis' and voseo_count >= 2:
                method = 'voseo_patterns'
        modismos_found = []
        for modismo, points in self.argentine_expressions.items():
            if re.search(rf'\b{modismo}\b', full_text):
                score += points
                modismos_found.append(modismo)
        if modismos_found:
            indicators.append(f'modismos_{len(modismos_found)}')
        locations_found = []
        province = "Argentina"
        for location, points in self.location_keywords.items():
            if location in full_text:
                score += points
                locations_found.append(location)
        if locations_found:
            indicators.append(f'locations_{len(locations_found)}')
            for loc in locations_found:
                if loc.title() in Config.PROVINCIAS_ARGENTINAS:
                    province = loc.title()
                    if method == 'cultural_analysis':
                        method = 'province_mention'
                    break
                elif loc.title() in Config.CIUDADES_ARGENTINAS:
                    city_to_province = {
                        'rosario': 'Santa Fe', 'la plata': 'Buenos Aires', 'mar del plata': 'Buenos Aires',
                        'córdoba': 'Córdoba', 'mendoza': 'Mendoza', 'tucumán': 'Tucumán', 'salta': 'Salta',
                        'santa fe': 'Santa Fe', 'neuquén': 'Neuquén', 'posadas': 'Misiones', 'resistencia': 'Chaco',
                        'corrientes': 'Corrientes', 'paraná': 'Entre Ríos', 'bahía blanca': 'Buenos Aires'
                    }
                    province = city_to_province.get(loc, 'Argentina')
                    break
        if videos_data:
            video_score = self._analyze_video_content(videos_data)
            score += video_score
            if video_score > 0:
                indicators.append(f'video_content_{video_score}')
        certainty = min(100, score)
        return {
            'is_argentine': certainty >= Config.MIN_CERTAINTY_ARGENTINA,
            'certainty': certainty,
            'province': province,
            'indicators': indicators,
            'score': score,
            'method': method
        }
    def _check_other_countries(self, text: str) -> Optional[str]:
        for country, keywords in self.country_exclusions.items():
            country_patterns = [
                f'soy de {country}', f'desde {country}', f'vivo en {country}',
                f'{country}no', f'{country}na'
            ]
            for pattern in country_patterns:
                if pattern in text:
                    return country
            keyword_count = sum(1 for kw in keywords if kw in text)
            if keyword_count >= 3:
                return country
        return None
    def _analyze_video_content(self, videos: List[dict]) -> int:
        score = 0
        for video in videos[:3]:
            video_text = video.get('snippet', {}).get('title', '').lower()
            video_text += ' ' + video.get('snippet', {}).get('description', '')[:200].lower()
            for pattern in self.voseo_patterns[:3]:
                if re.search(pattern, video_text):
                    score += 10
                    break
            for modismo in list(self.argentine_expressions.keys())[:10]:
                if modismo in video_text:
                    score += 5
                    break
        return score

# SIGUE EN LA PARTE 3...
# =============================================================================
# CLASIFICADOR DE CATEGORÍAS
# =============================================================================

class ContentCategorizer:
    def __init__(self):
        self.categories = {
            'Gaming': {
                'keywords': ['gaming', 'gamer', 'juegos', 'gameplay', 'jugando', 'partida', 'stream gaming', 'videojuegos', 'ps5', 'xbox', 'minecraft', 'fortnite', 'lol', 'valorant', 'warzone'],
                'weight': 1.0
            },
            'Música': {
                'keywords': ['música', 'music', 'cantante', 'cantar', 'cantando', 'concierto', 'show', 'banda', 'artista', 'covers', 'acústico', 'instrumental', 'dj', 'mix', 'remix'],
                'weight': 1.0
            },
            'Cocina': {
                'keywords': ['cocina', 'cocinando', 'recetas', 'chef', 'cooking', 'gastronomía', 'comida', 'platos', 'ingredientes', 'restaurant', 'parrilla', 'asado', 'empanadas'],
                'weight': 1.0
            },
            'Charlas/Podcast': {
                'keywords': ['podcast', 'charla', 'charlando', 'entrevista', 'conversación', 'debate', 'opinión', 'hablando', 'invitado', 'tertulia', 'mesa', 'análisis'],
                'weight': 1.0
            },
            'Educativo': {
                'keywords': ['educativo', 'enseñando', 'tutorial', 'clase', 'curso', 'aprender', 'explicando', 'lección', 'profesor', 'estudiante', 'universidad', 'escuela'],
                'weight': 1.0
            },
            'Deportes': {
                'keywords': ['deporte', 'deportivo', 'fútbol', 'básquet', 'tenis', 'rugby', 'gym', 'fitness', 'entreno', 'partido', 'equipo', 'club', 'torneo'],
                'weight': 1.0
            },
            'Arte/Creatividad': {
                'keywords': ['arte', 'artista', 'dibujando', 'pintura', 'diseño', 'creatividad', 'ilustración', 'dibujo', 'manualidades', 'craft', 'creativo', 'artesanía'],
                'weight': 1.0
            },
            'Tecnología': {
                'keywords': ['tecnología', 'tech', 'programación', 'código', 'software', 'hardware', 'computadora', 'móvil', 'desarrollo', 'innovación', 'gadget', 'review tech'],
                'weight': 1.0
            },
            'Estilo de Vida': {
                'keywords': ['lifestyle', 'vida', 'vlog', 'día', 'rutina', 'viajes', 'aventura', 'experiencia', 'tips', 'consejos', 'bienestar', 'salud', 'belleza'],
                'weight': 0.8
            },
            'Entretenimiento': {
                'keywords': ['entretenimiento', 'show', 'espectáculo', 'humor', 'comedia', 'reacciones', 'react', 'challenges', 'retos', 'diversión', 'juegos'],
                'weight': 0.7
            }
        }
    def categorize(self, channel_data: dict, videos_data: List[dict] = None) -> str:
        snippet = channel_data.get('snippet', {})
        text = snippet.get('title', '').lower()
        text += ' ' + snippet.get('description', '').lower()
        if videos_data:
            for video in videos_data[:5]:
                text += ' ' + video.get('snippet', {}).get('title', '').lower()
        scores = defaultdict(float)
        for category, data in self.categories.items():
            for keyword in data['keywords']:
                if keyword in text:
                    scores[category] += data['weight']
        if not scores:
            return 'Entretenimiento'
        return max(scores.items(), key=lambda x: x[1])[0]

# =============================================================================
# CLIENTE YOUTUBE OPTIMIZADO
# =============================================================================

class OptimizedYouTubeClient:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.quota_tracker = QuotaTracker()
        self.cache = APICache()
        self.api_keys = [
            Config.YOUTUBE_API_KEY,
            Config.API_KEY_2
        ]
        self.api_keys = [k for k in self.api_keys if k]
        if not self.api_keys:
            raise Exception("❌ No hay claves de API configuradas")
        self.current_key_index = 0
        self.youtube = None
        self.apis_exhausted = False
        self.channels_analyzed_today = 0
        if HAS_YOUTUBE_API:
            self.youtube = build('youtube', 'v3', developerKey=self.api_keys[0])
            self.mode = 'production'
            self.logger.info(f"✅ Modo producción con {len(self.api_keys)} APIs")
        else:
            self.mode = 'simulation'
            self.logger.warning("⚠️ Modo simulación")
    def rotate_api_key(self) -> bool:
        if self.current_key_index + 1 < len(self.api_keys):
            self.current_key_index += 1
            self.youtube = build('youtube', 'v3', developerKey=self.api_keys[self.current_key_index])
            self.logger.info(f"🔄 Rotando a API #{self.current_key_index + 1}")
            return True
        return False
    def can_continue(self) -> bool:
        if self.apis_exhausted:
            return False
        if self.channels_analyzed_today >= Config.MAX_CHANNELS_PER_DAY:
            self.logger.warning(f"⚠️ Límite diario alcanzado: {Config.MAX_CHANNELS_PER_DAY} canales")
            return False
        remaining = self.quota_tracker.get_remaining()
        if remaining < 1000:
            self.logger.warning(f"⚠️ Cuota muy baja: {remaining}")
            return False
        return True
    def search_channels(self, query: str, max_pages: int = None) -> List[dict]:
        if self.mode == 'simulation':
            return self._simulate_search(query)
        if max_pages is None:
            max_pages = Config.MAX_PAGES_PER_SEARCH
        channels = []
        page_token = None
        for page in range(max_pages):
            if not self.quota_tracker.can_use_quota(Config.COST_SEARCH):
                self.logger.warning("⚠️ Cuota insuficiente para búsqueda")
                break
            cache_key = f"search_{query}_p{page}"
            cached = self.cache.get(cache_key)
            if cached:
                channels.extend(cached['items'])
                page_token = cached.get('nextPageToken')
                if not page_token:
                    break
                continue
            try:
                response = self.youtube.search().list(
                    q=query,
                    part='snippet',
                    type='channel',
                    maxResults=Config.MAX_RESULTS_PER_PAGE,
                    pageToken=page_token,
                    regionCode='AR',
                    relevanceLanguage='es'
                ).execute()
                self.quota_tracker.use_quota(Config.COST_SEARCH)
                self.cache.set(cache_key, response)
                items = response.get('items', [])
                channels.extend(items)
                page_token = response.get('nextPageToken')
                if not page_token or len(items) < Config.MAX_RESULTS_PER_PAGE:
                    break
                time.sleep(0.5)
            except HttpError as e:
                if e.resp.status == 403 and 'quotaExceeded' in str(e):
                    if not self.rotate_api_key():
                        self.apis_exhausted = True
                        self.logger.error("❌ Todas las APIs agotadas")
                        break
                else:
                    self.logger.error(f"Error en búsqueda: {e}")
                    break
        return channels
    def get_channel_details(self, channel_id: str) -> Optional[dict]:
        if self.mode == 'simulation':
            return self._simulate_channel_details(channel_id)
        cache_key = f"channel_{channel_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        if not self.quota_tracker.can_use_quota(Config.COST_CHANNEL_DETAILS):
            return None
        try:
            response = self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            ).execute()
            self.quota_tracker.use_quota(Config.COST_CHANNEL_DETAILS)
            if response.get('items'):
                channel = response['items'][0]
                self.cache.set(cache_key, channel)
                return channel
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in str(e):
                if not self.rotate_api_key():
                    self.apis_exhausted = True
            self.logger.error(f"Error obteniendo canal: {e}")
        return None
    def get_recent_videos(self, channel_id: str) -> List[dict]:
        if self.mode == 'simulation':
            return self._simulate_videos(channel_id)
        cache_key = f"videos_{channel_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        if not self.quota_tracker.can_use_quota(Config.COST_VIDEO_LIST):
            return []
        try:
            response = self.youtube.search().list(
                channelId=channel_id,
                part='snippet',
                order='date',
                type='video',
                maxResults=5
            ).execute()
            self.quota_tracker.use_quota(Config.COST_VIDEO_LIST)
            videos = response.get('items', [])
            self.cache.set(cache_key, videos)
            return videos
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in str(e):
                if not self.rotate_api_key():
                    self.apis_exhausted = True
            self.logger.error(f"Error obteniendo videos: {e}")
        return []
    def _simulate_search(self, query: str) -> List[dict]:
        import random
        channels = []
        for i in range(20):
            channels.append({
                'id': {'channelId': f'sim_{query}_{i}'},
                'snippet': {
                    'title': f'Canal {random.choice(["Streaming", "En Vivo", "Directo"])} {query} #{i}',
                    'description': f'Hola che! Transmito {query} todos los días a las 20hs Argentina'
                }
            })
        return channels
    def _simulate_channel_details(self, channel_id: str) -> dict:
        import random
        return {
            'id': channel_id,
            'snippet': {
                'title': f'Canal Simulado {channel_id[-4:]}',
                'description': 'Streamer argentino, transmito en vivo de lunes a viernes',
                'country': 'AR'
            },
            'statistics': {
                'subscriberCount': str(random.randint(1000, 100000)),
                'videoCount': str(random.randint(50, 1000))
            }
        }
    def _simulate_videos(self, channel_id: str) -> List[dict]:
        videos = []
        for i in range(5):
            videos.append({
                'snippet': {
                    'title': f'🔴 EN VIVO - Stream del día #{i+1}',
                    'description': 'Charlando con los pibes'
                }
            })
        return videos

# =============================================================================
# GESTIÓN DE CUOTA MEJORADA
# =============================================================================

class QuotaTracker:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.quota_file = Config.QUOTA_TRACKER
        self.load_quota()
    def load_quota(self):
        if self.quota_file.exists():
            try:
                with open(self.quota_file, 'r') as f:
                    data = json.load(f)
                today = datetime.now().strftime('%Y-%m-%d')
                if data.get('date') == today:
                    self.used_today = data.get('used', 0)
                    self.api_quotas = data.get('api_quotas', {})
                else:
                    self.reset_daily_quota()
            except:
                self.reset_daily_quota()
        else:
            self.reset_daily_quota()
    def reset_daily_quota(self):
        self.used_today = 0
        self.api_quotas = {'api_1': 0, 'api_2': 0}
        self.save_quota()
    def save_quota(self):
        data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'used': self.used_today,
            'api_quotas': self.api_quotas,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.quota_file, 'w') as f:
            json.dump(data, f, indent=2)
    def can_use_quota(self, cost: int) -> bool:
        remaining = self.get_remaining()
        return remaining >= cost
    def use_quota(self, cost: int, api_index: int = 0):
        self.used_today += cost
        api_key = f'api_{api_index + 1}'
        self.api_quotas[api_key] = self.api_quotas.get(api_key, 0) + cost
        if self.used_today % 30 == 0:
            self.save_quota()
        if self.used_today > Config.QUOTA_WARNING_THRESHOLD:
            remaining = self.get_remaining()
            self.logger.warning(f"⚠️ Cuota alta: {self.used_today}/{Config.MAX_DAILY_QUOTA} (Restante: {remaining})")
    def get_remaining(self) -> int:
        return (Config.MAX_DAILY_QUOTA - Config.SAFETY_BUFFER) - self.used_today

# =============================================================================
# CACHE INTELIGENTE
# =============================================================================

class APICache:
    def __init__(self):
        self.cache_file = Config.API_CACHE
        self.cache_data = self.load_cache()
        self.cache_duration = 86400 * 3  # 3 días
    def load_cache(self) -> dict:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    def save_cache(self):
        self.clean_old_entries()
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache_data, f)
    def get(self, key: str) -> Optional[dict]:
        if key in self.cache_data:
            entry = self.cache_data[key]
            timestamp = entry.get('timestamp', 0)
            if time.time() - timestamp < self.cache_duration:
                return entry.get('data')
        return None
    def set(self, key: str, data: dict):
        self.cache_data[key] = {
            'data': data,
            'timestamp': time.time()
        }
        if len(self.cache_data) % 20 == 0:
            self.save_cache()
    def clean_old_entries(self):
        current_time = time.time()
        self.cache_data = {
            k: v for k, v in self.cache_data.items()
            if current_time - v.get('timestamp', 0) < self.cache_duration
        }

# =============================================================================
# GESTOR DE DATOS: ELIMINADOR Y FILTRO DE DUPLICADOS
# =============================================================================

class DataManager:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.processed_channels = self.load_processed_channels()
        self.rejected_channels = self.load_rejected_channels()
        self.remove_existing_duplicates()   # NUEVO: limpieza al iniciar
    def load_processed_channels(self) -> Set[str]:
        if Config.PROCESSED_CHANNELS.exists():
            try:
                with open(Config.PROCESSED_CHANNELS, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    def load_rejected_channels(self) -> Dict[str, str]:
        if Config.REJECTED_CHANNELS.exists():
            try:
                with open(Config.REJECTED_CHANNELS, 'rb') as f:
                    return pickle.load(f)
            except:
                return {}
        return {}
    def save_state(self):
        with open(Config.PROCESSED_CHANNELS, 'wb') as f:
            pickle.dump(self.processed_channels, f)
        with open(Config.REJECTED_CHANNELS, 'wb') as f:
            pickle.dump(self.rejected_channels, f)
    def is_processed(self, channel_id: str) -> bool:
        return channel_id in self.processed_channels or channel_id in self.rejected_channels
    def mark_processed(self, channel_id: str, rejected: bool = False, reason: str = ""):
        self.processed_channels.add(channel_id)
        if rejected:
            self.rejected_channels[channel_id] = reason
        if len(self.processed_channels) % 50 == 0:
            self.save_state()
    def save_channel(self, data: StreamerData):
        file_path = Path("data/streamers_argentinos.csv")
        file_exists = file_path.exists()
        canal_id = data.canal_id
        # Leer IDs existentes sólo si el archivo existe, para evitar duplicados
        ids_existentes = set()
        if file_exists:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ids_existentes.add(row['canal_id'])
        if canal_id in ids_existentes:
            return
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            fieldnames = [
                'canal_id', 'nombre_canal', 'categoria', 'provincia', 'ciudad',
                'suscriptores', 'certeza', 'metodo_deteccion', 'indicadores_argentinidad',
                'url', 'fecha_deteccion', 'ultima_actividad', 'tiene_streaming',
                'descripcion', 'pais_detectado', 'videos_analizados'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            row = asdict(data)
            row['indicadores_argentinidad'] = ', '.join(row['indicadores_argentinidad'])
            row['descripcion'] = row['descripcion'][:500]
            writer.writerow(row)
    def remove_existing_duplicates(self):
        file_path = Path("data/streamers_argentinos.csv")
        if not file_path.exists():
            return
        with open(file_path, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        seen = set()
        dedup_rows = []
        for row in rows:
            key = row.get('canal_id')
            if key and key not in seen:
                seen.add(key)
                dedup_rows.append(row)
        if len(dedup_rows) != len(rows):
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(dedup_rows)
            print(f"🧹 Se eliminaron {len(rows)-len(dedup_rows)} duplicados del CSV.")

# =============================================================================
# PROCESADOR PRINCIPAL OPTIMIZADO
# =============================================================================

class ChannelProcessor:
    def __init__(self, youtube_client, logger_system):
        self.youtube = youtube_client
        self.logger_system = logger_system
        self.logger = logging.getLogger('StreamingArgentina')
        self.argentine_detector = ArgentineDetector()
        self.live_detector = LiveContentDetector()
        self.categorizer = ContentCategorizer()
        self.data_manager = DataManager()
    def process_channel(self, channel_snippet: dict) -> Optional[StreamerData]:
        channel_id = (channel_snippet.get('id', {}).get('channelId') or channel_snippet.get('id'))
        if not channel_id:
            return None
        if self.data_manager.is_processed(channel_id):
            return None
        try:
            self.youtube.channels_analyzed_today += 1
            channel_details = self.youtube.get_channel_details(channel_id)
            if not channel_details:
                self.data_manager.mark_processed(channel_id, True, "sin_detalles")
                return None
            stats = channel_details.get('statistics', {})
            subscribers = int(stats.get('subscriberCount', 0))
            if subscribers < Config.MIN_SUBSCRIBERS:
                self.logger_system.log_channel_rejected(
                    channel_id, "pocos_suscriptores", f"({subscribers})"
                )
                self.data_manager.mark_processed(channel_id, True, "pocos_suscriptores")
                return None
            arg_analysis = self.argentine_detector.analyze_channel(channel_details)
            if not arg_analysis['is_argentine']:
                reason = arg_analysis.get('reason', 'no_argentino')
                country = arg_analysis.get('country', 'desconocido')
                self.logger_system.log_channel_rejected(
                    channel_id, reason, f"({country})"
                )
                self.data_manager.mark_processed(channel_id, True, reason)
                return None
            live_analysis = self.live_detector.analyze_channel(channel_details)
            if live_analysis['certainty'] < Config.MIN_CERTAINTY_STREAMING:
                videos = self.youtube.get_recent_videos(channel_id)
                if videos:
                    live_analysis = self.live_detector.analyze_channel(channel_details, videos)
            if not live_analysis['is_live_content']:
                self.logger_system.log_channel_rejected(
                    channel_id, "sin_contenido_vivo", f"(certeza: {live_analysis['certainty']:.0f}%)"
                )
                self.data_manager.mark_processed(channel_id, True, "sin_contenido_vivo")
                return None
            videos = self.youtube.get_recent_videos(channel_id) if not 'videos' in locals() else videos
            category = self.categorizer.categorize(channel_details, videos)
            snippet = channel_details.get('snippet', {})
            streamer_data = StreamerData(
                canal_id=channel_id,
                nombre_canal=snippet.get('title', 'Sin nombre'),
                categoria=category,
                provincia=arg_analysis.get('province', 'Argentina'),
                ciudad='Por determinar',
                suscriptores=subscribers,
                certeza=arg_analysis['certainty'],
                metodo_deteccion=arg_analysis.get('method', 'analysis_completo'),
                indicadores_argentinidad=arg_analysis.get('indicators', []),
                url=f"https://youtube.com/channel/{channel_id}",
                fecha_deteccion=datetime.now().strftime('%Y-%m-%d'),
                ultima_actividad=snippet.get('publishedAt', ''),
                tiene_streaming=True,
                descripcion=snippet.get('description', ''),
                pais_detectado='Argentina',
                videos_analizados=len(videos) if 'videos' in locals() else 0
            )
            self.data_manager.save_channel(streamer_data)
            self.data_manager.mark_processed(channel_id)
            self.logger_system.log_channel_found(streamer_data)
            return streamer_data
        except Exception as e:
            self.logger.error(f"Error procesando canal {channel_id}: {e}")
            self.data_manager.mark_processed(channel_id, True, "error_procesamiento")
            return None

# SIGUE EN LA PARTE 4...
# =============================================================================
# ESTRATEGIAS DE BÚSQUEDA OPTIMIZADAS
# =============================================================================

class SearchStrategies:
    """Estrategias de búsqueda optimizadas para encontrar canales en vivo argentinos"""
    @staticmethod
    def get_daily_queries() -> List[Tuple[str, int]]:
        day_of_year = datetime.now().timetuple().tm_yday
        phase = (day_of_year % 4) + 1
        if phase == 1:
            return [
                ('argentina en vivo', 2),
                ('argentina directo', 2),
                ('streaming argentina', 2),
                ('stream argentino', 2),
                ('transmisión argentina', 1),
                ('transmito argentina', 1),
                ('vivo argentina', 1),
                ('charlando argentina', 1),
                ('cocinando argentina', 1),
                ('cantando argentina', 1)
            ]
        elif phase == 2:
            provinces = ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'La Plata']
            queries = []
            for prov in provinces:
                queries.extend([
                    (f'{prov} en vivo', 2),
                    (f'{prov} streaming', 1),
                    (f'{prov} directo', 1)
                ])
            return queries
        elif phase == 3:
            return [
                ('che streaming', 2),
                ('che en vivo', 2),
                ('boludo streaming', 1),
                ('transmito che', 1),
                ('vivo buenos aires che', 1),
                ('argentina vivo música', 1),
                ('argentina vivo cocina', 1),
                ('argentina vivo charla', 1),
                ('podcast argentina vivo', 1),
                ('argentina twitch', 1)
            ]
        else:  # phase 4
            return [
                ('gaming argentina vivo', 2),
                ('música argentina vivo', 2),
                ('cocina argentina directo', 1),
                ('charla argentina vivo', 1),
                ('podcast argentina streaming', 1),
                ('educativo argentina vivo', 1),
                ('arte argentina directo', 1),
                ('deportes argentina vivo', 1),
                ('fitness argentina streaming', 1),
                ('tecnología argentina vivo', 1)
            ]

# =============================================================================
# MOTOR PRINCIPAL
# =============================================================================

class StreamingArgentinaEngine:
    def __init__(self):
        Config.setup_directories()
        self.logger_system = Logger()
        self.logger = self.logger_system.logger
        self.youtube = OptimizedYouTubeClient()
        self.processor = ChannelProcessor(self.youtube, self.logger_system)
        self.channels_found_today = 0
    def execute_daily_search(self):
        self.logger.info("="*80)
        self.logger.info("🚀 INICIANDO BÚSQUEDA DE CANALES EN VIVO ARGENTINOS")
        self.logger.info(f"📅 Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}")
        self.logger.info(f"🔋 Cuota disponible: {self.youtube.quota_tracker.get_remaining():,}")
        self.logger.info("="*80)
        queries = SearchStrategies.get_daily_queries()
        self.logger.info(f"📋 Queries programadas: {len(queries)}")
        for query, max_pages in queries:
            if not self.youtube.can_continue():
                self.logger.warning("⚠️ Deteniendo búsqueda - Límites alcanzados")
                break
            self.logger.info(f"\n🔍 Buscando: '{query}' (máx {max_pages} páginas)")
            try:
                channels = self.youtube.search_channels(query, max_pages)
                if not channels:
                    self.logger.info(f"   No se encontraron resultados")
                    continue
                self.logger.info(f"   📊 {len(channels)} canales encontrados")
                processed = 0
                for channel in channels:
                    if not self.youtube.can_continue():
                        break
                    result = self.processor.process_channel(channel)
                    if result:
                        self.channels_found_today += 1
                    processed += 1
                    if processed % 10 == 0:
                        self.logger_system.log_quota_status(
                            self.youtube.quota_tracker.used_today,
                            Config.MAX_DAILY_QUOTA
                        )
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error procesando query '{query}': {e}")
                continue
        self.processor.data_manager.save_state()
        self.youtube.cache.save_cache()
        self.youtube.quota_tracker.save_quota()
        self.logger_system.print_summary()
        return self.channels_found_today

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    try:
        if not Config.YOUTUBE_API_KEY:
            print("❌ ERROR: No hay API key configurada")
            print("Configura tu API key:")
            print("  export YOUTUBE_API_KEY='tu_api_key'")
            return
        print("\n" + "="*80)
        print("🎯 CARTOGRAFÍA DE CANALES EN VIVO ARGENTINOS")
        print("="*80)
        print("✅ Buscando SOLO canales que hacen contenido EN VIVO")
        print("✅ Verificando que sean realmente argentinos")
        print("✅ Sin importar la temática del contenido")
        print("✅ Optimizado para no exceder cuota diaria")
        print("="*80 + "\n")
        engine = StreamingArgentinaEngine()
        channels_found = engine.execute_daily_search()
        print(f"\n✅ Ejecución completada: {channels_found} canales encontrados")
        print(f"📁 Datos guardados en: {Config.STREAMERS_CSV}")
        print(f"   ({Config.STREAMERS_CSV.absolute()})")
    except KeyboardInterrupt:
        print("\n⚠️ Ejecución interrumpida por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()




