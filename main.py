#!/usr/bin/env python3
"""
CARTOGRAFÍA STREAMING ARGENTINO - Solo canales que mencionan explícitamente Argentina y hacen streaming real
"""

import os
import json
import csv
import re
import time
import pickle
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from collections import defaultdict

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
    SAFETY_BUFFER = 2000
    QUOTA_WARNING_THRESHOLD = 16000
    COST_SEARCH = 100
    COST_CHANNEL_DETAILS = 3
    COST_VIDEO_LIST = 3
    MIN_SUBSCRIBERS = 500
    MIN_CERTAINTY_ARGENTINA = 75
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / 'data'
    LOGS_DIR = BASE_DIR / 'logs'
    CACHE_DIR = BASE_DIR / 'cache'
    STREAMERS_CSV = DATA_DIR / 'streamers_argentinos.csv'
    PROCESSED_CHANNELS = CACHE_DIR / 'processed_channels.pkl'
    API_CACHE = CACHE_DIR / 'api_cache.json'
    QUOTA_TRACKER = CACHE_DIR / 'quota_tracker.json'
    PROVINCIAS_ARGENTINAS = {
        "Buenos Aires", "CABA", "Córdoba", "Santa Fe", "Mendoza", "Tucumán",
        "Salta", "Entre Ríos", "Misiones", "Chaco", "Corrientes",
        "Santiago del Estero", "Jujuy", "Neuquén", "Río Negro",
        "Formosa", "Chubut", "San Luis", "Catamarca", "La Rioja",
        "San Juan", "Santa Cruz", "Tierra del Fuego", "La Pampa"
    }
    CODIGOS_ARGENTINOS = {
        "MZA": "Mendoza", "COR": "Córdoba", "ROS": "Santa Fe",
        "MDQ": "Buenos Aires", "BRC": "Río Negro", "SLA": "Salta",
        "TUC": "Tucumán", "NQN": "Neuquén", "USH": "Tierra del Fuego",
        "JUJ": "Jujuy", "SFN": "Santa Fe", "CTC": "Catamarca",
        "LRJ": "La Rioja", "FSA": "Formosa", "SGO": "Santiago del Estero"
    }
    @classmethod
    def setup_directories(cls):
        for directory in [cls.DATA_DIR, cls.LOGS_DIR, cls.CACHE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

class Logger:
    def __init__(self):
        self.setup_logging()
        self.stats = defaultdict(int)
    def setup_logging(self):
        log_file = Config.LOGS_DIR / f'streaming_{datetime.now():%Y%m%d}.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('StreamingArgentina')
    def phase_start(self, phase: int, description: str):
        self.logger.info(f"{'='*80}")
        self.logger.info(f"🎯 INICIANDO FASE {phase}: {description}")
        self.logger.info(f"{'='*80}")
    def streamer_found(self, streamer: StreamerData):
        self.logger.info(
            f"✅ ENCONTRADO: {streamer.nombre_canal} | "
            f"{streamer.provincia} | {streamer.suscriptores:,} subs | "
            f"Certeza: {streamer.certeza:.1f}% | "
            f"Método: {streamer.metodo_deteccion}"
        )
        self.stats['streamers_found'] += 1
        self.stats[f'provincia_{streamer.provincia}'] += 1
    def channel_rejected(self, channel_name: str, reason: str):
        self.logger.debug(f"❌ RECHAZADO: {channel_name} - {reason}")
        self.stats['channels_rejected'] += 1
        self.stats[f'rejected_{reason}'] += 1
    def quota_warning(self, used: int, total: int):
        percentage = (used / total) * 100
        self.logger.warning(
            f"⚠️  CUOTA API: {used:,}/{total:,} ({percentage:.1f}%) - "
            f"Restante: {total-used:,}"
        )
    def daily_summary(self):
        self.logger.info("\n📊 RESUMEN DIARIO:")
        self.logger.info(f"   Streamers encontrados: {self.stats['streamers_found']}")
        self.logger.info(f"   Canales rechazados: {self.stats['channels_rejected']}")
        provincias = [(k.replace('provincia_', ''), v) 
                      for k, v in self.stats.items() 
                      if k.startswith('provincia_')]
        if provincias:
            provincias.sort(key=lambda x: x[1], reverse=True)
            self.logger.info("   Top provincias:")
            for prov, count in provincias[:5]:
                self.logger.info(f"     - {prov}: {count}")

class ArgentineDetector:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self._setup_patterns()
    def _setup_patterns(self):
        self.explicit_mentions = [
            'argentina', 'argentino', 'argentinos', 'argentinas', 'arg 🇦🇷', '🇦🇷'
        ]
    def detect_explicit_argentina(self, text: str) -> Optional[Dict]:
        text_lower = text.lower()
        for mention in self.explicit_mentions:
            if mention in text_lower:
                return {
                    'method': 'explicit_country',
                    'is_argentine': True,
                    'confidence': 95,
                    'indicators': [f'menciona_{mention}']
                }
        for code, province in Config.CODIGOS_ARGENTINOS.items():
            if re.search(rf'\b{code.lower()}\b', text_lower):
                return {
                    'method': 'local_code',
                    'is_argentine': True,
                    'confidence': 92,
                    'province': province,
                    'indicators': [f'codigo_{code}']
                }
        for province in Config.PROVINCIAS_ARGENTINAS:
            if province.lower() in text_lower:
                return {
                    'method': 'province_mention',
                    'is_argentine': True,
                    'confidence': 88,
                    'province': province,
                    'indicators': [f'provincia_{province}']
                }
        return None
    def detect_other_countries(self, text: str) -> Optional[Dict]:
        text_lower = text.lower()
        country_exclusions = {
            'españa': ['españa', 'español', 'madrid', 'barcelona', 'valencia'],
            'mexico': ['méxico', 'mexicano', 'cdmx', 'guadalajara', 'monterrey'],
            'chile': ['chile', 'chileno', 'santiago de chile', 'valparaíso'],
            'colombia': ['colombia', 'colombiano', 'bogotá', 'medellín', 'cali'],
            'peru': ['perú', 'peruano', 'lima', 'cusco', 'arequipa'],
            'uruguay': ['uruguay', 'uruguayo', 'montevideo', 'punta del este'],
            'venezuela': ['venezuela', 'venezolano', 'caracas', 'maracaibo'],
            'ecuador': ['ecuador', 'ecuatoriano', 'quito', 'guayaquil'],
            'paraguay': ['paraguay', 'paraguayo', 'asunción', 'ciudad del este'],
            'bolivia': ['bolivia', 'boliviano', 'la paz', 'santa cruz de la sierra']
        }
        for country, indicators in country_exclusions.items():
            matches = [ind for ind in indicators if ind in text_lower]
            if matches:
                return {
                    'method': 'other_country',
                    'is_argentine': False,
                    'confidence': 90,
                    'country': country,
                    'indicators': matches
                }
        return None
    def detect_region(self, text: str) -> Tuple[str, float]:
        text_lower = text.lower()
        regions = {
            'Buenos Aires': {
                'indicators': ['caba', 'capital federal', 'porteño', 'bonaerense', 'la plata'],
                'weight': 1.2
            },
            'Córdoba': {
                'indicators': ['córdoba', 'cordobés', 'fernet', 'cuarteto', 'la docta'],
                'weight': 1.1
            },
            'Mendoza': {
                'indicators': ['mendoza', 'mendocino', 'mza', 'vino', 'vendimia', 'aconcagua'],
                'weight': 1.1
            },
            'Santa Fe': {
                'indicators': ['rosario', 'santafesino', 'ros', 'paraná'],
                'weight': 1.0
            },
            'Patagonia': {
                'indicators': ['bariloche', 'patagonia', 'neuquén', 'ushuaia', 'calafate'],
                'weight': 1.0
            }
        }
        best_region = 'Argentina'
        best_score = 0
        for region, data in regions.items():
            score = sum(data['weight'] for ind in data['indicators'] if ind in text_lower)
            if score > best_score:
                best_score = score
                best_region = region
        confidence_boost = min(10, best_score * 5)
        return best_region, confidence_boost
    def analyze_channel(self, channel_data: Dict, videos_data: List[Dict] = None) -> Dict:
        snippet = channel_data.get('snippet', {})
        text_parts = [
            snippet.get('title', ''),
            snippet.get('description', ''),
            snippet.get('country', '')
        ]
        if videos_data:
            for video in videos_data[:10]:
                video_snippet = video.get('snippet', {})
                text_parts.extend([
                    video_snippet.get('title', ''),
                    video_snippet.get('description', '')[:200]
                ])
        full_text = ' '.join(text_parts)
        other_country = self.detect_other_countries(full_text)
        if other_country and other_country['confidence'] > 80:
            return {
                'is_argentine': False,
                'confidence': 0,
                'reason': f"Canal de {other_country['country']}",
                'method': 'country_exclusion',
                'indicators': other_country['indicators']
            }
        explicit = self.detect_explicit_argentina(full_text)
        if explicit:
            region, confidence_boost = self.detect_region(full_text)
            explicit['confidence'] = min(100, explicit['confidence'] + confidence_boost)
            explicit['province'] = explicit.get('province', region)
            return explicit
        return {
            'is_argentine': False,
            'confidence': 0,
            'reason': 'No mención explícita de Argentina en la descripción/título/country',
            'method': 'no_explicit_mention',
            'indicators': []
        }

class StreamingDetector:
    def __init__(self, youtube_client):
        self.youtube = youtube_client
        self.logger = logging.getLogger('StreamingArgentina')
    def check_streaming_capability(self, channel_data: dict) -> bool:
        channel_id = channel_data.get('id')
        if not channel_id:
            return False
        if getattr(self.youtube, 'all_apis_exhausted', False):
            self.logger.warning("⚠️ APIs agotadas - Saltando verificación de streaming")
            return True
        score = 0
        score += self._check_live_broadcasts(channel_id)
        score += self._check_live_keywords(channel_id)
        score += self._check_broadcast_content(channel_id)
        score += self._check_channel_description(channel_data)
        score += self._check_streaming_schedule(channel_data)
        score += self._check_streaming_platforms(channel_data)
        self.logger.debug(f"Canal {channel_id} - Total streaming score: {score}")
        return score >= 15
    def _check_live_broadcasts(self, channel_id: str) -> int:
        score = 0
        try:
            def api_call_live():
                return self.youtube.youtube.search().list(
                    channelId=channel_id,
                    part='snippet',
                    type='video',
                    eventType='live',
                    maxResults=1
                ).execute()
            response_live = self.youtube._call_with_rotation(api_call_live)
            self.youtube.quota_tracker.use_quota(3)
            if response_live.get('items'):
                score += 10
            def api_call_past():
                return self.youtube.youtube.search().list(
                    channelId=channel_id,
                    part='snippet',
                    type='video',
                    eventType='completed',
                    maxResults=3
                ).execute()
            response_past = self.youtube._call_with_rotation(api_call_past)
            self.youtube.quota_tracker.use_quota(3)
            if response_past.get('items'):
                score += 7
        except Exception as e:
            self.logger.warning(f"Error buscando transmisiones en vivo reales: {e}")
        return score
    def _check_live_keywords(self, channel_id: str) -> int:
        if not self.youtube.can_make_request(3):
            return 0
        if getattr(self.youtube, 'all_apis_exhausted', False):
            return 0
        live_keywords = [
            'stream', 'streaming', 'live', 'en vivo', 'directo',
            'transmisión', 'gameplay', 'jugando'
        ]
        score = 0
        try:
            for keyword in live_keywords[:3]:
                def api_call():
                    return self.youtube.youtube.search().list(
                        channelId=channel_id,
                        part='snippet',
                        q=keyword,
                        type='video',
                        maxResults=5,
                        order='date'
                    ).execute()
                response = self.youtube._call_with_rotation(api_call)
                self.youtube.quota_tracker.use_quota(3)
                videos = response.get('items', [])
                for video in videos:
                    title = video.get('snippet', {}).get('title', '').lower()
                    description = video.get('snippet', {}).get('description', '').lower()
                    strong_indicators = [
                        'live', 'en vivo', 'stream', 'directo', 'transmisión',
                        'jugando en vivo', 'streaming now', 'live now'
                    ]
                    for indicator in strong_indicators:
                        if indicator in title or indicator in description:
                            score += 3
                            break
                    if any(duration_word in title for duration_word in ['horas', 'hour', '2h', '3h']):
                        score += 2
                if score >= 10:
                    break
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                self.logger.warning("⚠️ APIs agotadas durante verificación de keywords")
                return 0
            self.logger.debug(f"Error buscando keywords live: {e}")
        return min(score, 15)
    def _check_broadcast_content(self, channel_id: str) -> int:
        if not self.youtube.can_make_request(3):
            return 0
        if getattr(self.youtube, 'all_apis_exhausted', False):
            return 0
        score = 0
        try:
            def api_call():
                return self.youtube.youtube.search().list(
                    channelId=channel_id,
                    part='snippet',
                    type='video',
                    maxResults=10,
                    order='date',
                    videoDuration='long'
                ).execute()
            response = self.youtube._call_with_rotation(api_call)
            self.youtube.quota_tracker.use_quota(3)
            videos = response.get('items', [])
            if len(videos) >= 5:
                score += 8
            elif len(videos) >= 3:
                score += 5
            elif len(videos) >= 1:
                score += 3
            if len(videos) >= 8:
                score += 5
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                self.logger.warning("⚠️ APIs agotadas durante verificación de broadcast")
                return 0
            self.logger.debug(f"Error verificando broadcast content: {e}")
        return score
    def _check_channel_description(self, channel_data: dict) -> int:
        description = channel_data.get('snippet', {}).get('description', '').lower()
        streaming_indicators = [
            'streamer', 'streaming', 'twitch', 'live', 'en vivo',
            'transmito', 'streams', 'directo', 'gameplay',
            'jugando en vivo', 'canal de gaming', 'gamer',
            'transmisiones', 'broadcasts', 'contenido en vivo'
        ]
        platform_mentions = [
            'twitch.tv', 'facebook gaming', 'youtube live',
            'discord', 'horarios', 'schedule', 'stream'
        ]
        score = 0
        for indicator in streaming_indicators:
            if indicator in description:
                score += 3
        for platform in platform_mentions:
            if platform in description:
                score += 2
        schedule_patterns = [
            r'\d+\s*hs', r'\d+:\d+', r'lunes.*viernes',
            r'horario', r'schedule', r'todos los días'
        ]
        for pattern in schedule_patterns:
            if re.search(pattern, description):
                score += 2
        return min(score, 12)
    def _check_streaming_schedule(self, channel_data: dict) -> int:
        snippet = channel_data.get('snippet', {})
        text = (snippet.get('description', '') + ' ' + snippet.get('title', '')).lower()
        schedule_indicators = [
            r'\d+\s*hs\s*(a|hasta)\s*\d+\s*hs',
            r'lunes.*viernes',
            r'stream.*\d+:\d+',
            r'en vivo.*\d+',
            r'transmito.*\d+',
            r'todos los días.*\d+',
        ]
        score = 0
        for pattern in schedule_indicators:
            if re.search(pattern, text):
                score += 4
        return min(score, 8)
    def _check_streaming_platforms(self, channel_data: dict) -> int:
        snippet = channel_data.get('snippet', {})
        text = (snippet.get('description', '') + ' ' + snippet.get('title', '')).lower()
        platforms = {
            'twitch': 5,
            'facebook gaming': 4,
            'youtube live': 3,
            'mixer': 3,
            'discord': 2,
            'obs': 4,
            'streamlabs': 4,
            'xsplit': 3
        }
        score = 0
        for platform, points in platforms.items():
            if platform in text:
                score += points
        return min(score, 10)

class YouTubeClient:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.quota_tracker = QuotaTracker()
        self.cache = APICache()
        self.api_keys = [
            os.getenv("YOUTUBE_API_KEY", ""),
            os.getenv("API_KEY_2", "")
        ]
        self.api_keys = [k for k in self.api_keys if k]
        if not self.api_keys:
            raise Exception("No hay claves de API de YouTube configuradas.")
        self.key_index = 0
        self.youtube = None
        self.all_apis_exhausted = False
        if HAS_YOUTUBE_API and self.api_keys:
            self.youtube = self._build_client(self.api_keys[self.key_index])
            self.mode = 'production'
            self.logger.info(f"✅ Modo producción con {len(self.api_keys)} APIs")
        else:
            self.mode = 'simulation'
            self.logger.warning("⚠️ Ejecutando en modo simulación")
    def _build_client(self, api_key):
        return build('youtube', 'v3', developerKey=api_key)
    def _rotate_key(self):
        if self.key_index + 1 < len(self.api_keys):
            self.key_index += 1
            self.youtube = self._build_client(self.api_keys[self.key_index])
            self.logger.warning(f"🔄 Cambiando a la clave de API #{self.key_index+1}")
            return True
        return False
    def _call_with_rotation(self, func, *args, **kwargs):
        if self.all_apis_exhausted:
            raise Exception("Todas las APIs han agotado su cuota diaria")
        attempts = 0
        max_attempts = len(self.api_keys)
        while attempts < max_attempts:
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                if (hasattr(e, 'resp') and e.resp.status == 403 and 
                    'quotaExceeded' in str(e)):
                    self.logger.warning(f"Encountered 403 Forbidden with reason \"quotaExceeded\"")
                    self.logger.error("❌ Cuota agotada para la clave actual.")
                    if self._rotate_key():
                        attempts += 1
                        continue
                    else:
                        self.logger.error("🛑 TODAS LAS APIS AGOTADAS - Esperando hasta mañana")
                        self.all_apis_exhausted = True
                        raise Exception("Todas las APIs han agotado su cuota diaria")
                else:
                    raise e
        self.logger.error("🛑 TODAS LAS APIS AGOTADAS - Esperando hasta mañana")
        self.all_apis_exhausted = True
        raise Exception("Todas las APIs han agotado su cuota diaria")
    def can_make_request(self, cost: int) -> bool:
        return self.quota_tracker.can_use_quota(cost)
    def search_channels(self, query: str, max_pages: int = 5) -> list:
        if self.all_apis_exhausted:
            raise Exception("Todas las APIs han agotado su cuota diaria")
        if self.mode == 'simulation':
            return self._simulate_search(query, max_pages)
        channels = []
        page_token = None
        for page in range(max_pages):
            if not self.can_make_request(Config.COST_SEARCH):
                self.logger.warning(f"⚠️ Cuota insuficiente para continuar búsqueda")
                break
            cache_key = f"search_{query}_{page}"
            cached = self.cache.get(cache_key)
            if cached:
                channels.extend(cached['items'])
                page_token = cached.get('nextPageToken')
                continue
            def api_call():
                return self.youtube.search().list(
                    q=query,
                    part='snippet',
                    type='channel',
                    maxResults=50,
                    pageToken=page_token,
                    regionCode='AR'
                ).execute()
            try:
                response = self._call_with_rotation(api_call)
                self.quota_tracker.use_quota(Config.COST_SEARCH)
                self.cache.set(cache_key, response)
                channels.extend(response.get('items', []))
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                time.sleep(0.5)
            except HttpError as e:
                self.logger.error(f"Error en búsqueda: {e}")
                break
            except Exception as e:
                if "Todas las APIs han agotado su cuota diaria" in str(e):
                    raise e
                self.logger.error(f"Error inesperado: {e}")
                break
        return channels
    def get_channel_details(self, channel_id: str) -> Optional[dict]:
        if self.all_apis_exhausted:
            raise Exception("Todas las APIs han agotado su cuota diaria")
        if self.mode == 'simulation':
            return self._simulate_channel_details(channel_id)
        if not self.can_make_request(Config.COST_CHANNEL_DETAILS):
            return None
        cache_key = f"channel_{channel_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        def api_call():
            return self.youtube.channels().list(
                part='snippet,statistics,status,contentDetails',
                id=channel_id
            ).execute()
        try:
            response = self._call_with_rotation(api_call)
            self.quota_tracker.use_quota(Config.COST_CHANNEL_DETAILS)
            if response.get('items'):
                channel = response['items'][0]
                streaming_detector = StreamingDetector(self)
                channel['has_streaming'] = streaming_detector.check_streaming_capability(channel)
                self.cache.set(cache_key, channel)
                return channel
        except HttpError as e:
            self.logger.error(f"Error obteniendo canal {channel_id}: {e}")
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                raise e
            self.logger.error(f"Error inesperado: {e}")
        return None
    def get_recent_videos(self, channel_id: str, max_videos: int = 5) -> list:
        if self.all_apis_exhausted:
            raise Exception("Todas las APIs han agotado su cuota diaria")
        if self.mode == 'simulation':
            return self._simulate_recent_videos(channel_id, max_videos)
        if not self.can_make_request(Config.COST_VIDEO_LIST):
            return []
        def api_call():
            return self.youtube.search().list(
                channelId=channel_id,
                part='snippet',
                order='date',
                type='video',
                maxResults=max_videos
            ).execute()
        try:
            response = self._call_with_rotation(api_call)
            self.quota_tracker.use_quota(Config.COST_VIDEO_LIST)
            return response.get('items', [])
        except HttpError as e:
            self.logger.error(f"Error obteniendo videos: {e}")
            return []
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                raise e
            self.logger.error(f"Error inesperado: {e}")
            return []
    def _simulate_search(self, query: str, max_pages: int) -> list:
        channels = []
        for i in range(min(max_pages * 10, 50)):
            channels.append({
                'id': {'channelId': f'sim_channel_{query}_{i}'},
                'snippet': {
                    'title': f'Canal {query} #{i}',
                    'description': f'Canal de streaming argentino sobre {query}',
                    'channelId': f'sim_channel_{query}_{i}'
                }
            })
        return channels
    def _simulate_channel_details(self, channel_id: str) -> dict:
        import random
        provinces = list(Config.PROVINCIAS_ARGENTINAS)
        return {
            'id': channel_id,
            'snippet': {
                'title': f'Canal Simulado {channel_id[-4:]}',
                'description': f'Soy un streamer argentino de {random.choice(provinces)}.',
                'country': 'AR',
                'publishedAt': '2020-01-01T00:00:00Z'
            },
            'statistics': {
                'subscriberCount': str(random.randint(500, 50000)),
                'videoCount': str(random.randint(50, 500)),
                'viewCount': str(random.randint(10000, 1000000))
            },
            'has_streaming': True
        }
    def _simulate_recent_videos(self, channel_id: str, max_videos: int) -> list:
        videos = []
        for i in range(max_videos):
            videos.append({
                'snippet': {
                    'title': f'Stream de gaming argentino - Parte {i+1}',
                    'description': 'Jugando con los pibes, che vení que arrancamos!',
                    'publishedAt': datetime.now().isoformat()
                }
            })
        return videos

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
                if data.get('date') == datetime.now().strftime('%Y-%m-%d'):
                    self.used_quota = data.get('used', 0)
                else:
                    self.used_quota = 0
            except:
                self.used_quota = 0
        else:
            self.used_quota = 0
    def save_quota(self):
        data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'used': self.used_quota,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.quota_file, 'w') as f:
            json.dump(data, f, indent=2)
    def can_use_quota(self, cost: int) -> bool:
        effective_limit = Config.MAX_DAILY_QUOTA - Config.SAFETY_BUFFER
        return (self.used_quota + cost) <= effective_limit
    def use_quota(self, cost: int):
        self.used_quota += cost
        self.save_quota()
        if self.used_quota > Config.QUOTA_WARNING_THRESHOLD:
            self.logger.warning(
                f"⚠️  CUOTA ALTA: {self.used_quota:,}/{Config.MAX_DAILY_QUOTA:,} "
                f"({(self.used_quota/Config.MAX_DAILY_QUOTA)*100:.1f}%)"
            )
        if self.used_quota >= (Config.MAX_DAILY_QUOTA - Config.SAFETY_BUFFER):
            self.logger.error("❌ CUOTA AGOTADA - Deteniendo ejecución")
            raise Exception("Cuota diaria agotada")
    def get_remaining(self) -> int:
        return Config.MAX_DAILY_QUOTA - self.used_quota

class APICache:
    def __init__(self):
        self.cache_file = Config.API_CACHE
        self.cache_data = self.load_cache()
    def load_cache(self) -> Dict:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    def save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache_data, f, indent=2)
    def get(self, key: str) -> Optional[Dict]:
        entry = self.cache_data.get(key)
        if entry:
            timestamp = entry.get('timestamp', 0)
            if time.time() - timestamp < 604800:
                return entry.get('data')
        return None
    def set(self, key: str, data: Dict):
        self.cache_data[key] = {
            'data': data,
            'timestamp': time.time()
        }
        if len(self.cache_data) % 10 == 0:
            self.save_cache()

class DataManager:
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.processed_channels = self.load_processed_channels()
        self.streamers_data = self.load_streamers_data()
    def load_processed_channels(self) -> Set[str]:
        if Config.PROCESSED_CHANNELS.exists():
            try:
                with open(Config.PROCESSED_CHANNELS, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    def save_processed_channels(self):
        with open(Config.PROCESSED_CHANNELS, 'wb') as f:
            pickle.dump(self.processed_channels, f)
    def load_streamers_data(self) -> List[Dict]:
        streamers = []
        if Config.STREAMERS_CSV.exists():
            try:
                with open(Config.STREAMERS_CSV, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    streamers = list(reader)
            except:
                pass
        return streamers
    def is_channel_processed(self, channel_id: str) -> bool:
        return channel_id in self.processed_channels
    def mark_channel_processed(self, channel_id: str):
        self.processed_channels.add(channel_id)
        if len(self.processed_channels) % 50 == 0:
            self.save_processed_channels()
    def save_streamer(self, streamer: StreamerData):
        write_headers = not Config.STREAMERS_CSV.exists()
        with open(Config.STREAMERS_CSV, 'a', newline='', encoding='utf-8') as f:
            fieldnames = [
                'canal_id', 'nombre_canal', 'categoria', 'provincia', 'ciudad',
                'suscriptores', 'certeza', 'metodo_deteccion', 'indicadores_argentinidad',
                'url', 'fecha_deteccion', 'ultima_actividad', 'tiene_streaming',
                'descripcion', 'pais_detectado', 'videos_analizados'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_headers:
                writer.writeheader()
            row = asdict(streamer)
            row['indicadores_argentinidad'] = ', '.join(row['indicadores_argentinidad'])
            writer.writerow(row)
        self.streamers_data.append(row)
    def get_statistics(self) -> Dict:
        stats = {
            'total': len(self.streamers_data),
            'por_provincia': defaultdict(int),
            'por_categoria': defaultdict(int),
            'por_metodo': defaultdict(int)
        }
        for streamer in self.streamers_data:
            stats['por_provincia'][streamer['provincia']] += 1
            stats['por_categoria'][streamer['categoria']] += 1
            stats['por_metodo'][streamer['metodo_deteccion']] += 1
        return stats

class ChannelAnalyzer:
    def __init__(self, youtube_client: YouTubeClient, detector: ArgentineDetector):
        self.youtube = youtube_client
        self.detector = detector
        self.logger = logging.getLogger('StreamingArgentina')
    def categorize_channel(self, description: str, videos: List[Dict]) -> str:
        text = description.lower()
        for video in videos[:5]:
            text += ' ' + video.get('snippet', {}).get('title', '').lower()
        categories = {
            'Gaming': ['gaming', 'games', 'juegos', 'videojuegos', 'gamer', 'twitch', 'minecraft', 'fortnite'],
            'Charlas/Podcast': ['charlas', 'podcast', 'entrevista', 'conversación', 'debate', 'opinión'],
            'IRL/Vlogs': ['irl', 'vlog', 'vida', 'día', 'diario', 'salida', 'aventura'],
            'Música': ['música', 'music', 'cantante', 'banda', 'cover', 'canción', 'musical'],
            'Cocina': ['cocina', 'receta', 'cooking', 'comida', 'gastronomía', 'chef'],
            'Educativo': ['educativo', 'tutorial', 'clase', 'enseña', 'aprende', 'curso'],
            'Deportes': ['deporte', 'fútbol', 'basket', 'tenis', 'gym', 'entrena'],
            'Tecnología': ['tech', 'tecnología', 'programación', 'código', 'software'],
            'Arte': ['arte', 'dibujo', 'pintura', 'diseño', 'ilustración', 'creativo']
        }
        scores = defaultdict(int)
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in text:
                    scores[category] += 1
        if scores:
            return max(scores.items(), key=lambda x: x[1])[0]
        return 'Entretenimiento'
    def enhanced_channel_filter(self, channel_details: dict) -> tuple[bool, str]:
        stats = channel_details.get('statistics', {})
        snippet = channel_details.get('snippet', {})
        if not channel_details.get('has_streaming', False):
            return False, "Sin evidencia de streaming real"
        published_at = snippet.get('publishedAt', '')
        if published_at:
            try:
                pub_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                if (datetime.now() - pub_date.replace(tzinfo=None)).days > 365 * 3:
                    return False, "Canal inactivo (>3 años)"
            except:
                pass
        subscribers = int(stats.get('subscriberCount', 0))
        view_count = int(stats.get('viewCount', 0))
        if subscribers > 0 and view_count > 0:
            views_per_sub = view_count / subscribers
            if views_per_sub < 50:
                return False, "Engagement muy bajo"
        title = snippet.get('title', '').lower()
        description = snippet.get('description', '').lower()
        institutional_keywords = [
            'oficial', 'empresa', 'corporativo', 'institucional',
            'news', 'noticias', 'government', 'gobierno',
            'university', 'universidad', 'school', 'escuela'
        ]
        for keyword in institutional_keywords:
            if keyword in title or keyword in description:
                return False, f"Canal institucional ({keyword})"
        video_count = int(stats.get('videoCount', 0))
        if video_count > 0:
            if video_count < 5 and subscribers > 10000:
                return False, "Pocos videos para ser streamer activo"
        return True, "Canal válido"
    def analyze_channel(self, channel_snippet: Dict) -> Optional[StreamerData]:
        try:
            channel_id = (channel_snippet.get('id', {}).get('channelId') or 
                         channel_snippet.get('id'))
            if not channel_id:
                return None
            channel_details = self.youtube.get_channel_details(channel_id)
            if not channel_details:
                return None
            stats = channel_details.get('statistics', {})
            subscribers = int(stats.get('subscriberCount', 0))
            if subscribers < Config.MIN_SUBSCRIBERS:
                self.logger.debug(f"Canal {channel_id} rechazado: pocos suscriptores ({subscribers})")
                return None
            is_valid, reason = self.enhanced_channel_filter(channel_details)
            if not is_valid:
                self.logger.debug(f"Canal {channel_id} rechazado: {reason}")
                return None
            recent_videos = self.youtube.get_recent_videos(channel_id, max_videos=5)
            analysis = self.detector.analyze_channel(channel_details, recent_videos)
            if not analysis['is_argentine']:
                reason = analysis.get('reason', 'No argentino')
                self.logger.debug(f"Canal {channel_id} rechazado: {reason}")
                return None
            if analysis['confidence'] < Config.MIN_CERTAINTY_ARGENTINA:
                self.logger.debug(
                    f"Canal {channel_id} rechazado: certeza baja ({analysis['confidence']}%)"
                )
                return None
            snippet = channel_details.get('snippet', {})
            category = self.categorize_channel(
                snippet.get('description', ''),
                recent_videos
            )
            streamer = StreamerData(
                canal_id=channel_id,
                nombre_canal=snippet.get('title', 'Sin nombre'),
                categoria=category,
                provincia=analysis.get('province', 'Argentina'),
                ciudad='Por determinar',
                suscriptores=subscribers,
                certeza=analysis['confidence'],
                metodo_deteccion=analysis['method'],
                indicadores_argentinidad=analysis.get('indicators', []),
                url=f"https://youtube.com/channel/{channel_id}",
                fecha_deteccion=datetime.now().strftime('%Y-%m-%d'),
                ultima_actividad=snippet.get('publishedAt', ''),
                tiene_streaming=True,
                descripcion=snippet.get('description', '')[:500],
                pais_detectado='Argentina',
                videos_analizados=len(recent_videos)
            )
            return streamer
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                raise e
            self.logger.error(f"Error analizando canal: {e}")
            return None

class SearchStrategy:
    @staticmethod
    def get_phase_queries(phase: int) -> List[Tuple[str, int]]:
        if phase == 1:
            return [
                ('streaming argentina', 10),
                ('youtuber argentino', 10),
                ('argentina gaming', 8),
                ('canal argentino', 8),
                ('twitch argentina', 6),
                ('stream argentino', 6),
                ('gamer argentina', 5),
                ('creador contenido argentina', 5),
            ]
        elif phase == 2:
            queries = []
            for prov in ['Buenos Aires', 'Córdoba', 'Santa Fe', 'Mendoza']:
                queries.extend([
                    (f'streaming {prov}', 8),
                    (f'youtuber {prov}', 6),
                    (f'gaming {prov}', 5),
                ])
            for prov in ['Tucumán', 'Salta', 'Neuquén', 'Entre Ríos']:
                queries.extend([
                    (f'streaming {prov}', 10),
                    (f'canal {prov}', 8),
                ])
            return queries
        elif phase == 3:
            queries = []
            for code, province in Config.CODIGOS_ARGENTINOS.items():
                queries.extend([
                    (f'{code} streaming', 12),
                    (f'{code} gaming', 10),
                    (f'youtuber {code}', 8),
                ])
            return queries
        else:
            return [
                ('che streaming gaming', 8),
                ('mate twitch', 6),
                ('asado streaming', 5),
                ('argentina vlog irl', 8),
                ('buenos aires youtuber', 6),
                ('patagonia streaming', 10),
                ('streaming folklore argentina', 8),
                ('gaming quilmes', 6),
                ('streaming rosario', 8),
                ('youtuber mendoza vino', 6),
            ]

class StreamingArgentinaEngine:
    def __init__(self):
        Config.setup_directories()
        self.logger_system = Logger()
        self.logger = self.logger_system.logger
        self.youtube = YouTubeClient()
        self.detector = ArgentineDetector()
        self.analyzer = ChannelAnalyzer(self.youtube, self.detector)
        self.data_manager = DataManager()
        self.channels_analyzed_today = 0
        self.streamers_found_today = 0
        self.apis_exhausted = False
    def process_search_term(self, term: str, max_pages: int) -> int:
        self.logger.info(f"🔍 Buscando: '{term}' (máx {max_pages} páginas)")
        if self.apis_exhausted:
            self.logger.warning(f"⚠️ Saltando '{term}' - APIs agotadas")
            return 0
        try:
            channels = self.youtube.search_channels(term, max_pages)
        except Exception as e:
            if "Todas las APIs han agotado su cuota diaria" in str(e):
                self.logger.error("🛑 APIs AGOTADAS - Deteniendo todas las búsquedas")
                self.apis_exhausted = True
                return 0
            else:
                self.logger.error(f"Error en búsqueda: {e}")
                return 0
        if not channels:
            self.logger.warning(f"No se encontraron canales para '{term}'")
            return 0
        new_channels = []
        for channel in channels:
            channel_id = (channel.get('id', {}).get('channelId') or 
                         channel.get('id'))
            if channel_id and not self.data_manager.is_channel_processed(channel_id):
                new_channels.append(channel)
                self.data_manager.mark_channel_processed(channel_id)
        self.logger.info(f"📊 {len(channels)} encontrados, {len(new_channels)} nuevos")
        streamers_found = 0
        for i, channel in enumerate(new_channels, 1):
            if self.apis_exhausted:
                self.logger.warning("⚠️ Deteniendo análisis - APIs agotadas")
                break
            needed_quota = Config.COST_CHANNEL_DETAILS + Config.COST_VIDEO_LIST + 9
            if not self.youtube.can_make_request(needed_quota):
                self.logger.warning("⚠️ Cuota insuficiente para continuar análisis")
                break
            self.channels_analyzed_today += 1
            try:
                streamer = self.analyzer.analyze_channel(channel)
            except Exception as e:
                if "Todas las APIs han agotado su cuota diaria" in str(e):
                    self.logger.error("🛑 APIs AGOTADAS durante análisis - Deteniendo")
                    self.apis_exhausted = True
                    break
                else:
                    self.logger.error(f"Error analizando canal: {e}")
                    continue
            if streamer:
                self.data_manager.save_streamer(streamer)
                self.logger_system.streamer_found(streamer)
                streamers_found += 1
                self.streamers_found_today += 1
            if i % 20 == 0:
                self.logger.info(
                    f"📈 Progreso: {i}/{len(new_channels)} analizados, "
                    f"{streamers_found} streamers encontrados"
                )
        return streamers_found
    def execute_phase(self, phase: int) -> Dict:
        queries = SearchStrategy.get_phase_queries(phase)
        self.logger_system.phase_start(
            phase,
            f"{len(queries)} búsquedas específicas"
        )
        results = {
            'phase': phase,
            'queries_processed': 0,
            'streamers_found': 0
        }
        for query, max_pages in queries:
            if self.apis_exhausted:
                self.logger.warning("⚠️ Deteniendo fase - APIs agotadas")
                break
            min_quota_needed = Config.COST_SEARCH * 2
            if not self.youtube.can_make_request(min_quota_needed):
                self.logger.warning("⚠️ Cuota insuficiente para continuar fase")
                break
            try:
                found = self.process_search_term(query, max_pages)
                results['queries_processed'] += 1
                results['streamers_found'] += found
                if self.apis_exhausted:
                    break
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error procesando '{query}': {e}")
                if ("Cuota diaria agotada" in str(e) or 
                    "Todas las APIs han agotado su cuota diaria" in str(e)):
                    self.apis_exhausted = True
                    break
        return results
    def run_daily_execution(self) -> Dict:
        start_time = time.time()
        self.logger.info("="*80)
        self.logger.info("🚀 INICIANDO EJECUCIÓN DIARIA")
        self.logger.info(f"📅 Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}")
        self.logger.info(f"🔋 Cuota disponible: {self.youtube.quota_tracker.get_remaining():,}")
        self.logger.info("="*80)
        day_of_year = datetime.now().timetuple().tm_yday
        current_phase = (day_of_year % 4) + 1
        self.logger.info(f"🎯 FASE ACTUAL: {current_phase} (Día {day_of_year} del año)")
        results = {'phase': current_phase, 'streamers_found': 0}
        try:
            phase_results = self.execute_phase(current_phase)
            results.update(phase_results)
        except Exception as e:
            self.logger.error(f"Error en ejecución: {e}")
        self.data_manager.save_processed_channels()
        self.youtube.cache.save_cache()
        duration = (time.time() - start_time) / 60
        stats = self.data_manager.get_statistics()
        self.logger.info("\n" + "="*80)
        self.logger.info("📊 RESUMEN DE EJECUCIÓN")
        self.logger.info("="*80)
        self.logger.info(f"⏱️  Duración: {duration:.1f} minutos")
        self.logger.info(f"🔍 Canales analizados hoy: {self.channels_analyzed_today}")
        self.logger.info(f"✅ Streamers encontrados hoy: {self.streamers_found_today}")
        self.logger.info(f"🔋 Cuota API usada: {self.youtube.quota_tracker.used_quota:,}")
        self.logger.info(f"📈 TOTAL ACUMULADO: {stats['total']} streamers argentinos")
        if stats['por_provincia']:
            self.logger.info("\n🏆 TOP 5 PROVINCIAS:")
            for prov, count in sorted(stats['por_provincia'].items(), 
                                     key=lambda x: x[1], reverse=True)[:5]:
                self.logger.info(f"   {prov}: {count}")
        self.logger_system.daily_summary()
        return results

def main():
    try:
        if not Config.YOUTUBE_API_KEY and not Config.API_KEY_2 and HAS_YOUTUBE_API:
            print("❌ ERROR: APIs de YouTube no configuradas")
            print("Configura tus API keys:")
            print("  export YOUTUBE_API_KEY='tu_primera_api_key'")
            print("  export API_KEY_2='tu_segunda_api_key'")
            print("\nO ejecuta en modo simulación sin la dependencia de Google")
            return
        if HAS_YOUTUBE_API:
            api_count = sum(1 for api in [Config.YOUTUBE_API_KEY, Config.API_KEY_2] if api)
            if api_count == 0:
                print("❌ ERROR: No hay APIs configuradas")
                return
            elif api_count == 1:
                print("⚠️  ADVERTENCIA: Solo 1 API configurada (10,000 requests/día)")
                print("Configura API_KEY_2 para 20,000 requests/día")
            else:
                print("✅ 2 APIs configuradas (20,000 requests/día)")
        # Mostrar información del día actual
        day_of_year = datetime.now().timetuple().tm_yday
        current_phase = (day_of_year % 4) + 1
        cycle_number = (day_of_year - 1) // 4 + 1
        days_remaining = 30 - ((day_of_year - 1) % 30)  # Solo ciclo de 30 días
        print("\n" + "="*80)
        print("🎯 CARTOGRAFÍA STREAMING ARGENTINO - FILTRO EXPLÍCITO")
        print("="*80)
        print(f"📅 Día del año: {day_of_year}")
        print(f"🔄 Fase actual: {current_phase}")
        print(f"📊 Ciclo: {cycle_number}/30")
        print(f"⏰ Días restantes: {days_remaining}")
        print("="*80)
        if day_of_year > 30:
            print("🏁 PROYECTO DE 30 DÍAS COMPLETADO")
            return
        engine = StreamingArgentinaEngine()
        results = engine.run_daily_execution()
        print("\n" + "="*80)
        print("✅ EJECUCIÓN DIARIA COMPLETADA")
        print(f"🎯 Fase ejecutada: {results.get('phase', 'N/A')}")
        print(f"📊 Streamers encontrados hoy: {results.get('streamers_found', 0)}")
        print(f"📁 Datos guardados en: {Config.STREAMERS_CSV}")
        if days_remaining > 0:
            print(f"🔄 Próxima ejecución: mañana (Fase {((day_of_year % 4) + 1) if (day_of_year % 4) + 1 <= 4 else 1})")
        else:
            print("🏁 PROYECTO DE 30 DÍAS COMPLETADO")
        print("="*80)
    except KeyboardInterrupt:
        print("\n⚠️  Ejecución interrumpida por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
