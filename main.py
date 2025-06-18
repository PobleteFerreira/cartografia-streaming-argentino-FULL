#!/usr/bin/env python3
"""
CARTOGRAFÍA COMPLETA DEL STREAMING ARGENTINO - VERSIÓN HÍBRIDA
Detecta y mapea todos los streamers argentinos en YouTube
Versión optimizada con límite estricto de 10,000 llamadas API diarias
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
import hashlib

# Configuración y dependencias
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_YOUTUBE_API = True
except ImportError:
    HAS_YOUTUBE_API = False
    print("⚠️  googleapiclient no instalado. Ejecutando en modo simulación.")

# =============================================================================
# CONFIGURACIÓN PRINCIPAL
# =============================================================================

@dataclass
class StreamerData:
    """Estructura de datos para cada streamer encontrado"""
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
    """Configuración centralizada del proyecto"""
    
    # API Configuration - LÍMITE ESTRICTO
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', '')
    MAX_DAILY_QUOTA = 10000  # LÍMITE ABSOLUTO
    SAFETY_BUFFER = 1000     # Buffer de seguridad (9000 efectivo)
    QUOTA_WARNING_THRESHOLD = 8000  # Advertencia al 80%
    
    # Costos de operaciones API
    COST_SEARCH = 100       # Búsqueda
    COST_CHANNEL_DETAILS = 3  # Detalles del canal
    COST_VIDEO_LIST = 3     # Lista de videos
    
    # Filtros mínimos estrictos
    MIN_SUBSCRIBERS = 500
    MIN_CERTAINTY_ARGENTINA = 75  # Aumentado para mayor precisión
    
    # Paths
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / 'data'
    LOGS_DIR = BASE_DIR / 'logs'
    CACHE_DIR = BASE_DIR / 'cache'
    
    # Archivos
    STREAMERS_CSV = DATA_DIR / 'streamers_argentinos.csv'
    PROCESSED_CHANNELS = CACHE_DIR / 'processed_channels.pkl'
    API_CACHE = CACHE_DIR / 'api_cache.json'
    QUOTA_TRACKER = CACHE_DIR / 'quota_tracker.json'
    
    # Provincias argentinas
    PROVINCIAS_ARGENTINAS = {
        "Buenos Aires", "CABA", "Córdoba", "Santa Fe", "Mendoza", "Tucumán",
        "Salta", "Entre Ríos", "Misiones", "Chaco", "Corrientes",
        "Santiago del Estero", "Jujuy", "Neuquén", "Río Negro",
        "Formosa", "Chubut", "San Luis", "Catamarca", "La Rioja",
        "San Juan", "Santa Cruz", "Tierra del Fuego", "La Pampa"
    }
    
    # Códigos locales argentinos
    CODIGOS_ARGENTINOS = {
        "MZA": "Mendoza", "COR": "Córdoba", "ROS": "Santa Fe",
        "MDQ": "Buenos Aires", "BRC": "Río Negro", "SLA": "Salta",
        "TUC": "Tucumán", "NQN": "Neuquén", "USH": "Tierra del Fuego",
        "JUJ": "Jujuy", "SFN": "Santa Fe", "CTC": "Catamarca",
        "LRJ": "La Rioja", "FSA": "Formosa", "SGO": "Santiago del Estero"
    }
    
    @classmethod
    def setup_directories(cls):
        """Crear directorios necesarios"""
        for directory in [cls.DATA_DIR, cls.LOGS_DIR, cls.CACHE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

# =============================================================================
# SISTEMA DE LOGGING MEJORADO
# =============================================================================

class Logger:
    """Sistema de logging con estadísticas"""
    
    def __init__(self):
        self.setup_logging()
        self.stats = defaultdict(int)
        
    def setup_logging(self):
        """Configurar sistema de logging"""
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
        """Log inicio de fase"""
        self.logger.info(f"{'='*80}")
        self.logger.info(f"🎯 INICIANDO FASE {phase}: {description}")
        self.logger.info(f"{'='*80}")
    
    def streamer_found(self, streamer: StreamerData):
        """Log cuando se encuentra un streamer argentino"""
        self.logger.info(
            f"✅ ENCONTRADO: {streamer.nombre_canal} | "
            f"{streamer.provincia} | {streamer.suscriptores:,} subs | "
            f"Certeza: {streamer.certeza:.1f}% | "
            f"Método: {streamer.metodo_deteccion}"
        )
        self.stats['streamers_found'] += 1
        self.stats[f'provincia_{streamer.provincia}'] += 1
    
    def channel_rejected(self, channel_name: str, reason: str):
        """Log cuando se rechaza un canal"""
        self.logger.debug(f"❌ RECHAZADO: {channel_name} - {reason}")
        self.stats['channels_rejected'] += 1
        self.stats[f'rejected_{reason}'] += 1
    
    def quota_warning(self, used: int, total: int):
        """Advertencia de cuota"""
        percentage = (used / total) * 100
        self.logger.warning(
            f"⚠️  CUOTA API: {used:,}/{total:,} ({percentage:.1f}%) - "
            f"Restante: {total-used:,}"
        )
    
    def daily_summary(self):
        """Resumen diario de estadísticas"""
        self.logger.info("\n📊 RESUMEN DIARIO:")
        self.logger.info(f"   Streamers encontrados: {self.stats['streamers_found']}")
        self.logger.info(f"   Canales rechazados: {self.stats['channels_rejected']}")
        
        # Top provincias
        provincias = [(k.replace('provincia_', ''), v) 
                      for k, v in self.stats.items() 
                      if k.startswith('provincia_')]
        if provincias:
            provincias.sort(key=lambda x: x[1], reverse=True)
            self.logger.info("   Top provincias:")
            for prov, count in provincias[:5]:
                self.logger.info(f"     - {prov}: {count}")

# =============================================================================
# DETECTOR DE ARGENTINIDAD AVANZADO
# =============================================================================

class ArgentineDetector:
    """Sistema avanzado para detectar streamers argentinos con alta precisión"""
    
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self._setup_patterns()
    
    def _setup_patterns(self):
        """Configurar patrones de detección específicos"""
        
        # Voseo argentino (muy específico)
        self.voseo_patterns = [
            r'\bvos\s+(?:tenés|sabés|querés|podés|andás|venís|hacés|decís|sos|estás)\b',
            r'\b(?:tenés|sabés|querés|podés|andás|venís|hacés|decís)\s+vos\b',
            r'\bche\s+vos\b', r'\bvos\s+che\b'
        ]
        
        # Jerga argentina (actualizada)
        self.argentine_slang = {
            'che', 'boludo', 'gil', 'loco', 'flaco', 'capo', 'crack',
            'bárbaro', 'copado', 'zarpado', 'piola', 'groso', 'genial',
            'laburo', 'guita', 'mango', 'luca', 'palo',
            'bondi', 'colectivo', 'subte', 'boliche', 'joda',
            'quilombo', 'bardo', 'pucho', 'faso', 'birra',
            'pibe', 'piba', 'pendejo', 'wacho', 'chabón'
        }
        
        # Cultura argentina
        self.argentine_culture = {
            'mate', 'asado', 'empanadas', 'choripán', 'milanesas',
            'alfajores', 'dulce de leche', 'facturas', 'medialunas',
            'boca', 'river', 'racing', 'independiente', 'san lorenzo',
            'maradona', 'messi', 'gardel', 'tango', 'folklore',
            'cuarteto', 'cumbia', 'rock nacional', 'charly garcía'
        }
        
        # Indicadores de horario argentino
        self.argentine_time_patterns = [
            r'\d+\s*hs', r'hora argentina', r'ART', r'UTC-3',
            r'buenos aires time', r'argentina time'
        ]
        
        # Exclusiones automáticas (otros países)
        self.country_exclusions = {
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
    
    def detect_explicit_argentina(self, text: str) -> Optional[Dict]:
        """Detección explícita de referencias a Argentina"""
        text_lower = text.lower()
        
        # 1. Menciona Argentina directamente
        argentina_mentions = ['argentina', 'argentino', 'argentinos', 'argentinas', 'arg 🇦🇷']
        for mention in argentina_mentions:
            if mention in text_lower:
                return {
                    'method': 'explicit_country',
                    'is_argentine': True,
                    'confidence': 95,
                    'indicators': [f'menciona_{mention}']
                }
        
        # 2. Códigos locales argentinos
        for code, province in Config.CODIGOS_ARGENTINOS.items():
            if re.search(rf'\b{code.lower()}\b', text_lower):
                return {
                    'method': 'local_code',
                    'is_argentine': True,
                    'confidence': 92,
                    'province': province,
                    'indicators': [f'codigo_{code}']
                }
        
        # 3. Provincias argentinas
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
        """Detectar si menciona otros países (exclusión)"""
        text_lower = text.lower()
        
        for country, indicators in self.country_exclusions.items():
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
    
    def analyze_cultural_patterns(self, text: str) -> Dict:
        """Análisis profundo de patrones culturales argentinos"""
        text_lower = text.lower()
        score = 0
        indicators = []
        
        # Voseo (peso muy alto)
        voseo_matches = []
        for pattern in self.voseo_patterns:
            matches = re.findall(pattern, text_lower)
            voseo_matches.extend(matches)
        
        if voseo_matches:
            score += len(voseo_matches) * 20
            indicators.append(f'voseo_{len(voseo_matches)}')
        
        # Jerga argentina
        slang_found = [slang for slang in self.argentine_slang if slang in text_lower]
        if slang_found:
            score += len(slang_found) * 10
            indicators.append(f'jerga_{len(slang_found)}')
        
        # Cultura argentina
        culture_found = [culture for culture in self.argentine_culture if culture in text_lower]
        if culture_found:
            score += len(culture_found) * 8
            indicators.append(f'cultura_{len(culture_found)}')
        
        # Horarios argentinos
        time_matches = sum(1 for pattern in self.argentine_time_patterns 
                          if re.search(pattern, text_lower))
        if time_matches:
            score += time_matches * 15
            indicators.append(f'horarios_{time_matches}')
        
        # Calcular confianza (máximo 95%)
        confidence = min(95, score * 1.5)
        
        return {
            'method': 'cultural_patterns',
            'is_argentine': confidence >= Config.MIN_CERTAINTY_ARGENTINA,
            'confidence': confidence,
            'score': score,
            'indicators': indicators
        }
    
    def detect_region(self, text: str) -> Tuple[str, float]:
        """Detectar región específica dentro de Argentina"""
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
        """Análisis completo de un canal para determinar si es argentino"""
        
        # Construir texto completo para análisis
        snippet = channel_data.get('snippet', {})
        text_parts = [
            snippet.get('title', ''),
            snippet.get('description', ''),
            snippet.get('country', '')
        ]
        
        # Agregar información de videos si está disponible
        if videos_data:
            for video in videos_data[:10]:  # Máximo 10 videos
                video_snippet = video.get('snippet', {})
                text_parts.extend([
                    video_snippet.get('title', ''),
                    video_snippet.get('description', '')[:200]  # Primeros 200 chars
                ])
        
        full_text = ' '.join(text_parts)
        
        # Paso 1: Verificar exclusiones de otros países
        other_country = self.detect_other_countries(full_text)
        if other_country and other_country['confidence'] > 80:
            return {
                'is_argentine': False,
                'confidence': 0,
                'reason': f"Canal de {other_country['country']}",
                'method': 'country_exclusion',
                'indicators': other_country['indicators']
            }
        
        # Paso 2: Detección explícita de Argentina
        explicit = self.detect_explicit_argentina(full_text)
        if explicit:
            region, confidence_boost = self.detect_region(full_text)
            explicit['confidence'] = min(100, explicit['confidence'] + confidence_boost)
            explicit['province'] = explicit.get('province', region)
            return explicit
        
        # Paso 3: Análisis de patrones culturales
        cultural = self.analyze_cultural_patterns(full_text)
        
        if cultural['is_argentine']:
            region, confidence_boost = self.detect_region(full_text)
            cultural['confidence'] = min(100, cultural['confidence'] + confidence_boost)
            cultural['province'] = region
            return cultural
        
        # Paso 4: No hay evidencia suficiente
        return {
            'is_argentine': False,
            'confidence': cultural['confidence'],
            'reason': 'Evidencia insuficiente',
            'method': 'insufficient_evidence',
            'indicators': cultural.get('indicators', [])
        }

# =============================================================================
# CLIENTE YOUTUBE API CON CONTROL ESTRICTO DE CUOTA
# =============================================================================

class YouTubeClient:
    """Cliente YouTube con control estricto de cuota"""
    
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.quota_tracker = QuotaTracker()
        self.cache = APICache()
        self.youtube = None
        
        if HAS_YOUTUBE_API and Config.YOUTUBE_API_KEY:
            self.youtube = build('youtube', 'v3', developerKey=Config.YOUTUBE_API_KEY)
            self.mode = 'production'
        else:
            self.mode = 'simulation'
            self.logger.warning("⚠️  Ejecutando en modo simulación")
    
    def can_make_request(self, cost: int) -> bool:
        """Verificar si se puede hacer una request sin exceder límites"""
        return self.quota_tracker.can_use_quota(cost)
    
    def search_channels(self, query: str, max_pages: int = 5) -> List[Dict]:
        """Buscar canales con paginación controlada"""
        if self.mode == 'simulation':
            return self._simulate_search(query, max_pages)
        
        channels = []
        page_token = None
        
        for page in range(max_pages):
            # Verificar cuota antes de cada página
            if not self.can_make_request(Config.COST_SEARCH):
                self.logger.warning(f"⚠️  Cuota insuficiente para continuar búsqueda")
                break
            
            # Verificar cache
            cache_key = f"search_{query}_{page}"
            cached = self.cache.get(cache_key)
            
            if cached:
                channels.extend(cached['items'])
                page_token = cached.get('nextPageToken')
                continue
            
            try:
                # Llamada a la API
                response = self.youtube.search().list(
                    q=query,
                    part='snippet',
                    type='channel',
                    maxResults=50,
                    pageToken=page_token,
                    regionCode='AR'
                ).execute()
                
                self.quota_tracker.use_quota(Config.COST_SEARCH)
                
                # Guardar en cache
                self.cache.set(cache_key, response)
                
                channels.extend(response.get('items', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                
                # Pequeña pausa para evitar rate limiting
                time.sleep(0.5)
                
            except HttpError as e:
                self.logger.error(f"Error en búsqueda: {e}")
                break
            except Exception as e:
                self.logger.error(f"Error inesperado: {e}")
                break
        
        return channels
    
    def get_channel_details(self, channel_id: str) -> Optional[Dict]:
        """Obtener detalles completos del canal"""
        if self.mode == 'simulation':
            return self._simulate_channel_details(channel_id)
        
        # Verificar cuota
        if not self.can_make_request(Config.COST_CHANNEL_DETAILS):
            return None
        
        # Verificar cache
        cache_key = f"channel_{channel_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        try:
            response = self.youtube.channels().list(
                part='snippet,statistics,status,contentDetails',
                id=channel_id
            ).execute()
            
            self.quota_tracker.use_quota(Config.COST_CHANNEL_DETAILS)
            
            if response.get('items'):
                channel = response['items'][0]
                
                # Verificar si tiene streaming
                channel['has_streaming'] = self._check_streaming_capability(channel)
                
                # Guardar en cache
                self.cache.set(cache_key, channel)
                
                return channel
            
        except HttpError as e:
            self.logger.error(f"Error obteniendo canal {channel_id}: {e}")
        
        return None
    
    def get_recent_videos(self, channel_id: str, max_videos: int = 5) -> List[Dict]:
        """Obtener videos recientes para análisis"""
        if self.mode == 'simulation':
            return self._simulate_recent_videos(channel_id, max_videos)
        
        # Verificar cuota
        if not self.can_make_request(Config.COST_VIDEO_LIST):
            return []
        
        try:
            response = self.youtube.search().list(
                channelId=channel_id,
                part='snippet',
                order='date',
                type='video',
                maxResults=max_videos
            ).execute()
            
            self.quota_tracker.use_quota(Config.COST_VIDEO_LIST)
            
            return response.get('items', [])
            
        except HttpError as e:
            self.logger.error(f"Error obteniendo videos: {e}")
            return []
    
    def _check_streaming_capability(self, channel_data: Dict) -> bool:
        """Verificar si el canal tiene capacidad de streaming"""
        # Verificar por features del canal
        content_details = channel_data.get('contentDetails', {})
        
        # Si tiene lista de uploads, probablemente puede hacer streaming
        if content_details.get('relatedPlaylists', {}).get('uploads'):
            stats = channel_data.get('statistics', {})
            video_count = int(stats.get('videoCount', 0))
            
            # Si tiene más de 10 videos, asumimos que puede hacer streaming
            return video_count > 10
        
        return False
    
    def _simulate_search(self, query: str, max_pages: int) -> List[Dict]:
        """Simular búsqueda para desarrollo"""
        channels = []
        
        # Generar canales simulados basados en la query
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
    
    def _simulate_channel_details(self, channel_id: str) -> Dict:
        """Simular detalles del canal"""
        import random
        
        provinces = list(Config.PROVINCIAS_ARGENTINAS)
        
        return {
            'id': channel_id,
            'snippet': {
                'title': f'Canal Simulado {channel_id[-4:]}',
                'description': f'Soy un streamer argentino de {random.choice(provinces)}. '
                             f'Hago streams de gaming y charlas. Vos sabés que acá '
                             f'la pasamos bárbaro che!',
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
    
    def _simulate_recent_videos(self, channel_id: str, max_videos: int) -> List[Dict]:
        """Simular videos recientes"""
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

# =============================================================================
# GESTIÓN DE CUOTA ESTRICTA
# =============================================================================

class QuotaTracker:
    """Control estricto de cuota API"""
    
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.quota_file = Config.QUOTA_TRACKER
        self.load_quota()
    
    def load_quota(self):
        """Cargar estado de cuota"""
        if self.quota_file.exists():
            try:
                with open(self.quota_file, 'r') as f:
                    data = json.load(f)
                    
                # Verificar si es del día actual
                if data.get('date') == datetime.now().strftime('%Y-%m-%d'):
                    self.used_quota = data.get('used', 0)
                else:
                    self.used_quota = 0
            except:
                self.used_quota = 0
        else:
            self.used_quota = 0
    
    def save_quota(self):
        """Guardar estado de cuota"""
        data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'used': self.used_quota,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.quota_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def can_use_quota(self, cost: int) -> bool:
        """Verificar si se puede usar cuota"""
        effective_limit = Config.MAX_DAILY_QUOTA - Config.SAFETY_BUFFER
        return (self.used_quota + cost) <= effective_limit
    
    def use_quota(self, cost: int):
        """Registrar uso de cuota"""
        self.used_quota += cost
        self.save_quota()
        
        # Logging según umbral
        if self.used_quota > Config.QUOTA_WARNING_THRESHOLD:
            self.logger.warning(
                f"⚠️  CUOTA ALTA: {self.used_quota:,}/{Config.MAX_DAILY_QUOTA:,} "
                f"({(self.used_quota/Config.MAX_DAILY_QUOTA)*100:.1f}%)"
            )
        
        # Detener si se alcanza el límite
        if self.used_quota >= (Config.MAX_DAILY_QUOTA - Config.SAFETY_BUFFER):
            self.logger.error("❌ CUOTA AGOTADA - Deteniendo ejecución")
            raise Exception("Cuota diaria agotada")
    
    def get_remaining(self) -> int:
        """Obtener cuota restante"""
        return Config.MAX_DAILY_QUOTA - self.used_quota

# =============================================================================
# CACHE INTELIGENTE
# =============================================================================

class APICache:
    """Cache para respuestas de API"""
    
    def __init__(self):
        self.cache_file = Config.API_CACHE
        self.cache_data = self.load_cache()
    
    def load_cache(self) -> Dict:
        """Cargar cache desde archivo"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_cache(self):
        """Guardar cache a archivo"""
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache_data, f, indent=2)
    
    def get(self, key: str) -> Optional[Dict]:
        """Obtener valor del cache"""
        entry = self.cache_data.get(key)
        if entry:
            # Cache válido por 7 días
            timestamp = entry.get('timestamp', 0)
            if time.time() - timestamp < 604800:  # 7 días
                return entry.get('data')
        return None
    
    def set(self, key: str, data: Dict):
        """Guardar en cache"""
        self.cache_data[key] = {
            'data': data,
            'timestamp': time.time()
        }
        
        # Guardar cada 10 entradas
        if len(self.cache_data) % 10 == 0:
            self.save_cache()

# =============================================================================
# GESTOR DE DATOS Y CSV
# =============================================================================

class DataManager:
    """Gestor de datos y archivos CSV"""
    
    def __init__(self):
        self.logger = logging.getLogger('StreamingArgentina')
        self.processed_channels = self.load_processed_channels()
        self.streamers_data = self.load_streamers_data()
    
    def load_processed_channels(self) -> Set[str]:
        """Cargar canales ya procesados"""
        if Config.PROCESSED_CHANNELS.exists():
            try:
                with open(Config.PROCESSED_CHANNELS, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    
    def save_processed_channels(self):
        """Guardar canales procesados"""
        with open(Config.PROCESSED_CHANNELS, 'wb') as f:
            pickle.dump(self.processed_channels, f)
    
    def load_streamers_data(self) -> List[Dict]:
        """Cargar datos de streamers existentes"""
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
        """Verificar si un canal ya fue procesado"""
        return channel_id in self.processed_channels
    
    def mark_channel_processed(self, channel_id: str):
        """Marcar canal como procesado"""
        self.processed_channels.add(channel_id)
        
        # Guardar cada 50 canales
        if len(self.processed_channels) % 50 == 0:
            self.save_processed_channels()
    
    def save_streamer(self, streamer: StreamerData):
        """Guardar streamer en CSV"""
        # Verificar si el archivo existe para determinar si escribir headers
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
            
            # Convertir StreamerData a dict
            row = asdict(streamer)
            # Convertir lista de indicadores a string
            row['indicadores_argentinidad'] = ', '.join(row['indicadores_argentinidad'])
            
            writer.writerow(row)
        
        # Agregar a datos en memoria
        self.streamers_data.append(row)
    
    def get_statistics(self) -> Dict:
        """Obtener estadísticas de streamers encontrados"""
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

# =============================================================================
# ANALIZADOR DE CANALES
# =============================================================================

class ChannelAnalyzer:
    """Analizador completo de canales"""
    
    def __init__(self, youtube_client: YouTubeClient, detector: ArgentineDetector):
        self.youtube = youtube_client
        self.detector = detector
        self.logger = logging.getLogger('StreamingArgentina')
    
    def categorize_channel(self, description: str, videos: List[Dict]) -> str:
        """Categorizar canal basado en contenido"""
        text = description.lower()
        
        # Agregar títulos de videos
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
    
    def analyze_channel(self, channel_snippet: Dict) -> Optional[StreamerData]:
        """Análisis completo de un canal"""
        try:
            # Obtener ID del canal
            channel_id = (channel_snippet.get('id', {}).get('channelId') or 
                         channel_snippet.get('id'))
            
            if not channel_id:
                return None
            
            # Obtener detalles completos
            channel_details = self.youtube.get_channel_details(channel_id)
            if not channel_details:
                return None
            
            # Verificar requisitos mínimos
            stats = channel_details.get('statistics', {})
            subscribers = int(stats.get('subscriberCount', 0))
            
            if subscribers < Config.MIN_SUBSCRIBERS:
                self.logger.debug(f"Canal {channel_id} rechazado: pocos suscriptores ({subscribers})")
                return None
            
            # Verificar si tiene streaming
            if not channel_details.get('has_streaming', False):
                self.logger.debug(f"Canal {channel_id} rechazado: sin capacidad de streaming")
                return None
            
            # Obtener videos recientes para análisis
            recent_videos = self.youtube.get_recent_videos(channel_id, max_videos=5)
            
            # Analizar argentinidad
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
            
            # Categorizar canal
            snippet = channel_details.get('snippet', {})
            category = self.categorize_channel(
                snippet.get('description', ''),
                recent_videos
            )
            
            # Crear objeto StreamerData
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
            self.logger.error(f"Error analizando canal: {e}")
            return None

# =============================================================================
# ESTRATEGIAS DE BÚSQUEDA
# =============================================================================

class SearchStrategy:
    """Estrategias de búsqueda optimizadas"""
    
    @staticmethod
    def get_phase_queries(phase: int) -> List[Tuple[str, int]]:
        """Obtener queries según la fase"""
        
        if phase == 1:
            # Términos generales - profundidad moderada
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
            # Búsqueda por provincias principales
            queries = []
            
            # Provincias grandes
            for prov in ['Buenos Aires', 'Córdoba', 'Santa Fe', 'Mendoza']:
                queries.extend([
                    (f'streaming {prov}', 8),
                    (f'youtuber {prov}', 6),
                    (f'gaming {prov}', 5),
                ])
            
            # Provincias medianas
            for prov in ['Tucumán', 'Salta', 'Neuquén', 'Entre Ríos']:
                queries.extend([
                    (f'streaming {prov}', 10),
                    (f'canal {prov}', 8),
                ])
            
            return queries
        
        elif phase == 3:
            # Búsqueda por códigos locales
            queries = []
            
            for code, province in Config.CODIGOS_ARGENTINOS.items():
                queries.extend([
                    (f'{code} streaming', 12),
                    (f'{code} gaming', 10),
                    (f'youtuber {code}', 8),
                ])
            
            return queries
        
        else:  # phase 4
            # Búsquedas culturales específicas
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

# =============================================================================
# MOTOR PRINCIPAL
# =============================================================================

class StreamingArgentinaEngine:
    """Motor principal del proyecto"""
    
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
    
    def process_search_term(self, term: str, max_pages: int) -> int:
        """Procesar un término de búsqueda"""
        self.logger.info(f"🔍 Buscando: '{term}' (máx {max_pages} páginas)")
        
        channels = self.youtube.search_channels(term, max_pages)
        
        if not channels:
            self.logger.warning(f"No se encontraron canales para '{term}'")
            return 0
        
        # Filtrar canales ya procesados
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
            # Verificar cuota antes de analizar
            if not self.youtube.can_make_request(Config.COST_CHANNEL_DETAILS + Config.COST_VIDEO_LIST):
                self.logger.warning("⚠️  Cuota insuficiente para continuar análisis")
                break
            
            self.channels_analyzed_today += 1
            
            # Analizar canal
            streamer = self.analyzer.analyze_channel(channel)
            
            if streamer:
                # Guardar streamer
                self.data_manager.save_streamer(streamer)
                self.logger_system.streamer_found(streamer)
                
                streamers_found += 1
                self.streamers_found_today += 1
            
            # Log de progreso
            if i % 20 == 0:
                self.logger.info(
                    f"📈 Progreso: {i}/{len(new_channels)} analizados, "
                    f"{streamers_found} streamers encontrados"
                )
        
        return streamers_found
    
    def execute_phase(self, phase: int) -> Dict:
        """Ejecutar una fase completa"""
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
            # Verificar cuota disponible
            min_quota_needed = Config.COST_SEARCH * 2  # Al menos 2 búsquedas
            if not self.youtube.can_make_request(min_quota_needed):
                self.logger.warning("⚠️  Cuota insuficiente para continuar fase")
                break
            
            try:
                found = self.process_search_term(query, max_pages)
                results['queries_processed'] += 1
                results['streamers_found'] += found
                
                # Pausa entre búsquedas
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error procesando '{query}': {e}")
                if "Cuota diaria agotada" in str(e):
                    break
        
        return results
    
    def run_daily_execution(self) -> Dict:
        """Ejecutar proceso diario completo"""
        start_time = time.time()
        
        self.logger.info("="*80)
        self.logger.info("🚀 INICIANDO EJECUCIÓN DIARIA")
        self.logger.info(f"📅 Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}")
        self.logger.info(f"🔋 Cuota disponible: {self.youtube.quota_tracker.get_remaining():,}")
        self.logger.info("="*80)
        
        # Determinar fase según día
        day_of_year = datetime.now().timetuple().tm_yday
        current_phase = (day_of_year % 4) + 1
        
        results = {'phase': current_phase, 'streamers_found': 0}
        
        try:
            # Ejecutar fase correspondiente
            phase_results = self.execute_phase(current_phase)
            results.update(phase_results)
            
        except Exception as e:
            self.logger.error(f"Error en ejecución: {e}")
        
        # Guardar datos pendientes
        self.data_manager.save_processed_channels()
        
        # Estadísticas finales
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
        
        # Top 5 provincias
        if stats['por_provincia']:
            self.logger.info("\n🏆 TOP 5 PROVINCIAS:")
            for prov, count in sorted(stats['por_provincia'].items(), 
                                     key=lambda x: x[1], reverse=True)[:5]:
                self.logger.info(f"   {prov}: {count}")
        
        self.logger_system.daily_summary()
        
        return results

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def create_github_action() -> str:
    """Crear archivo GitHub Action"""
    return """name: Cartografía Streaming Argentino

on:
  schedule:
    - cron: '0 8 * * *'  # 8 AM UTC (5 AM Argentina)
  workflow_dispatch:

jobs:
  search-streamers:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install google-api-python-client
    
    - name: Run streamer search
      env:
        YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
      run: |
        python cartografia_streaming_argentino.py
    
    - name: Commit results
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action"
        git add data/
        git diff --quiet && git diff --staged --quiet || git commit -m "Update: $(date +'%Y-%m-%d') streaming data"
        git push
"""

def create_readme() -> str:
    """Crear README del proyecto"""
    return """# 🎯 Cartografía Completa del Streaming Argentino

## 📋 Descripción

Sistema automatizado para detectar y mapear TODOS los streamers argentinos en YouTube, combatiendo el sesgo algorítmico que oculta el talento provincial.

## 🚀 Características

- **Detección precisa**: Sistema avanzado de NLP para identificar streamers argentinos
- **Anti-sesgo geográfico**: Búsquedas específicas por provincia y región
- **Control estricto de cuota**: Máximo 10,000 llamadas API por día
- **Análisis profundo**: Verifica streaming activo, categorización automática
- **Automatización completa**: GitHub Actions para ejecución diaria

## 📊 Datos Recopilados

- Nombre del canal
- Provincia/región
- Categoría de contenido
- Número de suscriptores
- Certeza de detección
- Método de detección
- Indicadores de argentinidad

## 🛠️ Instalación

1. Clonar el repositorio
2. Instalar dependencias: `pip install google-api-python-client`
3. Configurar API key: `export YOUTUBE_API_KEY='tu_key'`
4. Ejecutar: `python cartografia_streaming_argentino.py`

## 📈 Resultados

Los resultados se guardan en `data/streamers_argentinos.csv`

## 🤝 Contribuir

¡Ayudanos a encontrar más streamers argentinos! Reportá canales faltantes via Issues.
"""

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal"""
    try:
        # Verificar configuración
        if not Config.YOUTUBE_API_KEY and HAS_YOUTUBE_API:
            print("❌ ERROR: YOUTUBE_API_KEY no configurada")
            print("Configura tu API key:")
            print("  export YOUTUBE_API_KEY='tu_api_key'")
            print("\nO ejecuta en modo simulación sin la dependencia de Google")
            return
        
        # Crear archivos de configuración si no existen
        github_action_file = Path('.github/workflows/streaming_search.yml')
        if not github_action_file.exists():
            github_action_file.parent.mkdir(parents=True, exist_ok=True)
            github_action_file.write_text(create_github_action())
            print("✅ Archivo GitHub Action creado")
        
        readme_file = Path('README.md')
        if not readme_file.exists():
            readme_file.write_text(create_readme())
            print("✅ README.md creado")
        
        # Ejecutar motor principal
        engine = StreamingArgentinaEngine()
        results = engine.run_daily_execution()
        
        print("\n" + "="*80)
        print("✅ EJECUCIÓN COMPLETADA")
        print(f"📊 Streamers encontrados hoy: {results.get('streamers_found', 0)}")
        print(f"📁 Datos guardados en: {Config.STREAMERS_CSV}")
        print("="*80)
        
    except KeyboardInterrupt:
        print("\n⚠️  Ejecución interrumpida por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
