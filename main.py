# CARTOGRAFÍA COMPLETA DEL STREAMING ARGENTINO
# Proyecto para detectar y mapear todos los streamers argentinos en YouTube
# Rompe el sesgo algorítmico que oculta el streaming provincial

import os
import json
import csv
import re
import logging
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from collections import defaultdict
import hashlib

# Configuración y dependencias
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    import requests
    import spacy
    from textblob import TextBlob
except ImportError as e:
    print(f"Error: Instala las dependencias faltantes: {e}")
    print("pip install google-api-python-client spacy textblob requests")
    exit(1)

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
    url: str
    fecha_deteccion: str
    ultima_actividad: str
    tiene_streaming: bool
    descripcion: str
    pais_detectado: str

class ConfiguracionProyecto:
    """Configuración centralizada del proyecto"""
    
    # API Configuration
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', '')
    CUOTA_DIARIA_LIMITE = 10000
    CUOTA_BUFFER_SEGURIDAD = 500
    
    # Filtros mínimos
    MIN_SUSCRIPTORES = 500
    MIN_CERTEZA_ARGENTINA = 70
    
    # Paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    CACHE_DIR = os.path.join(BASE_DIR, 'cache')
    
    # Archivos de salida
    DB_FILE = os.path.join(DATA_DIR, 'streamers_argentinos.db')
    CSV_FINAL = os.path.join(DATA_DIR, 'cartografia_streaming_argentino.csv')
    LOG_FILE = os.path.join(LOGS_DIR, f'ejecucion_{datetime.now().strftime("%Y%m%d")}.log')
    
    # Códigos locales argentinos críticos
    CODIGOS_ARGENTINOS = {
        "MZA": "Mendoza", "COR": "Córdoba", "ROS": "Santa Fe", 
        "MDQ": "Buenos Aires", "BRC": "Río Negro", "SLA": "Salta",
        "TUC": "Tucumán", "NQN": "Neuquén", "USH": "Tierra del Fuego",
        "JUJ": "Jujuy", "SFN": "Santa Fe", "CTC": "Catamarca",
        "LRJ": "La Rioja", "FSA": "Formosa", "SGO": "Santiago del Estero"
    }
    
    # Provincias argentinas completas
    PROVINCIAS_ARGENTINAS = [
        "Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Tucumán",
        "Salta", "Entre Ríos", "Misiones", "Chaco", "Corrientes",
        "Santiago del Estero", "Jujuy", "Neuquén", "Río Negro",
        "Formosa", "Chubut", "San Luis", "Catamarca", "La Rioja",
        "San Juan", "Santa Cruz", "Tierra del Fuego", "La Pampa"
    ]
    
    @classmethod
    def crear_directorios(cls):
        """Crear directorios necesarios"""
        for directorio in [cls.DATA_DIR, cls.LOGS_DIR, cls.CACHE_DIR]:
            os.makedirs(directorio, exist_ok=True)

# =============================================================================
# SISTEMA DE LOGGING Y MONITOREO
# =============================================================================

class LoggerProyecto:
    """Sistema de logging especializado para el proyecto"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.setup_logger()
        self.estadisticas = {
            'canales_analizados': 0,
            'streamers_encontrados': 0,
            'llamadas_api_usadas': 0,
            'errores': 0,
            'por_provincia': defaultdict(int)
        }
    
    def setup_logger(self):
        """Configurar sistema de logging"""
        self.logger = logging.getLogger('StreamingArgentino')
        self.logger.setLevel(logging.INFO)
        
        # Handler para archivo
        file_handler = logging.FileHandler(self.config.LOG_FILE, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formato
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log_inicio_fase(self, fase: str, descripcion: str):
        """Log inicio de nueva fase"""
        self.logger.info(f"🎯 INICIANDO FASE: {fase}")
        self.logger.info(f"📋 Descripción: {descripcion}")
        self.logger.info("=" * 80)
    
    def log_streamer_encontrado(self, streamer: StreamerData):
        """Log cuando se encuentra un nuevo streamer"""
        self.logger.info(
            f"✅ STREAMER ENCONTRADO: {streamer.nombre_canal} "
            f"({streamer.provincia}, {streamer.suscriptores} subs, "
            f"{streamer.certeza:.1f}% certeza)"
        )
        self.estadisticas['streamers_encontrados'] += 1
        self.estadisticas['por_provincia'][streamer.provincia] += 1
    
    def log_progreso_busqueda(self, termino: str, pagina: int, encontrados: int):
        """Log progreso de búsqueda"""
        self.logger.info(
            f"🔍 Búsqueda '{termino}' - Página {pagina}: {encontrados} canales encontrados"
        )
    
    def log_estadisticas_diarias(self):
        """Log estadísticas del día"""
        self.logger.info("📊 ESTADÍSTICAS DIARIAS:")
        self.logger.info(f"   Canales analizados: {self.estadisticas['canales_analizados']}")
        self.logger.info(f"   Streamers encontrados: {self.estadisticas['streamers_encontrados']}")
        self.logger.info(f"   Llamadas API usadas: {self.estadisticas['llamadas_api_usadas']}")
        self.logger.info(f"   Errores: {self.estadisticas['errores']}")
        
        # Top 5 provincias del día
        top_provincias = sorted(
            self.estadisticas['por_provincia'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]
        
        if top_provincias:
            self.logger.info("   Top provincias encontradas:")
            for provincia, cantidad in top_provincias:
                self.logger.info(f"     {provincia}: {cantidad}")

# =============================================================================
# DETECTOR DE ARGENTINIDAD (PNL ESPECIALIZADO)
# =============================================================================

class DetectorArgentinidad:
    """Sistema especializado para detectar streamers argentinos"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.logger = LoggerProyecto().logger
        self.setup_patterns()
    
    def setup_patterns(self):
        """Configurar patrones de detección específicos"""
        
        # Voseo argentino (súper específico)
        self.voseo_patterns = [
            r'vos tenés', r'vos sabés', r'vos querés', r'vos podés',
            r'vos andás', r'vos venís', r'vos hacés', r'vos decís',
            r'vos sos', r'vos estás', r'che vos', r'vos che'
        ]
        
        # Lunfardo y jerga argentina
        self.jerga_argentina = [
            'che', 'boludo', 'gil', 'loco', 'flaco', 'gordito',
            'bárbaro', 'copado', 'zarpado', 'laburo', 'guita',
            'bondi', 'colectivo', 'subte', 'quilombo'
        ]
        
        # Cultura argentina específica
        self.cultura_argentina = [
            'mate', 'asado', 'empanadas', 'choripán', 'milanesas',
            'alfajores', 'dulce de leche', 'boca', 'river', 'maradona',
            'messi', 'tango', 'folklore', 'cuarteto', 'bariloche'
        ]
        
        # Horarios argentinos
        self.horarios_argentinos = [
            r'\d+hs', r'\d+ hs', 'hora argentina', 'ART', 'UTC-3'
        ]
        
        # Exclusiones automáticas (otros países)
        self.exclusiones = [
            'españa', 'méxico', 'chile', 'colombia', 'perú', 'uruguay',
            'bolivia', 'venezuela', 'ecuador', 'paraguay', 'brasil',
            'madrid', 'barcelona', 'cdmx', 'bogotá', 'lima', 'santiago'
        ]
    
    def detectar_explicitamente(self, texto: str) -> Optional[Dict]:
        """Detección explícita de argentinidad"""
        texto_lower = texto.lower()
        
        # 1. Menciona Argentina directamente
        if any(palabra in texto_lower for palabra in ['argentina', 'argentino', 'argentinos']):
            return {
                'metodo': 'explicito',
                'argentino': True,
                'confianza': 95,
                'indicador': 'menciona_argentina'
            }
        
        # 2. Códigos locales argentinos
        for codigo, provincia in self.config.CODIGOS_ARGENTINOS.items():
            if codigo.lower() in texto_lower:
                return {
                    'metodo': 'codigo_local',
                    'argentino': True,
                    'confianza': 92,
                    'provincia': provincia,
                    'codigo': codigo
                }
        
        # 3. Provincias argentinas
        for provincia in self.config.PROVINCIAS_ARGENTINAS:
            if provincia.lower() in texto_lower:
                return {
                    'metodo': 'provincia',
                    'argentino': True,
                    'confianza': 88,
                    'provincia': provincia
                }
        
        # 4. Exclusión automática
        for exclusion in self.exclusiones:
            if exclusion in texto_lower:
                return {
                    'metodo': 'exclusion',
                    'argentino': False,
                    'motivo': f'menciona_{exclusion}'
                }
        
        return None
    
    def analizar_patrones_culturales(self, texto: str) -> Dict:
        """Análisis de patrones culturales argentinos"""
        texto_lower = texto.lower()
        score = 0
        indicadores = []
        
        # Voseo (peso alto)
        voseo_count = sum(1 for pattern in self.voseo_patterns 
                         if re.search(pattern, texto_lower))
        if voseo_count > 0:
            score += voseo_count * 15
            indicadores.append(f'voseo({voseo_count})')
        
        # Jerga argentina
        jerga_count = sum(1 for jerga in self.jerga_argentina 
                         if jerga in texto_lower)
        if jerga_count > 0:
            score += jerga_count * 8
            indicadores.append(f'jerga({jerga_count})')
        
        # Cultura argentina
        cultura_count = sum(1 for cultura in self.cultura_argentina 
                           if cultura in texto_lower)
        if cultura_count > 0:
            score += cultura_count * 6
            indicadores.append(f'cultura({cultura_count})')
        
        # Horarios argentinos
        horario_count = sum(1 for pattern in self.horarios_argentinos 
                           if re.search(pattern, texto_lower))
        if horario_count > 0:
            score += horario_count * 10
            indicadores.append(f'horarios({horario_count})')
        
        # Convertir score a porcentaje de confianza
        confianza = min(90, score * 2)
        
        return {
            'metodo': 'patrones_culturales',
            'argentino': confianza >= 70,
            'confianza': confianza,
            'score': score,
            'indicadores': indicadores
        }
    
    def detectar_region(self, texto: str) -> Dict:
        """Detectar región específica dentro de Argentina"""
        texto_lower = texto.lower()
        
        regiones = {
            'rioplatense': {
                'indicadores': ['che', 'boludo', 'subte', 'bondi', 'laburo'],
                'provincias': ['Buenos Aires', 'CABA', 'Entre Ríos']
            },
            'cuyo': {
                'indicadores': ['mza', 'vino', 'vendimia', 'cordillera', 'aconcagua'],
                'provincias': ['Mendoza', 'San Juan', 'San Luis']
            },
            'noa': {
                'indicadores': ['sla', 'folklore', 'empanadas', 'locro', 'zamba'],
                'provincias': ['Salta', 'Jujuy', 'Tucumán', 'Santiago del Estero']
            },
            'patagonia': {
                'indicadores': ['brc', 'nieve', 'lago', 'cordero', 'bariloche', 'ushuaia'],
                'provincias': ['Neuquén', 'Río Negro', 'Chubut', 'Santa Cruz', 'Tierra del Fuego']
            }
        }
        
        for region, datos in regiones.items():
            score = sum(1 for indicador in datos['indicadores'] 
                       if indicador in texto_lower)
            if score >= 2:
                return {
                    'region': region,
                    'provincias_probables': datos['provincias'],
                    'confianza': min(90, 60 + score * 10),
                    'indicadores': [ind for ind in datos['indicadores'] 
                                  if ind in texto_lower]
                }
        
        return {'region': 'Argentina - Región incierta', 'confianza': 50}
    
    def analizar_argentinidad_completa(self, texto_completo: str) -> Dict:
        """Análisis completo de argentinidad"""
        # Paso 1: Detección explícita
        resultado_explicito = self.detectar_explicitamente(texto_completo)
        if resultado_explicito:
            if resultado_explicito['argentino']:
                # Detectar región específica
                region = self.detectar_region(texto_completo)
                resultado_explicito.update(region)
            return resultado_explicito
        
        # Paso 2: Análisis de patrones culturales
        resultado_patrones = self.analizar_patrones_culturales(texto_completo)
        
        if resultado_patrones['confianza'] >= 70:
            # Detectar región si es argentino
            if resultado_patrones['argentino']:
                region = self.detectar_region(texto_completo)
                resultado_patrones.update(region)
            return resultado_patrones
        
        # Paso 3: Insuficiente evidencia
        return {
            'argentino': False,
            'metodo': 'insuficiente_evidencia',
            'confianza': resultado_patrones['confianza']
        }

# =============================================================================
# CLIENTE YOUTUBE API OPTIMIZADO
# =============================================================================

class YouTubeAPIClient:
    """Cliente optimizado para YouTube API con gestión de cuota"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.logger = LoggerProyecto().logger
        self.llamadas_usadas = 0
        self.youtube = build('youtube', 'v3', developerKey=self.config.YOUTUBE_API_KEY)
        self.cache = CacheInteligente()
    
    def puede_hacer_llamada(self, costo: int = 1) -> bool:
        """Verificar si se puede hacer una llamada API"""
        limite_efectivo = self.config.CUOTA_DIARIA_LIMITE - self.config.CUOTA_BUFFER_SEGURIDAD
        return (self.llamadas_usadas + costo) <= limite_efectivo
    
    def registrar_llamada(self, costo: int = 1):
        """Registrar uso de cuota"""
        self.llamadas_usadas += costo
        
        # Log cuando se acerca al límite
        if self.llamadas_usadas > (self.config.CUOTA_DIARIA_LIMITE * 0.8):
            self.logger.warning(
                f"⚠️  Cuota alta: {self.llamadas_usadas}/{self.config.CUOTA_DIARIA_LIMITE}"
            )
    
    def buscar_canales(self, termino: str, max_paginas: int = 50) -> List[Dict]:
        """Búsqueda profunda de canales con paginación"""
        canales_encontrados = []
        next_page_token = None
        
        for pagina in range(1, max_paginas + 1):
            if not self.puede_hacer_llamada():
                self.logger.warning(f"⚠️  Cuota agotada en página {pagina}")
                break
            
            try:
                # Verificar cache primero
                cache_key = f"search_{termino}_{pagina}"
                resultado_cache = self.cache.obtener(cache_key)
                
                if resultado_cache:
                    response = resultado_cache
                else:
                    # Llamada a API
                    search_response = self.youtube.search().list(
                        q=termino,
                        part='snippet',
                        type='channel',
                        maxResults=50,
                        pageToken=next_page_token,
                        regionCode='AR'  # Sesgo hacia Argentina
                    ).execute()
                    
                    response = search_response
                    self.cache.guardar(cache_key, response)
                    self.registrar_llamada(1)
                
                canales_pagina = response.get('items', [])
                canales_encontrados.extend(canales_pagina)
                
                self.logger.info(
                    f"🔍 '{termino}' - Página {pagina}: {len(canales_pagina)} canales"
                )
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                
                # Pausa para evitar rate limiting
                time.sleep(0.1)
                
            except HttpError as e:
                self.logger.error(f"Error en búsqueda: {e}")
                break
            except Exception as e:
                self.logger.error(f"Error inesperado: {e}")
                break
        
        return canales_encontrados
    
    def obtener_detalles_canal(self, canal_id: str) -> Optional[Dict]:
        """Obtener detalles completos de un canal"""
        if not self.puede_hacer_llamada(3):  # Costo mayor por información completa
            return None
        
        # Verificar cache
        cache_key = f"channel_{canal_id}"
        resultado_cache = self.cache.obtener(cache_key)
        
        if resultado_cache:
            return resultado_cache
        
        try:
            # Obtener información del canal
            channel_response = self.youtube.channels().list(
                part='snippet,statistics,contentDetails,status,brandingSettings',
                id=canal_id
            ).execute()
            
            self.registrar_llamada(3)
            
            if not channel_response.get('items'):
                return None
            
            canal_info = channel_response['items'][0]
            
            # Verificar si tiene streaming
            tiene_streaming = self.verificar_streaming(canal_id)
            canal_info['tiene_streaming'] = tiene_streaming
            
            # Cache del resultado
            self.cache.guardar(cache_key, canal_info)
            
            return canal_info
            
        except HttpError as e:
            self.logger.error(f"Error obteniendo detalles de {canal_id}: {e}")
            return None
    
    def verificar_streaming(self, canal_id: str) -> bool:
        """Verificar si el canal tiene capacidad de streaming"""
        try:
            # Buscar videos en vivo o próximos
            search_response = self.youtube.search().list(
                channelId=canal_id,
                part='snippet',
                eventType='live',
                type='video',
                maxResults=1
            ).execute()
            
            if search_response.get('items'):
                return True
            
            # Buscar en streams programados
            search_response = self.youtube.search().list(
                channelId=canal_id,
                part='snippet',
                eventType='upcoming', 
                type='video',
                maxResults=1
            ).execute()
            
            return bool(search_response.get('items'))
            
        except:
            # Si hay error, asumir que no tiene streaming
            return False
    
    def obtener_videos_recientes(self, canal_id: str, max_videos: int = 10) -> List[Dict]:
        """Obtener videos recientes del canal para análisis"""
        if not self.puede_hacer_llamada():
            return []
        
        try:
            search_response = self.youtube.search().list(
                channelId=canal_id,
                part='snippet',
                order='date',
                type='video',
                maxResults=max_videos
            ).execute()
            
            self.registrar_llamada(1)
            return search_response.get('items', [])
            
        except HttpError as e:
            self.logger.error(f"Error obteniendo videos de {canal_id}: {e}")
            return []

# =============================================================================
# SISTEMA DE CACHE INTELIGENTE
# =============================================================================

class CacheInteligente:
    """Sistema de cache para evitar llamadas duplicadas a la API"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.cache_file = os.path.join(self.config.CACHE_DIR, 'api_cache.json')
        self.cache_data = self.cargar_cache()
    
    def cargar_cache(self) -> Dict:
        """Cargar cache desde archivo"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def guardar_cache(self):
        """Guardar cache a archivo"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error guardando cache: {e}")
    
    def obtener(self, key: str) -> Optional[Dict]:
        """Obtener valor del cache"""
        cache_entry = self.cache_data.get(key)
        if cache_entry:
            # Verificar si no ha expirado (24 horas)
            timestamp = cache_entry.get('timestamp', 0)
            if time.time() - timestamp < 86400:  # 24 horas
                return cache_entry.get('data')
        return None
    
    def guardar(self, key: str, data: Dict):
        """Guardar valor en cache"""
        self.cache_data[key] = {
            'data': data,
            'timestamp': time.time()
        }
        
        # Guardar a archivo cada 10 entradas
        if len(self.cache_data) % 10 == 0:
            self.guardar_cache()

# =============================================================================
# BASE DE DATOS SQLITE
# =============================================================================

class DatabaseManager:
    """Gestor de base de datos para streamers argentinos"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.db_path = self.config.DB_FILE
        self.init_database()
    
    def init_database(self):
        """Inicializar base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS streamers (
                canal_id TEXT PRIMARY KEY,
                nombre_canal TEXT NOT NULL,
                categoria TEXT,
                provincia TEXT,
                ciudad TEXT,
                suscriptores INTEGER,
                certeza REAL,
                metodo_deteccion TEXT,
                url TEXT,
                fecha_deteccion TEXT,
                ultima_actividad TEXT,
                tiene_streaming BOOLEAN,
                descripcion TEXT,
                pais_detectado TEXT,
                datos_completos TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS busquedas_realizadas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                termino TEXT,
                paginas_exploradas INTEGER,
                canales_encontrados INTEGER,
                fecha_busqueda TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS estadisticas_diarias (
                fecha TEXT PRIMARY KEY,
                canales_analizados INTEGER,
                streamers_encontrados INTEGER,
                llamadas_api INTEGER,
                distribucion_provincial TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def existe_canal(self, canal_id: str) -> bool:
        """Verificar si el canal ya fue procesado"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT 1 FROM streamers WHERE canal_id = ?', (canal_id,))
        resultado = cursor.fetchone() is not None
        
        conn.close()
        return resultado
    
    def guardar_streamer(self, streamer: StreamerData):
        """Guardar streamer en base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO streamers 
            (canal_id, nombre_canal, categoria, provincia, ciudad, suscriptores,
             certeza, metodo_deteccion, url, fecha_deteccion, ultima_actividad,
             tiene_streaming, descripcion, pais_detectado, datos_completos)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            streamer.canal_id, streamer.nombre_canal, streamer.categoria,
            streamer.provincia, streamer.ciudad, streamer.suscriptores,
            streamer.certeza, streamer.metodo_deteccion, streamer.url,
            streamer.fecha_deteccion, streamer.ultima_actividad,
            streamer.tiene_streaming, streamer.descripcion,
            streamer.pais_detectado, json.dumps(asdict(streamer))
        ))
        
        conn.commit()
        conn.close()
    
    def obtener_estadisticas(self) -> Dict:
        """Obtener estadísticas generales"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total de streamers
        cursor.execute('SELECT COUNT(*) FROM streamers WHERE pais_detectado = "Argentina"')
        total_streamers = cursor.fetchone()[0]
        
        # Por provincia
        cursor.execute('''
            SELECT provincia, COUNT(*) 
            FROM streamers 
            WHERE pais_detectado = "Argentina"
            GROUP BY provincia 
            ORDER BY COUNT(*) DESC
        ''')
        por_provincia = dict(cursor.fetchall())
        
        # Por categoría
        cursor.execute('''
            SELECT categoria, COUNT(*) 
            FROM streamers 
            WHERE pais_detectado = "Argentina"
            GROUP BY categoria 
            ORDER BY COUNT(*) DESC
        ''')
        por_categoria = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_streamers': total_streamers,
            'por_provincia': por_provincia,
            'por_categoria': por_categoria
        }
    
    def exportar_csv(self):
        """Exportar datos a CSV final"""
        conn = sqlite3.connect(self.db_path)
        
        with open(self.config.CSV_FINAL, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'Canal', 'Categoria', 'Provincia', 'Ciudad', 'Suscriptores',
                'Certeza', 'Metodo_Deteccion', 'URL', 'Fecha_Deteccion'
            ])
            
            cursor = conn.cursor()
            cursor.execute('''
                SELECT nombre_canal, categoria, provincia, ciudad, suscriptores,
                       certeza, metodo_deteccion, url, fecha_deteccion
                FROM streamers 
                WHERE pais_detectado = "Argentina"
                ORDER BY suscriptores DESC
            ''')
            
            for row in cursor.fetchall():
                writer.writerow(row)
        
        conn.close()

# =============================================================================
# ANALIZADOR DE CANALES
# =============================================================================

class AnalizadorCanal:
    """Analizador completo de canales para determinar si son streamers argentinos"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.detector = DetectorArgentinidad()
        self.youtube = YouTubeAPIClient()
        self.db = DatabaseManager()
        self.logger = LoggerProyecto().logger
    
    def categorizar_canal(self, descripcion: str, videos_recientes: List[Dict]) -> str:
        """Determinar categoría del canal basado en contenido"""
        texto_completo = descripcion.lower()
        
        # Agregar títulos de videos recientes al análisis
        for video in videos_recientes[:5]:
            titulo = video.get('snippet', {}).get('title', '').lower()
            texto_completo += f" {titulo}"
        
        # Categorías con palabras clave
        categorias = {
            'Gaming': ['gaming', 'games', 'juegos', 'videojuegos', 'twitch', 'stream gaming', 'gamer'],
            'Charlas': ['charlas', 'entrevistas', 'podcast', 'conversaciones', 'talks', 'bunker', 'mesa redonda'],
            'Música': ['música', 'music', 'covers', 'cantante', 'banda', 'artista', 'musical'],
            'Cocina': ['cocina', 'recetas', 'cooking', 'chef', 'gastronomía', 'comida'],
            'Educativo': ['educativo', 'tutorial', 'enseñanza', 'clases', 'aprende', 'educación'],
            'Entretenimiento': ['entretenimiento', 'humor', 'comedia', 'sketches', 'variedades'],
            'Deportes': ['deportes', 'fútbol', 'sports', 'deporte', 'análisis deportivo'],
            'Tecnología': ['tecnología', 'tech', 'programación', 'código', 'desarrollo'],
            'Arte': ['arte', 'dibujo', 'pintura', 'diseño', 'manualidades', 'creatividad'],
            'Viajes': ['viajes', 'turismo', 'travel', 'aventura', 'lugares']
        }
        
        # Contar coincidencias por categoría
        scores = {}
        for categoria, palabras in categorias.items():
            score = sum(1 for palabra in palabras if palabra in texto_completo)
            if score > 0:
                scores[categoria] = score
        
        # Devolver categoría con mayor score
        if scores:
            return max(scores.items(), key=lambda x: x[1])[0]
        
        return 'General'
    
    def analizar_canal_completo(self, canal_info: Dict) -> Optional[StreamerData]:
        """Análisis completo de un canal"""
        try:
            snippet = canal_info.get('snippet', {})
            statistics = canal_info.get('statistics', {})
            
            canal_id = canal_info.get('id', '')
            nombre_canal = snippet.get('title', 'Sin nombre')
            descripcion = snippet.get('description', '')
            
            # Verificar filtros mínimos
            suscriptores = int(statistics.get('subscriberCount', 0))
            if suscriptores < self.config.MIN_SUSCRIPTORES:
                return None
            
            # Verificar si tiene streaming
            tiene_streaming = canal_info.get('tiene_streaming', False)
            if not tiene_streaming:
                return None
            
            # Obtener videos recientes para análisis
            videos_recientes = self.youtube.obtener_videos_recientes(canal_id)
            
            # Construir texto completo para análisis
            texto_completo = f"{nombre_canal} {descripcion}"
            for video in videos_recientes:
                video_snippet = video.get('snippet', {})
                titulo = video_snippet.get('title', '')
                desc_video = video_snippet.get('description', '')
                texto_completo += f" {titulo} {desc_video}"
            
            # Análisis de argentinidad
            resultado_argentinidad = self.detector.analizar_argentinidad_completa(texto_completo)
            
            if not resultado_argentinidad.get('argentino', False):
                return None
            
            if resultado_argentinidad.get('confianza', 0) < self.config.MIN_CERTEZA_ARGENTINA:
                return None
            
            # Determinar provincia y ciudad
            provincia = resultado_argentinidad.get('provincia', 'Provincia Incierta')
            region = resultado_argentinidad.get('region', '')
            if provincia == 'Provincia Incierta' and region:
                provincia = f"Argentina - {region}"
            
            ciudad = 'Ciudad Incierta'  # Se podría mejorar con análisis más específico
            
            # Categorizar canal
            categoria = self.categorizar_canal(descripcion, videos_recientes)
            
            # Crear objeto StreamerData
            streamer = StreamerData(
                canal_id=canal_id,
                nombre_canal=nombre_canal,
                categoria=categoria,
                provincia=provincia,
                ciudad=ciudad,
                suscriptores=suscriptores,
                certeza=resultado_argentinidad.get('confianza', 0),
                metodo_deteccion=resultado_argentinidad.get('metodo', 'desconocido'),
                url=f"https://youtube.com/channel/{canal_id}",
                fecha_deteccion=datetime.now().strftime('%Y-%m-%d'),
                ultima_actividad=snippet.get('publishedAt', ''),
                tiene_streaming=tiene_streaming,
                descripcion=descripcion[:500],  # Limitar tamaño
                pais_detectado='Argentina'
            )
            
            return streamer
            
        except Exception as e:
            self.logger.error(f"Error analizando canal {canal_info.get('id', 'desconocido')}: {e}")
            return None

# =============================================================================
# ESTRATEGIAS DE BÚSQUEDA ANTI-SESGO
# =============================================================================

class EstrategiaBusqueda:
    """Estrategias de búsqueda específicas para combatir el sesgo geográfico"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.logger = LoggerProyecto().logger
    
    def obtener_terminos_fase_1(self) -> List[Tuple[str, int]]:
        """Términos generales con máxima profundidad (Días 1-30)"""
        return [
            # Términos amplios - profundidad moderada para CABA
            ('argentina', 25),
            ('argentino', 25), 
            ('argentinos', 20),
            ('streaming argentina', 30),
            ('youtuber argentina', 25),
            ('canal argentino', 20),
            ('gaming argentina', 25),
            ('en vivo argentina', 20),
        ]
    
    def obtener_terminos_fase_2(self) -> List[Tuple[str, int]]:
        """Términos provinciales con profundidad variable (Días 31-60)"""
        terminos = []
        
        # Provincias grandes - profundidad alta
        provincias_grandes = ['Buenos Aires', 'Córdoba', 'Santa Fe', 'Mendoza']
        for provincia in provincias_grandes:
            terminos.extend([
                (f'streaming {provincia}', 40),
                (f'gaming {provincia}', 35),
                (f'youtuber {provincia}', 30),
                (f'en vivo {provincia}', 25)
            ])
        
        # Provincias medianas - profundidad muy alta
        provincias_medianas = ['Tucumán', 'Salta', 'Entre Ríos', 'Misiones', 'Chaco', 'Corrientes']
        for provincia in provincias_medianas:
            terminos.extend([
                (f'streaming {provincia}', 50),
                (f'gaming {provincia}', 45),
                (f'youtuber {provincia}', 40)
            ])
        
        # Provincias pequeñas - máxima profundidad
        provincias_pequenas = ['Catamarca', 'La Rioja', 'Formosa', 'San Luis', 'Tierra del Fuego']
        for provincia in provincias_pequenas:
            terminos.extend([
                (f'streaming {provincia}', 50),
                (f'gaming {provincia}', 50),
                (f'canal {provincia}', 50)
            ])
        
        return terminos
    
    def obtener_terminos_fase_3(self) -> List[Tuple[str, int]]:
        """Términos por códigos locales (Días 61-90)"""
        terminos = []
        
        for codigo, provincia in self.config.CODIGOS_ARGENTINOS.items():
            terminos.extend([
                (f'bunker {codigo.lower()}', 50),
                (f'charlas {codigo.lower()}', 50),
                (f'gaming {codigo.lower()}', 45),
                (f'streaming {codigo.lower()}', 45),
                (f'en vivo {codigo.lower()}', 40)
            ])
        
        return terminos
    
    def obtener_terminos_fase_4(self) -> List[Tuple[str, int]]:
        """Términos culturales híper-específicos (Días 91-120)"""
        return [
            # Combinaciones culturales regionales
            ('mate gaming', 30),
            ('folklore streaming', 35),
            ('tango en vivo', 25),
            ('asado live', 20),
            ('che gaming', 40),
            ('boludo streaming', 35),
            ('cuarteto en vivo', 30),
            ('chamamé live', 40),
            ('empanadas streaming', 25),
            ('vino gaming', 30),
            ('cordillera live', 25),
            ('patagonia streaming', 35),
            ('noa gaming', 30),
            ('cuyo en vivo', 25),
            ('litoral streaming', 30),
            ('pampa gaming', 25)
        ]

# =============================================================================
# MOTOR PRINCIPAL DE BÚSQUEDA
# =============================================================================

class MotorBusquedaStreaming:
    """Motor principal que ejecuta la búsqueda completa"""
    
    def __init__(self):
        self.config = ConfiguracionProyecto()
        self.youtube = YouTubeAPIClient()
        self.analizador = AnalizadorCanal()
        self.db = DatabaseManager()
        self.logger_sistema = LoggerProyecto()
        self.logger = self.logger_sistema.logger
        self.estrategia = EstrategiaBusqueda()
        
        # Estadísticas de sesión
        self.canales_procesados = set()
        self.streamers_encontrados_hoy = 0
    
    def filtrar_canales_duplicados(self, canales: List[Dict]) -> List[Dict]:
        """Filtrar canales duplicados y ya procesados"""
        canales_unicos = []
        ids_vistos = set()
        
        for canal in canales:
            canal_id = canal.get('id', {}).get('channelId') or canal.get('id')
            if canal_id and canal_id not in ids_vistos and canal_id not in self.canales_procesados:
                if not self.db.existe_canal(canal_id):
                    canales_unicos.append(canal)
                    ids_vistos.add(canal_id)
                    self.canales_procesados.add(canal_id)
        
        return canales_unicos
    
    def ejecutar_busqueda_termino(self, termino: str, max_paginas: int) -> int:
        """Ejecutar búsqueda para un término específico"""
        self.logger.info(f"🔍 INICIANDO BÚSQUEDA: '{termino}' (hasta {max_paginas} páginas)")
        
        # Buscar canales
        canales_encontrados = self.youtube.buscar_canales(termino, max_paginas)
        
        if not canales_encontrados:
            self.logger.warning(f"⚠️  No se encontraron canales para '{termino}'")
            return 0
        
        # Filtrar duplicados
        canales_unicos = self.filtrar_canales_duplicados(canales_encontrados)
        
        self.logger.info(f"📊 '{termino}': {len(canales_encontrados)} encontrados, {len(canales_unicos)} únicos")
        
        streamers_encontrados = 0
        
        # Analizar cada canal
        for i, canal_snippet in enumerate(canales_unicos, 1):
            if not self.youtube.puede_hacer_llamada(3):
                self.logger.warning("⚠️  Cuota agotada, pausando análisis")
                break
            
            try:
                canal_id = canal_snippet.get('id', {}).get('channelId') or canal_snippet.get('id')
                if not canal_id:
                    continue
                
                # Obtener detalles completos
                canal_completo = self.youtube.obtener_detalles_canal(canal_id)
                if not canal_completo:
                    continue
                
                # Analizar si es streamer argentino
                streamer = self.analizador.analizar_canal_completo(canal_completo)
                
                if streamer:
                    # Guardar en base de datos
                    self.db.guardar_streamer(streamer)
                    
                    # Log del hallazgo
                    self.logger_sistema.log_streamer_encontrado(streamer)
                    
                    streamers_encontrados += 1
                    self.streamers_encontrados_hoy += 1
                
                # Log progreso cada 50 canales
                if i % 50 == 0:
                    self.logger.info(
                        f"📈 Progreso '{termino}': {i}/{len(canales_unicos)} analizados, "
                        f"{streamers_encontrados} streamers encontrados"
                    )
                
            except Exception as e:
                self.logger.error(f"Error procesando canal: {e}")
                continue
        
        self.logger.info(
            f"✅ COMPLETADO '{termino}': {streamers_encontrados} streamers argentinos encontrados"
        )
        
        return streamers_encontrados
    
    def ejecutar_fase(self, numero_fase: int, terminos: List[Tuple[str, int]]) -> Dict:
        """Ejecutar una fase completa de búsqueda"""
        self.logger_sistema.log_inicio_fase(
            f"FASE {numero_fase}",
            f"Búsqueda con {len(terminos)} términos específicos"
        )
        
        resultados_fase = {
            'terminos_procesados': 0,
            'streamers_encontrados': 0,
            'total_canales_analizados': 0
        }
        
        for termino, max_paginas in terminos:
            if not self.youtube.puede_hacer_llamada(100):  # Reservar cuota mínima
                self.logger.warning("⚠️  Cuota insuficiente para continuar fase")
                break
            
            try:
                streamers_termino = self.ejecutar_busqueda_termino(termino, max_paginas)
                
                resultados_fase['terminos_procesados'] += 1
                resultados_fase['streamers_encontrados'] += streamers_termino
                
                # Pausa entre términos
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error en término '{termino}': {e}")
                continue
        
        self.logger.info(
            f"🎯 FASE {numero_fase} COMPLETADA: "
            f"{resultados_fase['streamers_encontrados']} streamers encontrados"
        )
        
        return resultados_fase
    
    def ejecutar_dia_completo(self) -> Dict:
        """Ejecutar búsqueda completa de un día"""
        inicio = time.time()
        self.logger.info("🚀 INICIANDO EJECUCIÓN DIARIA")
        self.logger.info(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Determinar qué fase ejecutar según el día del proyecto
        # Para simplificar, rotamos entre fases
        dia_año = datetime.now().timetuple().tm_yday
        fase_actual = (dia_año % 4) + 1
        
        resultados_dia = {'fase_ejecutada': fase_actual, 'streamers_encontrados': 0}
        
        try:
            if fase_actual == 1:
                terminos = self.estrategia.obtener_terminos_fase_1()
                resultados = self.ejecutar_fase(1, terminos)
            elif fase_actual == 2:
                terminos = self.estrategia.obtener_terminos_fase_2()
                resultados = self.ejecutar_fase(2, terminos)
            elif fase_actual == 3:
                terminos = self.estrategia.obtener_terminos_fase_3()
                resultados = self.ejecutar_fase(3, terminos)
            else:
                terminos = self.estrategia.obtener_terminos_fase_4()
                resultados = self.ejecutar_fase(4, terminos)
            
            resultados_dia.update(resultados)
            
        except Exception as e:
            self.logger.error(f"Error crítico en ejecución diaria: {e}")
        
        # Estadísticas finales
        duracion = time.time() - inicio
        self.logger.info(f"⏱️  Ejecución completada en {duracion/60:.1f} minutos")
        self.logger.info(f"📊 Streamers encontrados hoy: {self.streamers_encontrados_hoy}")
        self.logger.info(f"🔥 Llamadas API usadas: {self.youtube.llamadas_usadas}")
        
        # Exportar CSV actualizado
        self.db.exportar_csv()
        
        # Log estadísticas generales
        estadisticas = self.db.obtener_estadisticas()
        self.logger.info(f"📈 TOTAL PROYECTO: {estadisticas['total_streamers']} streamers argentinos")
        
        self.logger_sistema.log_estadisticas_diarias()
        
        return resultados_dia

# =============================================================================
# SCRIPT PRINCIPAL
# =============================================================================

def main():
    """Función principal de ejecución"""
    try:
        # Verificar configuración
        config = ConfiguracionProyecto()
        
        if not config.YOUTUBE_API_KEY:
            print("❌ ERROR: Variable YOUTUBE_API_KEY no configurada")
            print("Configura tu API key: export YOUTUBE_API_KEY='tu_api_key'")
            return
        
        # Crear directorios necesarios
        config.crear_directorios()
        
        # Inicializar motor de búsqueda
        motor = MotorBusquedaStreaming()
        
        # Ejecutar día completo
        resultados = motor.ejecutar_dia_completo()
        
        print("\n" + "="*80)
        print("🎯 CARTOGRAFÍA STREAMING ARGENTINO - EJECUCIÓN COMPLETADA")
        print("="*80)
        print(f"Fase ejecutada: {resultados['fase_ejecutada']}")
        print(f"Streamers encontrados hoy: {resultados.get('streamers_encontrados', 0)}")
        print(f"CSV actualizado: {config.CSV_FINAL}")
        print("="*80)
        
    except KeyboardInterrupt:
        print("\n⚠️  Ejecución interrumpida por el usuario")
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
