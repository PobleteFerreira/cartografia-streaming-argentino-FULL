#!/usr/bin/env python3
"""
CARTOGRAFÍA STREAMING ARGENTINO - Versión avanzada, robusta y segura de cuotas
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

# --- CONFIGURACIÓN Y CONSTANTES ---
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

# --- DATACLASS PARA LOS DATOS DE STREAMERS ---
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

# --- LOGGING ---
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

# --- CLASES AVANZADAS DEL PROYECTO (no modifiques) ---
# Pega aquí tus clases: ArgentineDetector, StreamingDetector, YouTubeClient, QuotaTracker, APICache, DataManager, ChannelAnalyzer, SearchStrategy
# Si las tienes en otros archivos, solo importa aquí. Si están aquí, déjalas igual que en tu main.py anterior.
# (Por razones de espacio y claridad, asumo que ya las tienes bien en tu main.py o en imports)

# --- ENGINE AJUSTADO PARA LARGA DURACIÓN, CUIDADO DE CUOTA Y SIN BUCLES ---
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
        # AJUSTA AQUÍ LOS LÍMITES SEGÚN TU NECESIDAD PARA MÁS DÍAS:
        self.max_pages_per_query = 2      # Menos páginas por término
        self.max_channels_per_term = 15   # Menos canales nuevos por término por día
        self.max_videos_per_channel = 5   # Mantén profundidad de análisis

    def process_search_term(self, term: str, max_pages: int) -> int:
        if self.apis_exhausted:
            self.logger.warning(f"⚠️ Saltando '{term}' - APIs agotadas")
            return 0
        try:
            channels = self.youtube.search_channels(term, max_pages)
        except Exception as e:
            if "Cuota diaria agotada" in str(e) or "Todas las APIs han agotado su cuota diaria" in str(e):
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
            channel_id = (channel.get('id', {}).get('channelId') or channel.get('id'))
            if channel_id and not self.data_manager.is_channel_processed(channel_id):
                new_channels.append(channel)
                self.data_manager.mark_channel_processed(channel_id)
        self.logger.info(f"📊 {len(channels)} encontrados, {len(new_channels)} nuevos")
        streamers_found = 0
        for i, channel in enumerate(new_channels[:self.max_channels_per_term], 1):
            if self.apis_exhausted:
                self.logger.warning("⚠️ Deteniendo análisis - APIs agotadas")
                break
            streamer = self.analyzer.analyze_channel(channel)
            if streamer:
                self.data_manager.save_streamer(streamer)
                self.logger_system.streamer_found(streamer)
                streamers_found += 1
                self.streamers_found_today += 1
            if i % 10 == 0:
                self.logger.info(
                    f"📈 Progreso: {i}/{len(new_channels)} analizados, "
                    f"{streamers_found} streamers encontrados"
                )
            time.sleep(1)
        return streamers_found

    def run_daily_execution(self) -> Dict:
        start_time = time.time()
        self.logger.info("="*80)
        self.logger.info("🚀 INICIANDO EJECUCIÓN DIARIA")
        self.logger.info(f"📅 Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}")
        self.logger.info(f"🔋 Cuota disponible: {self.youtube.quota_tracker.get_remaining():,}")
        self.logger.info("="*80)
        day_of_year = datetime.now().timetuple().tm_yday
        # AJUSTA AQUÍ EL LÍMITE DE DÍAS DEL PROYECTO
        if day_of_year > 120:
            self.logger.info("🏁 PROYECTO DE 120 DÍAS COMPLETADO")
            print("🏁 PROYECTO DE 120 DÍAS COMPLETADO")
            return {}
        current_phase = (day_of_year % 4) + 1
        queries = SearchStrategy.get_phase_queries(current_phase)
        for query, max_pages in queries:
            if self.apis_exhausted:
                self.logger.warning("⚠️ APIs agotadas - Deteniendo ejecución del día")
                break
            self.process_search_term(query, min(max_pages, self.max_pages_per_query))
        self.data_manager.save_processed_channels()
        self.youtube.cache.save_cache()
        self.logger_system.daily_summary()
        duration = (time.time() - start_time) / 60
        print(f"\n⏱️ Duración: {duration:.1f} minutos")

# --- AGREGAR CANAL MANUAL ---
def agregar_canal_manual():
    yt = YouTubeClient()
    data_manager = DataManager()
    print("\nAgregar canal manualmente")
    canal_id = input("ID del canal de YouTube: ").strip()
    canal = yt.get_channel_details(canal_id)
    if not canal:
        print("❌ No se pudo obtener el canal con ese ID.")
        return
    stats = canal.get('statistics', {})
    snippet = canal.get('snippet', {})
    suscriptores = stats.get('subscriberCount', '0')
    nombre = snippet.get('title', '')
    descripcion = snippet.get('description', '')
    print(f"Nombre detectado: {nombre} | Suscriptores: {suscriptores}")
    categoria = input("Categoría: ").strip()
    provincia = input("Provincia (opcional): ").strip()
    ciudad = input("Ciudad (opcional): ").strip()
    certeza = input("Certeza (ej: 100): ").strip()
    fecha = datetime.now().strftime('%Y-%m-%d')
    streamer = StreamerData(
        canal_id=canal_id,
        nombre_canal=nombre,
        categoria=categoria,
        provincia=provincia,
        ciudad=ciudad,
        suscriptores=int(suscriptores),
        certeza=float(certeza) if certeza else 100,
        metodo_deteccion="manual",
        indicadores_argentinidad=["manual"],
        url=f"https://youtube.com/channel/{canal_id}",
        fecha_deteccion=fecha,
        ultima_actividad=fecha,
        tiene_streaming=True,
        descripcion=descripcion[:500],
        pais_detectado="Argentina",
        videos_analizados=0
    )
    data_manager.save_streamer(streamer)
    print(f"✅ Canal {nombre} agregado correctamente.")

# --- MAIN ---
def main():
    print("1. Ejecución automática (búsqueda y análisis)")
    print("2. Agregar canal manualmente")
    opcion = input("Elige una opción: ").strip()
    if opcion == "2":
        agregar_canal_manual()
    else:
        engine = StreamingArgentinaEngine()
        engine.run_daily_execution()

if __name__ == "__main__":
    main()
