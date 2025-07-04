import os
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
from dotenv import load_dotenv
import re

# Cargar variables de entorno (.env si se usa en local)
load_dotenv()

# Configuración
XLSX_FILE = "CANALES ARGENTINOS FINALES.xlsx"
CSV_FILE = "data/streamers_argentinos.csv"
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

def clean_text(text):
    """Limpia texto para evitar problemas en CSV"""
    if not text:
        return ""
    
    # Convertir a string y limpiar
    text = str(text)
    
    # Remover caracteres problemáticos
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text)  # Múltiples espacios -> un espacio
    text = text.strip()
    
    # Escapar comillas dobles
    text = text.replace('"', '""')
    
    return text

def get_youtube_client():
    return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

def get_channel_info(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()
        items = response.get("items", [])
        if not items:
            return None
        
        info = items[0]
        snippet = info.get("snippet", {})
        stats = info.get("statistics", {})
        
        # Limpiar todos los campos de texto
        return {
            "canal_id": clean_text(channel_id),
            "nombre_canal": clean_text(snippet.get("title", "")),
            "categoria": "",
            "provincia": "",
            "ciudad": "",
            "suscriptores": clean_text(stats.get("subscriberCount", "")),
            "certeza": "100",
            "metodo_deteccion": "manual",
            "indicadores_argentinidad": "",
            "url": f"https://youtube.com/channel/{channel_id}",
            "fecha_deteccion": datetime.now().strftime('%Y-%m-%d'),
            "ultima_actividad": clean_text(snippet.get("publishedAt", "")),
            "tiene_streaming": "",
            "descripcion": clean_text(snippet.get("description", "")),
            "pais_detectado": clean_text(snippet.get("country", "")),
            "videos_analizados": ""
        }
    except Exception as e:
        print(f"Error obteniendo info para {channel_id}: {e}")
        return None

def read_channel_ids_from_excel():
    """Lee los channel IDs del archivo Excel"""
    try:
        df = pd.read_excel(XLSX_FILE)
        print(f"Columnas encontradas: {list(df.columns)}")
        
        # Buscar columnas conocidas
        for candidato in ['channel_id', 'canal_id', 'id', 'ID', 'ChannelID']:
            if candidato in df.columns:
                print(f"✅ Usando columna: {candidato}")
                ids = df[candidato].dropna().astype(str).unique().tolist()
                print(f"Se encontraron {len(ids)} IDs únicos")
                return ids
        
        # Auto-detectar columna con IDs
        for col in df.columns:
            if not df[col].isna().all():
                sample_values = df[col].dropna().astype(str).head(10)
                if any(len(str(val).strip()) >= 20 for val in sample_values):
                    print(f"🔍 Auto
