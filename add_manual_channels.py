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
                    print(f"🔍 Auto-detectada columna de IDs: {col}")
                    ids = df[col].dropna().astype(str).unique().tolist()
                    print(f"Se encontraron {len(ids)} IDs únicos")
                    return ids
        
        raise ValueError("No se pudo encontrar una columna con IDs de YouTube")
        
    except Exception as e:
        print(f"ERROR leyendo Excel: {e}")
        raise

def save_dataframe_safely(df, filename):
    """Guarda el DataFrame de manera segura"""
    try:
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Guardar con encoding específico y configuración CSV limpia
        df.to_csv(
            filename, 
            index=False, 
            encoding='utf-8-sig',  # Mejor compatibilidad
            sep=',',
            quotechar='"',
            quoting=1,  # QUOTE_ALL
            lineterminator='\n'
        )
        
        # Verificar que se guardó correctamente
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            test_df = pd.read_csv(filename)
            print(f"✅ Archivo guardado: {filename} ({file_size} bytes, {len(test_df)} filas)")
            return True
        else:
            print(f"❌ Error: archivo no se creó")
            return False
            
    except Exception as e:
        print(f"❌ Error guardando archivo: {e}")
        return False

def main():
    # Crear directorio data si no existe
    os.makedirs("data", exist_ok=True)
    
    # Verificar archivos y configuración
    if not os.path.exists(XLSX_FILE):
        print(f"❌ Error: No se encuentra el archivo {XLSX_FILE}")
        return
    
    if not YOUTUBE_API_KEY:
        print("❌ Error: No se encontró YOUTUBE_API_KEY")
        return
    
    print("✅ Archivos y configuración verificados")
    
    # Leer IDs del Excel
    print("\n📋 Leyendo IDs del archivo Excel...")
    canal_ids = read_channel_ids_from_excel()
    print(f"✅ Se encontraron {len(canal_ids)} IDs únicos")
    
    # Verificar archivo CSV existente
    if os.path.exists(CSV_FILE):
        print(f"\n📄 Leyendo archivo CSV existente...")
        existentes_df = pd.read_csv(CSV_FILE, dtype=str, encoding='utf-8-sig')
        ids_existentes = set(existentes_df['canal_id'])
        print(f"📄 {len(ids_existentes)} canales ya existen en {CSV_FILE}")
    else:
        existentes_df = pd.DataFrame()
        ids_existentes = set()
        print(f"📄 Creando nuevo archivo {CSV_FILE}")
    
    # Procesar canales
    youtube = get_youtube_client()
    nuevos_registros = []
    
    print(f"\n🔄 Procesando canales...")
    
    for i, channel_id in enumerate(canal_ids, 1):
        channel_id = str(channel_id).strip()
        if not channel_id or channel_id.lower() in ['nan', 'none']:
            continue
            
        if channel_id in ids_existentes:
            print(f"⏭️ Ya existe: {channel_id}")
            continue
        
        info = get_channel_info(youtube, channel_id)
        if info:
            nuevos_registros.append(info)
            print(f"✅ Agregado: {info['nombre_canal']} ({channel_id})")
        else:
            print(f"❌ No encontrado: {channel_id}")
    
    # Guardar resultados
    if nuevos_registros:
        print(f"\n💾 Guardando {len(nuevos_registros)} registros nuevos...")
        nuevos_df = pd.DataFrame(nuevos_registros)
        
        if not existentes_df.empty:
            final_df = pd.concat([existentes_df, nuevos_df], ignore_index=True)
        else:
            final_df = nuevos_df
        
        # Guardar con función mejorada
        if save_dataframe_safely(final_df, CSV_FILE):
            print(f"\n✔️ PROCESO COMPLETADO")
            print(f"📊 Se agregaron {len(nuevos_registros)} canales nuevos")
            print(f"📊 Total de canales en el archivo: {len(final_df)}")
        else:
            print(f"\n❌ Error al guardar el archivo")
    else:
        print("\nℹ️ No hubo canales nuevos para agregar.")

if __name__ == "__main__":
    main()
