import os
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno (.env si se usa en local)
load_dotenv()

# Configuración
XLSX_FILE = "CANALES ARGENTINOS FINALES.xlsx"  # Cambia el nombre si es necesario
CSV_FILE = "streamers_argentinos.csv"
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

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
        return {
            "canal_id": channel_id,
            "nombre_canal": snippet.get("title", ""),
            "categoria": "",  # Puedes rellenar si tienes lógica para categorizar
            "provincia": "",
            "ciudad": "",
            "suscriptores": stats.get("subscriberCount", ""),
            "certeza": 100,
            "metodo_deteccion": "manual",
            "indicadores_argentinidad": "",
            "url": f"https://youtube.com/channel/{channel_id}",
            "fecha_deteccion": datetime.now().strftime('%Y-%m-%d'),
            "ultima_actividad": snippet.get("publishedAt", ""),
            "tiene_streaming": "",
            "descripcion": snippet.get("description", ""),
            "pais_detectado": snippet.get("country", ""),
            "videos_analizados": ""
        }
    except Exception as e:
        print(f"Error obteniendo info para {channel_id}: {e}")
        return None

def main():
    # Leer IDs del XLSX
    canales_df = pd.read_excel(XLSX_FILE)
    # Asumimos que hay una columna 'id' o 'canal_id' (ajusta si se llama distinto)
    col = 'canal_id' if 'canal_id' in canales_df.columns else 'id'
    canal_ids = canales_df[col].dropna().astype(str).unique().tolist()

    # Leer IDs ya presentes en el CSV (si existe)
    if os.path.exists(CSV_FILE):
        existentes_df = pd.read_csv(CSV_FILE, dtype=str)
        ids_existentes = set(existentes_df['canal_id'])
    else:
        existentes_df = pd.DataFrame()
        ids_existentes = set()

    youtube = get_youtube_client()
    nuevos_registros = []

    print(f"Procesando {len(canal_ids)} canales…")
    for channel_id in canal_ids:
        if channel_id in ids_existentes:
            print(f"⏭️ Ya existe: {channel_id}")
            continue

        info = get_channel_info(youtube, channel_id)
        if info:
            nuevos_registros.append(info)
            print(f"✅ Agregado: {info['nombre_canal']} ({channel_id})")
        else:
            print(f"❌ No se encontró información para: {channel_id}")

    # Guardar en CSV
    if nuevos_registros:
        nuevos_df = pd.DataFrame(nuevos_registros)
        if not existentes_df.empty:
            final_df = pd.concat([existentes_df, nuevos_df], ignore_index=True)
        else:
            final_df = nuevos_df
        final_df.to_csv(CSV_FILE, index=False)
        print(f"\n✔️ Se agregaron {len(nuevos_registros)} canales nuevos a {CSV_FILE}")
    else:
        print("No hubo canales nuevos para agregar.")

if __name__ == "__main__":
    main()
