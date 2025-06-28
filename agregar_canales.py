import csv
import os
from datetime import datetime
from pathlib import Path
import re

def agregar_canal_manual(canal_input, categoria="Por determinar", provincia="Argentina"):
    """
    Agrega un canal manualmente al CSV de streamers argentinos
    
    Args:
        canal_input: URL del canal o Canal ID (UCxxxxxxxxx)
        categoria: Categoría del contenido (opcional)
        provincia: Provincia argentina (opcional)
    """
    
    # Extraer canal_id de la URL si es necesario
    if canal_input.startswith('http'):
        # Buscar el Canal ID en la URL
        canal_id_match = re.search(r'UC[a-zA-Z0-9_-]{22}', canal_input)
        if canal_id_match:
            canal_id = canal_id_match.group()
            url = canal_input
        else:
            print("❌ No se pudo extraer el Canal ID de la URL")
            return False
    elif canal_input.startswith('UC'):
        canal_id = canal_input
        url = f"https://youtube.com/channel/{canal_id}"
    else:
        print("❌ Formato inválido. Usa una URL de YouTube o un Canal ID que empiece con 'UC'")
        return False
    
    # Datos por defecto para canal manual
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    timestamp_actual = datetime.now().isoformat() + 'Z'
    
    # Pedir nombre del canal
    nombre_canal = input(f"📝 Nombre del canal (Enter para 'Canal {canal_id[:8]}'): ").strip()
    if not nombre_canal:
        nombre_canal = f"Canal {canal_id[:8]}"
    
    # Pedir suscriptores
    suscriptores_input = input("👥 Número de suscriptores (Enter para 0): ").strip()
    try:
        suscriptores = int(suscriptores_input) if suscriptores_input else 0
    except ValueError:
        suscriptores = 0
    
    # Pedir descripción
    descripcion = input("📄 Descripción del canal (Enter para vacío): ").strip()
    if not descripcion:
        descripcion = "Canal agregado manualmente"
    
    # Crear fila CSV
    nueva_fila = [
        canal_id,                           # canal_id
        nombre_canal,                       # nombre_canal
        categoria,                          # categoria
        provincia,                          # provincia
        "Por determinar",                   # ciudad
        suscriptores,                       # suscriptores
        100,                                # certeza (100% porque es manual)
        "manual_addition",                  # metodo_deteccion
        "agregado_manualmente",             # indicadores_argentinidad
        url,                                # url
        fecha_actual,                       # fecha_deteccion
        timestamp_actual,                   # ultima_actividad
        True,                               # tiene_streaming
        descripcion,                        # descripcion
        "Argentina",                        # pais_detectado
        0                                   # videos_analizados
    ]
    
    # Definir path del CSV
    csv_path = Path("data/streamers_argentinos.csv")
    
    # Verificar si el archivo existe
    if not csv_path.exists():
        print(f"❌ No se encontró el archivo CSV en: {csv_path}")
        print("🔍 Busca el archivo 'streamers_argentinos.csv' y verifica la ruta")
        return False
    
    # Verificar si el canal ya existe
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            for row in reader:
                if row and row[0] == canal_id:
                    print(f"⚠️ El canal {canal_id} ya existe en el CSV")
                    return False
    except Exception as e:
        print(f"❌ Error al leer el CSV: {e}")
        return False
    
    # Agregar al CSV
    try:
        with open(csv_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(nueva_fila)
        
        print(f"✅ Canal agregado exitosamente:")
        print(f"   📺 Nombre: {nombre_canal}")
        print(f"   🆔 ID: {canal_id}")
        print(f"   📁 Categoría: {categoria}")
        print(f"   👥 Suscriptores: {suscriptores:,}")
        print(f"   📄 CSV actualizado: {csv_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error al escribir en el CSV: {e}")
        return False

def agregar_multiple_canales():
    """
    Función interactiva para agregar múltiples canales
    """
    print("🚀 AGREGADOR DE CANALES MANUALES")
    print("=" * 40)
    
    while True:
        print("\n📋 Agregar nuevo canal:")
        canal_input = input("🔗 URL o Canal ID (UC...): ").strip()
        
        if not canal_input:
            break
            
        categoria = input("📂 Categoría (Gaming/Música/Cocina/etc): ").strip()
        if not categoria:
            categoria = "Por determinar"
            
        provincia = input("🗺️ Provincia (Enter para 'Argentina'): ").strip()
        if not provincia:
            provincia = "Argentina"
        
        print("\n" + "=" * 40)
        agregar_canal_manual(canal_input, categoria, provincia)
        
        continuar = input("\n❓ ¿Agregar otro canal? (s/n): ").lower()
        if continuar != 's' and continuar != 'si':
            break
    
    print("\n✅ ¡Proceso completado!")

# Ejemplo de uso
if __name__ == "__main__":
    # Modo interactivo
    agregar_multiple_canales()
    
    # O agregar un canal específico:
    # agregar_canal_manual("https://youtube.com/@ejemplo", "Gaming", "Buenos Aires")
