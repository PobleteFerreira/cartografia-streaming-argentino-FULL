#!/usr/bin/env python3
"""
Script para combinar dos CSVs de streamers sin duplicados
"""

import pandas as pd
from pathlib import Path

def merge_csvs():
    """Combinar CSVs existente y nuevo"""
    
    # Rutas de archivos
    csv_existente = "data/streamers_argentinos.csv"  # Tu CSV con 85 canales
    csv_nuevo = "streamers_argentinos.csv"           # CSV del artifact con 60 canales
    csv_combinado = "data/streamers_argentinos_combinado.csv"
    
    print("🔍 Verificando archivos...")
    
    # Verificar que existen
    if not Path(csv_existente).exists():
        print(f"❌ No encontrado: {csv_existente}")
        return
    
    if not Path(csv_nuevo).exists():
        print(f"❌ No encontrado: {csv_nuevo}")
        print("💡 Asegúrate de que el CSV del artifact esté en la raíz del proyecto")
        return
    
    print("✅ Ambos archivos encontrados")
    
    # Leer CSVs
    print("📖 Leyendo CSV existente...")
    df_existente = pd.read_csv(csv_existente)
    print(f"   📊 {len(df_existente)} canales existentes")
    
    print("📖 Leyendo CSV nuevo...")
    df_nuevo = pd.read_csv(csv_nuevo)
    print(f"   📊 {len(df_nuevo)} canales nuevos")
    
    # Combinar
    print("🔄 Combinando CSVs...")
    df_combinado = pd.concat([df_existente, df_nuevo], ignore_index=True)
    print(f"   📊 {len(df_combinado)} canales total (con posibles duplicados)")
    
    # Eliminar duplicados basado en canal_id
    print("🧹 Eliminando duplicados...")
    df_final = df_combinado.drop_duplicates(subset=['canal_id'], keep='first')
    duplicados_removidos = len(df_combinado) - len(df_final)
    print(f"   ❌ {duplicados_removidos} duplicados removidos")
    print(f"   ✅ {len(df_final)} canales únicos finales")
    
    # Guardar resultado
    print(f"💾 Guardando en: {csv_combinado}")
    df_final.to_csv(csv_combinado, index=False, encoding='utf-8')
    
    print("🎉 ¡Combinación completada!")
    print(f"📁 Archivo final: {csv_combinado}")
    
    # Mostrar resumen
    print("\n📊 RESUMEN:")
    print(f"   📈 Canales originales: {len(df_existente)}")
    print(f"   📈 Canales nuevos únicos: {len(df_final) - len(df_existente)}")
    print(f"   📈 Total final: {len(df_final)}")
    
    # Mostrar algunos canales nuevos añadidos
    canales_nuevos_ids = set(df_nuevo['canal_id']) - set(df_existente['canal_id'])
    if canales_nuevos_ids:
        print(f"\n🆕 Primeros 5 canales nuevos añadidos:")
        canales_nuevos = df_final[df_final['canal_id'].isin(list(canales_nuevos_ids))]
        for i, (_, canal) in enumerate(canales_nuevos.head().iterrows()):
            print(f"   {i+1}. {canal['nombre_canal']} ({canal['provincia']}) - {canal['suscriptores']:,} subs")

if __name__ == "__main__":
    merge_csvs()
