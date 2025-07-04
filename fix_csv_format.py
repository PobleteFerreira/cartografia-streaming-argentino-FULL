import pandas as pd
import re

def clean_text_for_csv(text):
    """Limpia texto para CSV sin romper el formato"""
    if not text:
        return ""
    
    text = str(text)
    # Remover saltos de línea problemáticos
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text)  # Múltiples espacios -> un espacio
    text = text.strip()
    
    # Si contiene comas, escapar con comillas
    if ',' in text:
        text = f'"{text.replace('"', '""')}"'
    
    return text

# Leer el CSV actual
print("📄 Leyendo CSV actual...")
df = pd.read_csv('data/streamers_argentinos.csv')
print(f"✅ Leídos {len(df)} canales")

# Limpiar todos los campos de texto
text_columns = ['nombre_canal', 'categoria', 'provincia', 'ciudad', 'descripcion', 'indicadores_argentinidad']

for col in text_columns:
    if col in df.columns:
        df[col] = df[col].apply(clean_text_for_csv)
        print(f"✅ Limpiado: {col}")

# Guardar con formato compatible (sin comillas en todo)
print("💾 Guardando con formato compatible...")
df.to_csv(
    'data/streamers_argentinos.csv', 
    index=False, 
    encoding='utf-8',
    sep=',',
    quoting=0,  # QUOTE_MINIMAL - solo cuando es necesario
    lineterminator='\n'
)

print("✅ CSV arreglado con formato compatible")

# Verificar el resultado
print("\n📋 Verificando header:")
with open('data/streamers_argentinos.csv', 'r', encoding='utf-8') as f:
    header = f.readline().strip()
    print(header)

print(f"\n📊 Total de líneas: {len(df) + 1}")  # +1 por el header
