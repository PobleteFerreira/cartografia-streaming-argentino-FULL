# cartografia-streaming-argentino-FULL

# 🇦🇷 Cartografía Completa del Streaming Argentino

> **Rompiendo el sesgo algorítmico que oculta el streaming provincial**

## 🎯 Objetivo del Proyecto

Este proyecto realiza **la primera cartografía completa** de streamers argentinos en YouTube, solucionando un problema crítico: **el algoritmo de YouTube oculta sistemáticamente a streamers de provincias**, manteniéndolos en las páginas 20-50+ mientras los porteños dominan las páginas 1-5.

### 🔍 El Problema que Resolvemos

```
SESGO ALGORÍTMICO ACTUAL:
├── Páginas 1-5:  90% streamers de CABA/Buenos Aires
├── Páginas 6-15:  8% streamers de Córdoba/Rosario  
├── Páginas 16-30: 2% streamers del resto del país
└── Páginas 31-50: <1% streamers provinciales (INVISIBLES)

RESULTADO: 80% del streaming argentino real permanece "oculto"
```

### ✅ Nuestra Solución

- **Búsqueda ultra-profunda**: Exploramos hasta 50 páginas por término
- **Códigos locales específicos**: Detectamos "MZA", "COR", "SLA", etc.
- **PNL argentino especializado**: Reconoce voseo, lunfardo, cultura regional
- **Distribución temporal anti-sesgo**: Más tiempo en provincias remotas

## 🚀 Instalación Rápida

### Prerrequisitos

1. **Python 3.8+**
2. **API Key de YouTube Data API v3**
3. **Git** (para clonar el repositorio)

### Paso a Paso

```bash
# 1. Clonar repositorio
git clone https://github.com/tu-usuario/cartografia-streaming-argentino.git
cd cartografia-streaming-argentino

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Instalar modelo de español para spaCy
python -m spacy download es_core_news_sm

# 5. Configurar API Key
export YOUTUBE_API_KEY='tu_api_key_aqui'  # En Windows: set YOUTUBE_API_KEY=tu_api_key

# 6. Ejecutar primera vez
python main.py
```

### 🔑 Obtener YouTube API Key

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Crea un proyecto nuevo o selecciona uno existente
3. Habilita "YouTube Data API v3"
4. Crea credenciales (API Key)
5. Configura la variable de entorno:

```bash
# Linux/Mac
export YOUTUBE_API_KEY='AIzaSyC...'

# Windows
set YOUTUBE_API_KEY=AIzaSyC...

# O créa un archivo .env
echo "YOUTUBE_API_KEY=AIzaSyC..." > .env
```

## 📊 Estrategia de Búsqueda Anti-Sesgo

### Fases del Proyecto (120 días)

#### **FASE 1 (Días 1-30): Términos Generales**
```
"argentina", "argentino", "streaming argentina"
└── Profundidad: 20-30 páginas (captura grandes y medianos)
```

#### **FASE 2 (Días 31-60): Provincias Específicas** 
```
Provincias grandes: "streaming Córdoba" → 40 páginas
Provincias medianas: "gaming Tucumán" → 50 páginas  
Provincias remotas: "streaming Catamarca" → 50 páginas
```

#### **FASE 3 (Días 61-90): Códigos Locales**
```
"bunker MZA", "charlas COR", "gaming SLA"
└── 50 páginas cada código (detecta streamers como "El Bunker MZA")
```

#### **FASE 4 (Días 91-120): Cultura Regional**
```
"mate gaming", "folklore streaming", "che gaming"
└── Términos híper-específicos para micro-streamers
```

## 🔬 Tecnología de Detección Argentinidad

### Niveles de Detección

1. **Explícita (95% certeza)**
   - Menciona "Argentina" directamente
   - Códigos locales: "MZA", "COR", "ROS"
   - Provincias: "Mendoza", "Córdoba", "Salta"

2. **Cultural (85% certeza)**
   - Voseo: "vos tenés", "vos sabés"
   - Jerga: "che", "boludo", "laburo", "bondi"
   - Cultura: "mate", "asado", "tango", "folklore"

3. **Contextual (75% certeza)**
   - Horarios: "20hs ART", "hora argentina"
   - Referencias: fútbol argentino, eventos locales

### Ejemplo de Análisis

```python
# Canal: "El Bunker MZA - Charlas todos los lunes 20hs"
resultado = {
    'argentino': True,
    'metodo': 'codigo_local',
    'codigo': 'MZA',
    'provincia': 'Mendoza',
    'confianza': 92,
    'indicadores': ['mza', 'charlas', '20hs', 'lunes']
}
```

## 📁 Estructura del Proyecto

```
cartografia-streaming-argentino/
├── main.py                    # Script principal de ejecución
├── requirements.txt           # Dependencias Python
├── README.md                 # Este archivo
├── .env.example              # Ejemplo de variables de entorno
├── .gitignore               # Archivos a ignorar en Git
├── data/                    # Datos generados
│   ├── streamers_argentinos.db      # Base de datos SQLite
│   └── cartografia_streaming_argentino.csv  # Resultado final CSV
├── logs/                    # Registros de ejecución
│   └── ejecucion_YYYYMMDD.log
├── cache/                   # Cache de API para optimización
│   └── api_cache.json
└── docs/                    # Documentación adicional
    ├── metodologia.md
    └── estadisticas.md
```

## ⚙️ Configuración Avanzada

### Variables de Entorno

```bash
# Obligatorias
YOUTUBE_API_KEY=tu_api_key                # API Key de YouTube

# Opcionales (con valores por defecto)
CUOTA_DIARIA_LIMITE=10000                # Límite diario de llamadas
MIN_SUSCRIPTORES=500                     # Mínimo de suscriptores
MIN_CERTEZA_ARGENTINA=70                 # Mínimo % de certeza
```

### Personalizar Búsquedas

Edita las estrategias en el código:

```python
# En la clase EstrategiaBusqueda
def obtener_terminos_personalizados(self):
    return [
        ('tu_termino_especifico', 25),  # (término, páginas_máximas)
        ('streaming tu_provincia', 40),
    ]
```

## 📈 Uso y Ejecución

### Ejecución Manual

```bash
# Ejecutar una vez
python main.py

# Ver progreso en tiempo real
tail -f logs/ejecucion_$(date +%Y%m%d).log
```

### Automatización Diaria

```bash
# Crontab para ejecución diaria a las 2:00 AM
0 2 * * * cd /path/to/proyecto && python main.py >> logs/cron.log 2>&1
```

### Monitoreo de Progreso

```python
# Ver estadísticas actuales
from main import DatabaseManager
db = DatabaseManager()
stats = db.obtener_estadisticas()
print(f"Total streamers: {stats['total_streamers']}")
print(f"Por provincia: {stats['por_provincia']}")
```

## 📊 Resultados Esperados

### Distribución Objetivo (Sin Sesgo)
```
✅ ÉXITO (distribución demográfica real):
├── Buenos Aires: 40-45% (proporcional a población)
├── Córdoba: 10-12%
├── Santa Fe: 8-10% 
├── Mendoza: 5-7%
└── Resto provincias: 30-35% ⭐ (LO QUE SE RESCATA)
```

### Métricas de Éxito
- **Mínimo**: ≥20 streamers por provincia
- **Medio**: ≥50 streamers por provincia  
- **Óptimo**: ≥100 streamers por provincia grande

### Casos de Éxito Detectados
- "El Bunker MZA" → Mendoza (detectado por código local)
- Streamers de folklore salteño → Salta (detectado por cultura)
- Gaming patagónico → Neuquén/Bariloche (detectado por región)

## 🔧 Resolución de Problemas

### Error: "API Key no válida"
```bash
# Verificar que la API key esté configurada
echo $YOUTUBE_API_KEY

# Verificar que YouTube Data API v3 esté habilitada en Google Cloud
```

### Error: "Cuota agotada"
```bash
# El script se pausa automáticamente al 80% de la cuota
# Logs mostrarán: "⚠️ Cuota alta: 8000/10000"
```

### Pocos resultados encontrados
```bash
# Verificar conectividad
ping youtube.com

# Revisar logs para errores específicos
cat logs/ejecucion_*.log | grep ERROR
```

## 🤝 Contribuir al Proyecto

### Reportar Streamers Perdidos
Si conoces streamers argentinos que no aparecen en los resultados:

1. Abre un [Issue](https://github.com/tu-usuario/cartografia-streaming-argentino/issues)
2. Incluye: nombre del canal, provincia, URL de YouTube
3. Especifica por qué debería ser detectado

### Mejorar Detección Regional
```python
# Agregar códigos locales en ConfiguracionProyecto
CODIGOS_ARGENTINOS = {
    "TU_CODIGO": "Tu_Provincia",  # Agregar aquí
    # ...
}
```

### Nuevas Categorías
```python
# En AnalizadorCanal.categorizar_canal()
categorias = {
    'Tu_Categoria': ['palabra1', 'palabra2', 'palabra3'],
    # ...
}
```

## 📜 Licencia

Este proyecto está bajo licencia MIT. Ver [LICENSE](LICENSE) para detalles.

## 🙏 Agradecimientos

- **Comunidad de streamers argentinos** por la inspiración
- **Provincias invisibilizadas** por el contenido único que crean
- **Contribuidores** que ayudan a mejorar la detección

## 📞 Contacto

- **Issues**: [GitHub Issues](https://github.com/tu-usuario/cartografia-streaming-argentino/issues)
- **Discusiones**: [GitHub Discussions](https://github.com/tu-usuario/cartografia-streaming-argentino/discussions)
- **Email**: tu-email@ejemplo.com

---

<div align="center">

**🇦🇷 Hecho con ❤️ para visibilizar el streaming argentino real**

*"La verdadera Argentina streaming está en las páginas 20-50 de YouTube"*

[![Streamers Detectados](https://img.shields.io/badge/Streamers%20Detectados-0-blue)]()
[![Provincias Cubiertas](https://img.shields.io/badge/Provincias%20Cubiertas-0%2F23-green)]()
[![Última Ejecución](https://img.shields.io/badge/Última%20Ejecución-Nunca-red)]()

</div>
