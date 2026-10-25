# 🇦🇷 Cartografía del Streaming Argentino

Herramienta para encontrar canales argentinos de YouTube que hacen
**transmisiones en vivo**, con una estrategia de búsqueda pensada para
compensar el sesgo de visibilidad hacia los canales de CABA y Buenos Aires.

Es una propuesta metodológica: el código está abierto para que cualquiera lo
use con su propia API key. El listado generado hasta ahora está en
[`data/streamers_argentinos.csv`](data/streamers_argentinos.csv).

## Artículo

Este repositorio está citado en un artículo (consultado el 25/2/2026). La
versión del código de esa fecha está en la etiqueta
[`version-articulo-2026-02`](../../tree/version-articulo-2026-02). A esa
fecha, la ejecución del script había arrojado **1.005 canales potenciales**
de ser incluidos en futuros estudios; ese listado está en
[`data/streamers_argentinos.csv`](data/streamers_argentinos.csv) (y, tal como
estaba ese día, en la misma etiqueta).

La versión actual corrige problemas de aquella: la búsqueda profunda y las
cuatro fases ahora se ejecutan como estaban descritas, la detección de
contenido en vivo usa los datos de la API en lugar de palabras clave y la
asignación de provincia es más precisa.

## El problema

Los resultados de búsqueda de YouTube priorizan canales grandes y, en la
práctica, porteños. Los canales provinciales pequeños aparecen muy abajo o
no aparecen. Una búsqueda genérica ("streaming argentina") devuelve siempre
los mismos canales; para llegar al resto hace falta buscar más profundo y con
términos locales.

## Estrategia: cuatro fases en un ciclo de 120 días

| Fase | Días | Qué busca | Ejemplos |
|---|---|---|---|
| 1 | 1-30 | Términos generales | "streaming argentina", "podcast argentina en vivo" |
| 2 | 31-60 | Provincias y ciudades; las remotas reciben más profundidad | "streaming Catamarca", "en vivo Ushuaia" |
| 3 | 61-90 | Códigos locales combinados con formatos | "charlas MZA", "gaming SLA", "bunker CBA" |
| 4 | 91-120 | Cultura regional, para micro-streamers | "chamamé en vivo", "folklore streaming", "truco en vivo" |

Cada corrida retoma donde quedó la anterior: guarda qué términos terminó y en
qué página iba. Si un día se agota la cuota, al siguiente sigue desde ahí.
Si los términos de la fase actual se terminan antes de tiempo, avanza con los
de las otras fases. Al día 121 empieza un ciclo nuevo; los canales ya
analizados no se vuelven a analizar.

**Límite real de profundidad:** la API devuelve 50 resultados por página y
deja de dar páginas siguientes alrededor de los 500 resultados por término.
Por eso la profundidad se gana sobre todo multiplicando términos específicos,
más que pidiendo más páginas de un término general.

## Cómo decide si un canal entra

Un canal se guarda si pasa los tres filtros:

1. **Suscriptores** ≥ `MIN_SUSCRIPTORES` (500 por defecto). Los canales que
   ocultan el número no se descartan por esto.
2. **Es argentino** (certeza ≥ `MIN_CERTEZA_ARGENTINA`, 70 por defecto):
   - País declarado en el canal = AR → certeza 100.
   - Si no: menciona Argentina o 🇦🇷, códigos locales en mayúsculas (MZA,
     CBA, SLA, NQN...), provincias y ciudades, voseo ("vos tenés",
     "suscribite", "seguinos"), modismos (pibe, laburo, bondi...) y el
     lenguaje de sus videos recientes.
   - Se excluye si dice "soy de / desde / vivo en" otro país, o si nombra
     dos o más términos propios de otro país (ciudades, gentilicios, jerga).
   - Los nombres ambiguos (Misiones, Corrientes, Salta, San Juan, Córdoba...)
     suman poco y sólo ubican al canal si hay otra señal argentina.
3. **Hace vivos**: al menos una de sus últimas 50 subidas fue una
   transmisión en vivo, según los datos de la propia API
   (`liveStreamingDetails`). Las palabras clave ("en vivo", Twitch, OBS,
   horarios) se guardan como indicadores, pero no alcanzan solas.

La provincia se asigna con esta prioridad: código local → provincia →
ciudad → provincia ambigua. Si no hay ninguna, queda "Argentina".

## Instalación

Requiere Python 3.9 o superior y una API key de YouTube Data API v3.

```bash
git clone https://github.com/PobleteFerreira/cartografia-streaming-argentino.git
cd cartografia-streaming-argentino
python -m venv venv
source venv/bin/activate        # En Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # y completá YOUTUBE_API_KEY
```

### Conseguir la API key

1. Entrá a [Google Cloud Console](https://console.cloud.google.com/) y creá un proyecto.
2. Habilitá **YouTube Data API v3**.
3. En *Credenciales*, creá una **clave de API**.
4. **Restringila**: en *Restricciones de API* elegí sólo YouTube Data API v3.
   Así, si la clave se filtra, no sirve para otra cosa.
5. Pegala en `.env`. Ese archivo está en `.gitignore`: nunca lo subas.

## Uso

```bash
python main.py                  # corrida del día (fase según el día del ciclo)
python main.py --fase 3         # forzar una fase
python main.py --max-terminos 5 # corrida corta
python main.py --simulacion     # probar todo el flujo sin API key
python main.py -v               # ver también los canales rechazados y por qué
```

Resultados: `data/streamers_argentinos.csv` (formato en
[data/README.md](data/README.md); hay un ejemplo con canales inventados en
`data/ejemplo_streamers.csv`).

### Cuota

YouTube da 10.000 unidades por día y por proyecto. Una página de búsqueda
cuesta 100; analizar un canal cuesta ~2 (datos del canal en lotes de 50, sus
últimas subidas y los detalles de esas subidas). Con una clave se recorren
unas 40-45 páginas de búsqueda por día. Con una segunda clave de **otro
proyecto** (`API_KEY_2`) se duplica. El script lleva la cuenta por clave,
rota automáticamente cuando una se agota y la cuota se reinicia a la
medianoche del Pacífico, como en YouTube.

### Corrida diaria automática

En tu computadora. No se usa GitHub Actions porque la cuota y las claves
quedan bajo tu control y no hace falta guardarlas como secretos del repo.

```bash
# Linux/Mac (crontab -e): todos los días a las 2:00
0 2 * * * cd /ruta/al/proyecto && venv/bin/python main.py >> logs/cron.log 2>&1
```

En Windows, con el Programador de tareas, ejecutando
`venv\Scripts\python.exe main.py` con la carpeta del proyecto como directorio
de inicio.

## Estructura

```
main.py              # búsqueda, detectores y estado
requirements.txt
.env.example         # plantilla de configuración (copiar a .env)
data/                # listado de canales y ejemplo de formato
cache/               # estado de la búsqueda y cuota (no se sube)
logs/                # registros de cada corrida (no se suben)
tests/               # tests en modo simulación
```

## Tests

```bash
python -m unittest discover -s tests -v
```

No usan la API ni gastan cuota.

## Ajustar la búsqueda

- Términos de cada fase: `SearchStrategies` en `main.py` (pares
  `(término, páginas)`).
- Códigos locales, provincias, ciudades y nombres ambiguos: `Config`.
- Modismos, voseo y exclusiones por país: `ArgentineDetector`.
- Categorías de contenido: `ContentCategorizer`.

Si conocés canales que deberían aparecer y no aparecen, abrí un issue con el
link al canal y los términos que usa en su descripción, para mejorar la
detección.
