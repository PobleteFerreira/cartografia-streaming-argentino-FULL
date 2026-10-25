# data/

`streamers_argentinos.csv` es el listado de canales potenciales que fue
generando el script (1.005 canales al 25/2/2026, fecha citada en el
artículo). Cada corrida de `main.py` agrega los canales nuevos al final.

Algunas filas vienen de la versión anterior del script, que guardaba menos
columnas: en esas filas las columnas nuevas quedan vacías. Las filas con
`metodo_deteccion = manual` se cargaron a mano desde una lista propia.

`ejemplo_streamers.csv` muestra el formato completo con canales **inventados**.

| Columna | Qué contiene |
|---|---|
| canal_id | ID del canal (UC...) |
| nombre_canal | Título del canal |
| categoria | Gaming, Música, Charlas/Podcast, Noticias/Radio, etc. |
| provincia / ciudad | Ubicación detectada; "Argentina" si no se pudo precisar |
| suscriptores | 0 si el canal oculta el número |
| certeza | 0-100, certeza de que el canal es argentino |
| metodo_deteccion | explicit_country, local_code, province_mention, voseo_patterns, cultural_analysis o manual (filas históricas) |
| indicadores_argentinidad | Señales que encontró el detector |
| ultima_actividad | Fecha del video más reciente |
| videos_analizados / videos_en_vivo | Subidas recientes revisadas y cuántas fueron transmisiones en vivo |
