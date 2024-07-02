# FM-Movie

Repositorio destinado al desarrollo del trabajo práctico de la materia Teoría de Lenguaje (75.31).


### Integrantes

- Dolores Pavón - 108221
- Facundo Fernández - 109097
- Miguel Metz - 106148


#### Para poder ejecutar los archivos presentes en este trabajo, es necesario tener instalados los siguientes paquetes:

- DataFrames
- CSV
- Dates
- Statistics
- Plots
- MLJ
- Random
- ScikitLearn
- GLM
- HTTP
- CodecZlib
- Tables
- Mongoc
- VaderSentiment
- PyCall

Para la instalación de dichos paquetes, puede ejecutarse el archivo `requirements.jl` presente en este repositorio (mediante el comando `julia requirements.jl`), o pueden instalarse manualmente por terminal, siguiendo la guía en este link: https://www.educative.io/answers/how-to-install-julia-packages-via-command-line

** Para la ejecución de un archivo que hace uso de threads, debe ejecutarse el archivo con un argumento especial. Por ejemplo, si quiere ejecutarse el archivo en donde se obtienen datos de letterbox usando threads, debe correrse por terminal `julia -t {n} extract_data/imdb_data.jl`, siendo n el número de threads que quieren utilizarse. Esta ejecución sólo tendrá sentido si se utiliza la conexión a MongoDB

*** Para la ejecución de un notebook, el usuario debe asegurarse que se está usando un kernel de Julia y no uno de python u otros.


#### Objetivo y orden del trabajo práctico:

El objetivo de este trabajo es demostrar lo que aprendimos sobre Julia a lo largo de estos meses, obteniendo de la web data sobre películas, manipulándola y analizándola para finalmente poder realizar una predicción sobre el rating de la película en el sitio especializado imdb. Así, el trabajo puede dividirse en tres etapas principales: 

- Extracción de los datos, desarrollado en la carpeta `extract_data`.

- Preparación de los datos para el análisis, desarrollado en la carpeta `prepare_data`.

- Análisis de los datos y predicción, desarrollado en la carpeta `analyze_data`.


#### Video presentación:

- Puede encontrarse el video presentación del trabajo práctico en: https://drive.google.com/file/d/19gwR9E29dRvEpUX_eYdllaq1QsOLBK-j/view?usp=drive_link


#### Recursos necesarios:

- Drive con recursos necesarios a descargar: https://drive.google.com/drive/folders/1OVPiWARIcQzV6Ls7PwysuIIc9KuOHG_i?usp=sharing
- Se necesita descargar los archivos datos/data_to_analyze.csv y datos/imdb_complete.csv, los cuales deben ser alojados en el directorio 'data_output' para su posterior uso.
- El archivo datos/imdb_complete.csv se utiliza para el proceso prepare_data\unify_data.jl, y no tiene una una descarga automática ya que lo descargamos a mano de internet.
- El archivo datos/data_to_analyze.csv es un set de datos completo que se utiliza para el analisis en los notebooks. Al estar limitado el proceso prepare_data/unify_data.jl por el tiempo, se disponibiliza este archivo para poder hacer un analisis completo. 


#### Ejecuciones de los procesos:
- Se asume que el usuario tiene previamente instalado python, julia, y los packages incicados en los requirements. Además debe haber descargado 
- El trabajo consiste en 2 archivos julia de extracción de datos dentro la carpeta 'extract_data', 2 archivos julia de procesamiento dentro de la carpeta 'prepare_data' y 3 notebooks de analysis y gráficos.
- El archivo index.jl ejecuta los 4 procesos de etract_data y prepare_data. Estos procesos generan csvs con datos que se guardarán en el diectorio 'data_output' y desde ahí serán utilizados.
- Los notebooks serán ejecutados por el usuario, y tendrán como entrada de datos el archivo data_output/data_to_analyze.csv que debe ser descargado.


#### Tiempos y limitaciones:
- Por la extensa cantidad de datos utilizados, los tiempos de ejecución de los procesos completos son, por ej:
    - extract_data/imdb_data.jl : 3hs
    - prepare_data/unify_data.jl : 4hs
- Por lo tanto se ha decidido:
    - limitar la cantidad de datos extraídos con extract_data/imdb_data.jl a 100 mil contenidos
    - limitar la cantidad de páginas de Letterbox a scrapear de https://letterboxd.com/films a 7
    - limitar la cantidad de datos en el archivo de Drive "imdb_complete" de 10 millones de datos a 80 mil
    - limitar los datos a analizar en los notebooks a 20 mil.


#### Uso de CSVs y/o MongoDB
- Se ha desarrollado la integración de MongoDB para escritura, modificación y lectura de datos. Para la comodidad del usuario, se reemplazó esta integración por uso de CSVs.
- Los usos de MongoDB son % funcionales y quedaron en el código comentados.


<br>

---
---

<br>

#### Colabs usados para la práctica, incluyendo la integración de Julia.
#### Colab - visus: https://colab.research.google.com/drive/1pS4eYxQP-qluCUBjyM1cTXtz2T6uUVoJ?usp=drive_link y https://colab.research.google.com/drive/1M82lhFwg3xL7IXVU4h-kgF9TV5UIVw-h?usp=drive_link
#### Colab ml: https://colab.research.google.com/drive/1H19kVU2oGjh3IQyPHD0BW7s4H0uaTjkC?usp=drive_link

