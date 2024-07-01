# FM-Movie

Repositorio destinado al desarrollo del trabajo práctico de la materia Teoría de Lenguaje (75.31), 

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

** Para la ejecución de un archivo que hace uso de threads, debe ejecutarse el archivo con un argumento especial. Por ejemplo, si quiere ejecutarse el archivo en donde se obtienen datos de letterbox usando threads, debe correrse por terminal `julia letterbox.jl --threads n`, siendo n el número de threads que quieren utilizarse.

*** Para la ejecución de un notebook, el usuario debe asegurarse que se está usando un kernel de Julia y no uno de python u otros.

#### Objetivo y orden del trabajo práctico:

El objetivo de este trabajo es demostrar lo que aprendimos sobre Julia a lo largo de estos meses, obteniendo de la web data sobre películas, manipulándola y analizándola para finalmente poder realizar una predicción sobre el rating de la película en el sitio especializado imdb. Así, el trabajo puede dividirse en tres etapas principales: 

- Extracción de los datos, desarrollado en la carpeta `extract_data`.

- Preparación de los datos para el análisis, desarrollado en la carpeta `prepare_data`.

- Análisis de los datos y predicción, desarrollado en la carpeta `analyze_data`.

#### Video presentación:

Puede encontrarse el video presentación del trabajo práctico en...

#### Colab - visus: https://colab.research.google.com/drive/1pS4eYxQP-qluCUBjyM1cTXtz2T6uUVoJ?usp=drive_link y https://colab.research.google.com/drive/1M82lhFwg3xL7IXVU4h-kgF9TV5UIVw-h?usp=drive_link
#### Colab ml: https://colab.research.google.com/drive/1H19kVU2oGjh3IQyPHD0BW7s4H0uaTjkC?usp=drive_link