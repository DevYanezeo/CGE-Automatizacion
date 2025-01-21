from pathlib import Path
import polars as pl
import xlwings as xw    
import duckdb
import time

def get_cmg_barra(folder: Path, barra: str, date_i: str, date_f: str):
    """
    Extrae los datos de CMg para una barra específica en un rango de fechas.

    Args:
        folder (Path): Ruta a la carpeta base de datos.
        barra (str): Nombre de la barra a filtrar.
        date_i (str): Fecha de inicio en formato 'AAAA-MM'.
        date_f (str): Fecha de fin en formato 'AAAA-MM'.

    Returns:
    """
    def recursive_load(rango_fechas, data):
        try:
            fecha = next(rango_fechas)
        except StopIteration:
            return data
        
        
        year, month = fecha.split('-')
        df_path = fder / f"CMg_{year[-2:]}_{month}_def.parquet"
        archivo = f"CMg_{year[-2:]}_{month}_def.parquet"
        print(f'Obteniendo datos de {archivo}')

        if not df_path.exists():
            print(f"Archivo no encontrado: {df_path}")
            return recursive_load(rango_fechas, data)

        # Carga el archivo y filtra por barra
        df = pl.read_parquet(df_path)
        barras = '|'.join([x.strip().upper() for x in barra.split(',')])
        df = df.filter(pl.col('Barra').str.to_uppercase().str.contains(barras))

        return recursive_load(rango_fechas, data.vstack(df))

    # Directorio base
    fder = folder.parent / 'All_Data'

    # Inicializar rango de fechas
    year_i, month_i = date_i.split('-')
    year_f, month_f = date_f.split('-')
    rango_fechas = crea_rango(year_i, month_i, year_f, month_f)

    # Cargar datos
    start_time = time.time()
    data = recursive_load(rango_fechas, pl.DataFrame())
    elapsed_time = time.time() - start_time

    print(f"Extracción de CMg para la barra '{barra}' completada en {elapsed_time:.2f} segundos.")
    return data




# ------- get_ivt_cliente ------- #
def get_ivt_cliente(folder: Path, cliente: str, barra: str, date_i: str, date_f: str):
    def recursive_load(rango_fechas, data):
        try:
            fecha = next(rango_fechas)
        except StopIteration:
            return data

        # Imprimir el mensaje de depuración con la fecha que se está procesando
        year, month = fecha.split('-')
        
        # Solo leer el archivo si está dentro del rango de fechas especificado
        if year > date_f.split('-')[0] or (year == date_f.split('-')[0] and int(month) > int(date_f.split('-')[1])):
            return data
        
        fder = folder.parent / 'All_Data'
        archivo = f"IVT_{year[-2:]}_{month}.parquet"
        print(f'Obteniendo datos de {archivo}')
        
        try:
            df = pl.read_parquet(fder / archivo)
        except Exception as e:
            print(f"Error al leer el archivo {archivo}: {e}")
            return data

        # Filtrar los datos según el cliente y la barra
        cliente_regex = '|'.join([x.strip().upper() for x in cliente.split(',')])
        barra_regex = barra.upper()  # Buscar la barra específica
        df = df.filter(pl.col('Cliente').str.to_uppercase().str.contains(cliente_regex) & 
                       pl.col('nombre_barra').str.to_uppercase().str.contains(barra_regex))

        return recursive_load(rango_fechas, data.vstack(df))

    start_time = time.time()
    
    # Dividir las fechas de inicio y fin
    year_i, month_i = date_i.split('-')
    year_f, month_f = date_f.split('-')

    # Crear el rango de fechas
    rango_fechas = crea_rango(year_i, month_i, year_f, month_f)
    
    # Llamar a la función recursiva para obtener los datos
    data = recursive_load(rango_fechas, pl.DataFrame())
    
    # Renombrar la columna 'nombre_barra' a 'Barra'
    data = data.rename({'nombre_barra': 'Barra'})
    
    elapsed_time = time.time() - start_time
    print(f'Extracción de IVT cliente completada en {elapsed_time:.2f} segundos.')
    
    return data

# Busca clientes y barras específicos asociados en los datos de CMg para una fecha específica.
#
# :param sh: Objeto de hoja de cálculo para mostrar mensajes de progreso.
# :param folder: Carpeta base donde se encuentran los archivos de datos.
# :param barra: Barras específicas para buscar (separadas por comas).
# :param date_i: Fecha inicial 
# :param date_f: Fecha final en formato "YYYY-MM".
# :return: Lista ordenada de barras encontradas.
def busca_cliente_bdd(folder: Path, cliente: str, date_i: str, date_f: str):
    year_i, month_i = date_i.split('-')
    year_f, month_f = date_f.split('-')
    
    # Ruta del archivo parquet
    fder = folder.parent / 'All_Data'
    df = pl.read_parquet(fder / f"IVT_{year_f[-2:]}_{month_f}.parquet")  # Consolidado de los .parquet de las fechas de entrada

    cliente = '|'.join([x.strip().upper() for x in cliente.split(',')])  # Adaptar la entrada
    con = duckdb.connect()  # Conectar base de datos duckdb
    con.register("clientes", df)  # Registrar df en la base de datos en una tabla "clientes"
    
    # Consulta SQL
    query = f"SELECT DISTINCT Cliente, nombre_barra FROM clientes WHERE Cliente LIKE '%{cliente}%'"
    results = con.execute(query).df()
    
    # Validación de columnas
    if "Cliente" not in results.columns or "nombre_barra" not in results.columns:
        raise ValueError("La consulta no devolvió las columnas esperadas: 'Cliente' y 'nombre_barra'")
    
    # Rellenar valores faltantes
    results.fillna({"Cliente": "Desconocido", "nombre_barra": "Sin Barra"}, inplace=True)
    
    # Crear lista de resultados
    results_list = []
    for _, row in results.iterrows():
        cliente = row.get("Cliente", "Desconocido")
        barra = row.get("nombre_barra", "Sin Barra")
        results_list.append({"Cliente": cliente, "Barra": barra})
    
    return results_list

# Genera un rango de fechas en formato "YYYY-MM".
#
# :param agno_i: Año inicial como cadena.
# :param mes_i: Mes inicial como cadena.
# :param agno_f: Año final como cadena.
# :param mes_f: Mes final como cadena.
# :yield: Fechas en formato "YYYY-MM" desde la inicial hasta la final.
def crea_rango(agno_i, mes_i, agno_f, mes_f):
    a_aux = int(agno_i)
    mes_aux = int(mes_i)
    while (a_aux < int(agno_f)) or (a_aux == int(agno_f) and mes_aux <= int(mes_f)):
        yield f'{a_aux}-{str(mes_aux).zfill(2)}'
        if mes_aux == 12:
            a_aux += 1
            mes_aux = 1
        else:
            mes_aux += 1


def busca_barra_cmg(folder: Path, barras: str, date_i: str, date_f: str):
    """
    Busca múltiples barras en un archivo Parquet y devuelve una lista de coincidencias.

    Args:
        folder (Path): Ruta base que contiene los datos.
        barras (str): Barras separadas por comas (e.g., "ARICA, POLPAICO, ANTOFA").
        date_i (str): Fecha inicial en formato 'YYYY-MM'.
        date_f (str): Fecha final en formato 'YYYY-MM'.

    Returns:
        list: Lista de barras encontradas.
    """
    year_f, month_f = date_f.split('-')
    fder = folder.parent / 'All_Data'
    # Cargar el archivo Parquet
    df = pl.read_parquet(fder / f"CMg_{year_f[-2:]}_{month_f}_def.parquet")

    # Preparar las barras para la búsqueda
    barras = [barra.strip().upper() for barra in barras.split(',')]

    # Crear una conexión con DuckDB
    con = duckdb.connect()
    con.register("barras", df)

    # Buscar cada barra y acumular los resultados
    all_results = set()
    for barra in barras:
        query = f"SELECT DISTINCT Barra FROM barras WHERE Barra LIKE '%{barra}%'"
        results = con.execute(query).df()
        all_results.update(results["Barra"].tolist())

    return list(all_results)

def crea_rango(agno_i, mes_i, agno_f, mes_f):
    """
    Genera un rango de fechas en formato 'YYYY-MM' desde la fecha de inicio hasta la fecha de fin.

    Args:
        agno_i (str): Año de inicio.
        mes_i (str): Mes de inicio.
        agno_f (str): Año de fin.
        mes_f (str): Mes de fin.

    Yields:
        str: Fechas en formato 'YYYY-MM' dentro del rango.
    """
    a_aux = int(agno_i)
    mes_aux = int(mes_i)
    while (a_aux < int(agno_f)) or (a_aux == int(agno_f) and mes_aux <= int(mes_f)):
        yield f'{a_aux}-{str(mes_aux).zfill(2)}'
        if mes_aux == 12:
            a_aux += 1
            mes_aux = 1
        else:
            mes_aux += 1

def buscarClientesPorBarra(folder: Path, barra_seleccionada: str, date_i: str, date_f: str):
    """
    Busca los clientes asociados a una barra específica en los archivos Parquet dentro de un rango de fechas,
    y devuelve los clientes únicos encontrados en todos esos archivos.

    Args:
        folder (Path): Ruta base que contiene los datos.
        barra_seleccionada (str): Nombre de la barra seleccionada.
        date_i (str): Fecha inicial en formato 'YYYY-MM'.
        date_f (str): Fecha final en formato 'YYYY-MM'.

    Returns:
        list: Lista de clientes únicos asociados a la barra seleccionada.
    """
    try:
        # Extraer año y mes de las fechas de inicio y fin
        year_i, month_i = date_i.split('-')
        year_f, month_f = date_f.split('-')

        # Lista para almacenar los clientes únicos encontrados
        clientes_unicos = set()

        # Iterar sobre el rango de fechas
        for fecha in crea_rango(year_i, month_i, year_f, month_f):
            # Construir la ruta del archivo Parquet para cada fecha (formato IVT_20_01.parquet)
            fder = folder.parent / 'All_Data'
            archivo_parquet = fder / f"IVT_{fecha.split('-')[0][2:]}_{fecha.split('-')[1]}.parquet"  # Formato: IVT_20_01.parquet
            print(f"Buscando en archivo: {archivo_parquet}")

            # Intentar leer el archivo Parquet
            try:
                df = pl.read_parquet(archivo_parquet)
                print(f"Archivo {archivo_parquet} cargado correctamente.")

                # Normalizar y filtrar por la barra seleccionada
                barra_seleccionada_normalizada = barra_seleccionada.strip().upper()
                df = df.with_columns(
                    df["nombre_barra"].str.strip_chars().str.to_uppercase().alias("nombre_barra_normalizado")
                )

                # Filtrar las filas que coincidan con la barra seleccionada
                df_filtrado = df.filter(df["nombre_barra_normalizado"] == barra_seleccionada_normalizada)

                # Mostrar la cantidad de filas encontradas para esa barra
                print(f"Cantidad de filas filtradas para la barra '{barra_seleccionada_normalizada}' en el archivo {archivo_parquet}: {len(df_filtrado)}")

                # Si se encontraron filas, mostrar algunos detalles
                if len(df_filtrado) > 0:
                    print(f"Ejemplo de filas filtradas en el archivo {archivo_parquet}:")
                    print(df_filtrado.head(5))  # Mostrar un ejemplo de las filas filtradas
                    # Extraer los clientes únicos de la columna 'Cliente'
                    clientes_unicos.update(df_filtrado["Cliente"].unique().to_list())
                else:
                    print(f"No se encontraron coincidencias para la barra '{barra_seleccionada_normalizada}' en el archivo {archivo_parquet}")

            except Exception as e:
                print(f"Error al procesar el archivo {archivo_parquet}: {e}")

        # Convertir el conjunto de clientes únicos en una lista
        clientes_unicos = list(clientes_unicos)

        
        # Validar resultados
        if not clientes_unicos:
            print("No se encontraron clientes para la barra seleccionada en el rango de fechas.")
            return ["No se encontraron clientes para la barra seleccionada."]

        return clientes_unicos

    except Exception as e:
        print(f"Error al buscar clientes: {str(e)}")
        return [f"Error al buscar clientes: {str(e)}"]
