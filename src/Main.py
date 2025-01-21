# ------- Bloque de Importaciones ------- #
import pandas as pd
import xlwings as xw
import numpy as np
import time
from pathlib import Path

from bbdd_cmg import get_cmg_barra, busca_barra_cmg
from bbdd_cmg import get_ivt_cliente, busca_cliente_bdd

# ------- Funciones de Validación ------- #

# ------- 2) Método validar_celda_texto() ------- #
"""
Descripción: Valida que un valor ingresado en una celda sea texto (string).

Parametros:
    - valor: Valor ingresado en la celda.
    - nombre_campo (str): Nombre del campo que se está validando.

Return:
    - Ninguno: Si el valor es válido.
    - Error: Si es invalido
"""
def validar_celda_texto(valor, nombre_campo): 

    if not isinstance(valor, str):
        raise TypeError(f"El valor ingresado en {nombre_campo} debe ser texto (string), pero se recibió: {type(valor).__name__}")


# ------- Método inicio() ------- #
"""
Descripción: Inicializa las variables de configuración de acuerdo a los datos obtenidos
del archivo .csv y determina qué acción tomar según la opción proporcionada de búsqueda (cmg, cliente o data).

Configura las celdas del archivo a utilizar y limpia los datos previos según la operación seleccionada.

Parámetros:
    - opcion (str): El tipo de operación que se va a realizar que puede tomar los valores:
        'cmg': Operación de búsqueda de la barra CMG.
        'cliente': Operación para búsqueda de clientes.
        'data': Operación para obtener datos relacionados al consumo eléctrico.

Return: 
    - data (dict): Diccionario con las configuraciones necesarias para realizar operaciones.
      Contiene las rutas de archivo y las celdas del Excel donde el usuario ingresa los datos de filtro para buscar en el CSV.
"""
def inicio(opcion):
    wb = xw.Book.caller()
    hoja = wb.sheets['BBDD']

    # Obtiene el directorio donde se encuentra el archivo .py (script Python)
    script_dir = Path(__file__).parent  # Obtiene la ruta del directorio del archivo .py

    # Se definen las celdas a utilizar
    celdas = {
        'path': 'C2',          # Celda donde se mostrará la ruta
        'f_ini': 'C3',
        'f_fin': 'D3',
        'barra': 'C4',
        'cliente': 'C5',
        'mensajes': 'L2',
        'c_barras': 'B11',
        'c_clientes': 'E11',
        'msg_cmg': 'L3',
        'msgs2': 'L4',
        'msg_cli': 'L5',
    }

    # Definición de variables globales
    BBDD = script_dir / Path(hoja[celdas['path']].value)  # Usar la ruta del directorio del script .py
    fecha_ini = hoja[celdas['f_ini']].value
    fecha_fin = hoja[celdas['f_fin']].value
    barra = hoja[celdas['barra']].value
    cliente = hoja[celdas['cliente']].value

    # Mostrar la ruta del directorio del archivo .py en la celda C2
    hoja[celdas['path']].value = str(script_dir)

    # Validación de valores de texto en las celdas de barra y cliente
    try:
        validar_celda_texto(barra, "Barra (C4)")
        validar_celda_texto(cliente, "Cliente (C5)")
    except TypeError as e:
        hoja[celdas['mensajes']].value = str(e)
        raise

    # Diccionario con los datos que se utilizarán en otras partes del código
    data = {
        'destino': BBDD,
        'hoja': hoja,
        'fecha_ini': fecha_ini,
        'fecha_fin': fecha_fin,
        'c_barra': celdas['barra'],
        'barra': barra,
        'cliente': cliente,
        'mensajes': celdas['mensajes'],
        'c_barras': celdas['c_barras'],
        'c_clientes': celdas['c_clientes'],
        'msgs2': celdas['msgs2'],
        'msg_cmg': celdas['msg_cmg'],
        'msg_cli': celdas['msg_cli'],
    }

    # Limpiar celdas
    lfin = hoja[celdas['mensajes']].end('down').address
    bfin = hoja[celdas['c_barras']].end('down').address
    cfin = hoja[celdas['c_clientes']].end('down').address
    if opcion == 'cmg':
        hoja.range(f'{celdas['mensajes']}:{lfin}').clear_contents()
        hoja.range(f'{celdas['c_barras']}:{bfin}').clear_contents()
    elif opcion == 'cliente':
        hoja.range(f'{celdas['mensajes']}:{lfin}').clear_contents()
        hoja.range(f'{celdas['c_clientes']}:{cfin}').clear_contents()
    elif opcion == 'data':
        hoja.range(f'{celdas['c_barras']}:{bfin}').clear_contents()
    
    return data

# ------- Método busca_barra() ------- #
"""
Descripción: Busca información sobre las barras CMG en la base de datos utilizando la función busca_barra_cmg y 
coloca los resultados en una celda específica de la hoja de Excel.

Parámetros: Ninguno. Utiliza la configuración obtenida a través del método inicio().

Return: Ninguno. Los resultados se muestran directamente en el archivo Excel.
"""
def busca_barra():
    info = inicio('cmg')
    sh = info['hoja']

    sh[info['mensajes']].value = 'Iniciando Búsqueda de barras...'
    start_time = time.time()  # Iniciar medición
    results = busca_barra_cmg(
        folder= info['destino'],
        barra= info['barra'],
        date_i= info['fecha_ini'],
        date_f= info['fecha_fin']
    )
    end_time = time.time()  # Finalizar medición
    tiempoConsulta = end_time - start_time

    # Actualizar el mensaje con el tiempo de consulta
    sh[info['mensajes']].value = f'Iniciando Búsqueda de Barras... Tiempo de consulta: {tiempoConsulta:.4f} segundos'
    
    # Colocar los resultados en la celda de la hoja de Excel
    results_lines = results.split('\n')  # Dividir por líneas
    sh[info['c_barras']].options(expand='table', index=False, transpose=True).value = results_lines


# ------- Método busca_cliente() ------- #
"""
Descripción: Busca los datos de clientes en la base de datos utilizando la función busca_cliente_bdd y 
coloca los resultados en una celda específica de la hoja de Excel.

Parámetros: Ninguno. Utiliza la configuración obtenida a través del método inicio().

Return: Ninguno. Los resultados se colocan en una celda específica en el archivo Excel.
"""
def busca_cliente():
    info = inicio('cliente')
    sh = info['hoja']
    ruta = Path.cwd()

    sh[info['mensajes']].value = 'Iniciando Búsqueda de Clientes...'
    start_time = time.time()  # Iniciar medición
    results  = busca_cliente_bdd(folder=info['destino'], cliente=info['cliente'],
                       date_i=info['fecha_ini'], date_f=info['fecha_fin'])
    end_time = time.time()  # Finalizar medición
    tiempoConsulta = end_time - start_time
    sh[info['mensajes']].value = f'Iniciando Búsqueda de Clientes... Tiempo de consulta: {tiempoConsulta:.4f} segundos'
    results_lines = results.split('\n')  # Dividir por líneas
    sh[info['c_clientes']].options(expand='table', index=False, transpose=True).value = results_lines
    

    
from pathlib import Path
import time

# ------- Método get_cmg() ------- #
def get_cmg(destino, barra, fecha_ini, fecha_fin, tipo_barra):
    """
    Obtiene los datos de CMg y guarda el resultado en un archivo específico según el tipo de barra.
    
    Parámetros:
    - destino: Carpeta de destino para guardar los archivos.
    - barra: Identificador de la barra.
    - fecha_ini: Fecha de inicio del rango.
    - fecha_fin: Fecha de fin del rango.
    - tipo_barra: Tipo de barra ('retiro' o 'inyeccion').

    Retorna:
    - df: DataFrame con los datos obtenidos.
    """
    print(f"Iniciando proceso de obtener CMg para barra {barra} ({tipo_barra})...")
    start_time = time.time()
    
    # Llamada a función para obtener datos (simulación de get_cmg_barra)
    df = get_cmg_barra(
        folder=destino,
        barra=barra,
        date_i=fecha_ini,
        date_f=fecha_fin
    )
    
    # Definir el nombre del archivo según el tipo de barra
    if tipo_barra.lower() == "retiro":
        filename = 'CMg_Ret.parquet'
    elif tipo_barra.lower() == "inyeccion":
        filename = 'CMg_Iny.parquet'
    else:
        raise ValueError("El tipo de barra debe ser 'retiro' o 'inyeccion'.")
    
    parquet_path = Path(destino) / filename
    df.write_parquet(parquet_path)
    
    elapsed_time = time.time() - start_time
    print(f"Proceso de obtener CMg completado en {elapsed_time:.2f} segundos. Archivo guardado en {parquet_path}.")
    return df


# ------- Método get_cliente() ------- #
def get_cliente(destino, cliente, barraCliente, fecha_ini, fecha_fin):
    print("Iniciando proceso de obtener Clientes...")
    start_time = time.time()
    
    # Llamada a función para obtener datos (simulación de get_ivt_cliente)
    df = get_ivt_cliente(
        folder=destino,
        cliente=cliente,
        barra=barraCliente,
        date_i=fecha_ini,
        date_f=fecha_fin
    )
    parquet_path = Path(destino) / 'Consumo.parquet'
    fecha_path = Path(destino) / 'Fecha.parquet'
    
    df.write_parquet(parquet_path)
    df[['Fecha']].write_parquet(fecha_path)
    
    elapsed_time = time.time() - start_time
    print(f"Proceso de obtener Clientes completado en {elapsed_time:.2f} segundos.")
    return df

# ------- Método get_data ------- #
"""
Descripción: Recoge los datos de CMG y los de consumo de los clientes, los combina, 
y genera un archivo .parquet con los datos consolidados. Los resultados son exportados a Excel.

Parámetros: Ninguno. Obtiene los datos necesarios directamente desde los métodos anteriores.

Return: Ninguno. Los datos consolidados se guardan en un archivo .parquet y se exportan a Excel.
"""
# ------- Método get_data ------- #
def get_data(destino, barraCliente, barraIny, clientes, fecha_ini, fecha_fin):
    print("Iniciando el proceso get_data...")
    start_time = time.time()

    # Obtener datos de CMG para barra de inyección
    try:
        cmg_start = time.time()
        print("Obteniendo datos de CMg para barra de inyección...")
        cmg_iny = get_cmg(destino, barraIny, fecha_ini, fecha_fin, tipo_barra="inyeccion").to_pandas()
        # Verificar los nombres originales de las columnas
        print("Nombres originales de las columnas:", cmg_iny.columns.tolist())
        # Eliminar corchetes y limpiar espacios
        cmg_iny.columns = cmg_iny.columns.str.replace(r"\[|\]", "", regex=True).str.strip()
        # Renombrar la columna específica
        cmg_iny.rename(columns={'CMg USD/MWh': 'CMg Iny USD/MWh'}, inplace=True)
        # Configurar índices
        cmg_iny.set_index(['Fecha', 'Barra'], inplace=True)
        print(f"Extracción de CMg para barra de inyección completada en {time.time() - cmg_start:.2f} segundos.")
    except Exception as e:
        print("Error al extraer datos de CMg para barra de inyección.")
        print(str(e))
        return

    # Obtener datos de Clientes
    try:
        cli_start = time.time()
        print("Obteniendo datos de Clientes...")

        cli = get_cliente(destino, clientes, barraCliente, fecha_ini, fecha_fin).to_pandas()
        cli.set_index(['Fecha', 'Barra'], inplace=True)

        print(f"Extracción de Clientes completada en {time.time() - cli_start:.2f} segundos.")
    except Exception as e:
        print("Error al extraer datos de Clientes.")
        print(str(e))
        return

    # Obtener datos de CMG para barra de retiro del cliente
    try:
        cmg_start = time.time()
        print("Obteniendo datos de CMg para barra de retiro...")

        cmg_ret = get_cmg(destino, barraCliente, fecha_ini, fecha_fin, tipo_barra="retiro").to_pandas()
        # Renombrar columnas correctamente y eliminar corchetes
        cmg_ret.columns = cmg_ret.columns.str.replace(r"\[|\]", "", regex=True)
        cmg_ret.rename(columns={'CMg USD/MWh': 'CMg Retiro USD/MWh'}, inplace=True)
        cmg_ret.set_index(['Fecha', 'Barra'], inplace=True)

        print(f"Extracción de CMg para barra de retiro completada en {time.time() - cmg_start:.2f} segundos.")
    except Exception as e:
        print("Error al extraer datos de CMg para barra de retiro.")
        print(str(e))
        return

    # Tiempo total
    print(f"Proceso get_data completado en {time.time() - start_time:.2f} segundos.")
