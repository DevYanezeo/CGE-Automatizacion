import time
from pathlib import Path
from bbdd_cmg import busca_barra_cmg, busca_cliente_bdd

def buscar_barra(
    entrada_ruta, 
    entrada_fecha_inicio, 
    entrada_fecha_fin, 
    entrada_barra, 
    lista_resultados, 
    escribir_mensaje
):
    """
    Método para buscar una barra específica en la base de datos.
    """
    try:
        escribir_mensaje("Buscando Barra...")

        ruta = Path(entrada_ruta)
        fecha_inicio = entrada_fecha_inicio
        fecha_fin = entrada_fecha_fin
        barra = entrada_barra

        start_time = time.time()

        escribir_mensaje("Ejecutando consulta...")
        results = busca_barra_cmg(folder=ruta, date_i=fecha_inicio, date_f=fecha_fin, barras=barra)

        end_time = time.time()
        tiempo_consulta = end_time - start_time

        escribir_mensaje(f"Tiempo de consulta: {tiempo_consulta:.4f} segundos")
        lista_resultados.clear()
        if isinstance(results, str):
            results = [results]
        lista_resultados.addItems(results)

        escribir_mensaje("Búsqueda completada.")

    except Exception as e:
        escribir_mensaje(f"Error: {e}")

def buscar_cliente(
    entrada_ruta, 
    entrada_fecha_inicio, 
    entrada_fecha_fin, 
    entrada_cliente, 
    lista_resultados, 
    escribir_mensaje, 
    contador_clientes_label
):

    try:
        escribir_mensaje("Buscando Cliente...")

        ruta = Path(entrada_ruta)
        fecha_inicio = entrada_fecha_inicio
        fecha_fin = entrada_fecha_fin
        cliente = entrada_cliente

        start_time = time.time()
        results = busca_cliente_bdd(folder=ruta, date_i=fecha_inicio, date_f=fecha_fin, cliente=cliente)
        end_time = time.time()
        tiempo_consulta = end_time - start_time

        escribir_mensaje(f"Tiempo de consulta: {tiempo_consulta:.4f} segundos")

        # Mostrar resultados en la lista
        lista_resultados.clear()
        num_clientes = len(results)  # Contar los clientes encontrados
        
        for result in results:
            cliente = result["Cliente"]
            barra = result["Barra"]
            lista_resultados.addItem(f"{cliente} (Barra: {barra})")

        # Actualizar el contador de clientes encontrados
        contador_clientes_label.setText(f"Clientes Encontrados: {num_clientes}")

        escribir_mensaje("Búsqueda de Cliente completada.")

        # Llamar a la función de agregar botones solo si no han sido agregados
        
    except KeyError as e:
        escribir_mensaje(f"Error: {e}. Verifica el esquema de los datos.")
    except Exception as e:
        escribir_mensaje(f"Error: {e}")
