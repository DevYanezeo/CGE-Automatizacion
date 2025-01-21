# --- Librerías estándar ---
import os
import time
from pathlib import Path
import subprocess

# --- Librerías de terceros ---
import pandas as pd
from openpyxl import Workbook
from PyQt5 import QtWidgets, QtGui, QtCore
from PyQt5.QtWidgets import (
    QApplication, QWidget, QLabel, QVBoxLayout, QPushButton, QLineEdit, 
    QTextEdit, QListWidget, QFormLayout, QGridLayout, QProgressBar, 
    QTabWidget, QMessageBox
)
from PyQt5.QtGui import QFont, QCursor
from PyQt5.QtCore import Qt, QThread, pyqtSignal

# --- Módulos propios ---
from bbdd_cmg import (
    busca_barra_cmg, busca_cliente_bdd, get_cmg_barra, 
    get_ivt_cliente, buscarClientesPorBarra
)
from Metodos_Busqueda import buscar_barra, buscar_cliente
from Metodos_Extraccion import obtener_data, extraer_cmg, mostrar_consumo


class PowerBIThread(QThread):
    mensaje = pyqtSignal(str)  # Señal para enviar mensajes a la interfaz

    def __init__(self, archivo_pbix, *args, **kwargs):
        super(PowerBIThread, self).__init__(*args, **kwargs)
        self.archivo_pbix = archivo_pbix
        self.ruta_power_bi = r"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe"  # Ruta fija o configurada en otro lugar

    def run(self):
        # Verificar si el archivo .pbix existe
        if not os.path.isfile(self.archivo_pbix):
            self.mensaje.emit(f"El archivo .pbix no existe: {self.archivo_pbix}")
            return

        if not os.path.isfile(self.ruta_power_bi):
            self.mensaje.emit(f"El ejecutable de Power BI Desktop no se encuentra en la ruta: {self.ruta_power_bi}")
            return

        try:
            self.mensaje.emit("Iniciando Power BI Desktop...")
            subprocess.Popen([self.ruta_power_bi, self.archivo_pbix], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.mensaje.emit("Archivo .pbix enviado correctamente a Power BI Desktop. Espere mientras se carga...")
        except Exception as e:
            self.mensaje.emit(f"Error al intentar abrir el archivo .pbix: {e}")


class WorkerThread(QtCore.QThread):
    resultados_signal = QtCore.pyqtSignal(str)  # Señal para enviar mensajes a la lista de resultados
    estado_signal = QtCore.pyqtSignal(str)     # Señal para actualizar el estado en la barra
    finalizado_signal = QtCore.pyqtSignal(list)  # Señal para indicar que el proceso ha terminado

    def run(self):
        """
        Código que se ejecutará en el hilo.
        """
        print("WorkerThread iniciado.")
        try:
            import DescargarBD  # Importa el módulo que contiene la función de descarga

            lista_resultados_global = []

            def escribir_mensaje(texto):
                lista_resultados_global.append(texto)
                self.resultados_signal.emit(texto)  # Enviar mensaje a la UI

            def actualizar_estado(texto):
                self.estado_signal.emit(texto)  # Enviar estado a la UI

            # Ejecutar la función de descarga
            DescargarBD.descargar_archivos(escribir_mensaje, lista_resultados_global, actualizar_estado)

            # Enviar resultados finales
            self.finalizado_signal.emit(lista_resultados_global)

        except Exception as e:
            print(f"Error en WorkerThread: {str(e)}")
            self.finalizado_signal.emit([f"Error: {str(e)}"])

        print("WorkerThread terminado.")

def crear_interfaz():
    # Obtener la ruta del directorio del script
    script_dir = Path(__file__).parent
    powerbi_dir = script_dir.parent / "PowerBi"
    powerExtraerData = powerbi_dir / "BBDD_CMg_Retiro_Inyeccion.pbix"
    powerConsumoCli = powerbi_dir / "BBDD_Cli_Consumo.pbix"

    # Crear la aplicación
    app = QtWidgets.QApplication([]) 
    ventana = QtWidgets.QWidget()
    ventana.setWindowTitle("Automatización y Gestión de Datos Energéticos")
    ventana.resize(1920, 1080)  # Establece un tamaño grande
    ventana.showMaximized()

    # Estilo general
    app.setStyleSheet('''
        QWidget {
            font-family: "Open Sans";
            background-color: #E6EEF5; /* Fondo claro por defecto */
        }
        QLabel {
            font-size: 16px;
            color: #1F2937; /* Texto oscuro */
        }
        QPushButton {
            padding: 8px 12px;
            font-size: 15px;
            background-color: #F97316; /* Naranja corporativo */
            color: #FFFFFF; /* Texto blanco */
            border: none;
            border-radius: 6px;
            min-width: 120px;
        }
        QPushButton:hover {
            background-color: #EA580C; /* Naranja más oscuro */
        }
        QLineEdit, QTextEdit, QListWidget {
            background-color: #FFFFFF;
            border: 1px solid #CBD5E1; /* Borde gris claro */
            padding: 5px;
            border-radius: 4px;
        }
    ''')

    # Layout principal
    layout_principal = QtWidgets.QVBoxLayout(ventana)
    layout_principal.setContentsMargins(10, 10, 10, 10)
    layout_principal.setSpacing(15)

    # Encabezado principal
    titulo = QtWidgets.QLabel("Automatización de Datos Históricos")
    titulo.setFont(QtGui.QFont("Arial", 24, QtGui.QFont.Bold))
    layout_principal.addWidget(titulo, alignment=Qt.AlignCenter)

    # Crear sistema de pestañas
    tabs = QtWidgets.QTabWidget()
    layout_principal.addWidget(tabs)

    # Pestaña 1: Consultar consumo de cliente
    tab1 = QtWidgets.QWidget()
    tabs.addTab(tab1, "Consultar consumo de cliente")
    layout_tab1 = QtWidgets.QVBoxLayout(tab1)

    # Contenedor del formulario
    formulario_tab1 = QtWidgets.QGridLayout()
    formulario_tab1.setSpacing(15)

    # Campos del formulario Tab 1
    entrada_ruta = QtWidgets.QLineEdit()
    entrada_ruta.setReadOnly(True)
    entrada_ruta.setPlaceholderText("Ruta Carpeta Base de Datos (.Parquet)")
    entrada_ruta.setText(str(script_dir))
    boton_ruta = QtWidgets.QPushButton("Cargar Carpeta")
    boton_ruta.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab1.addWidget(QtWidgets.QLabel("Ruta Carpeta Base de Datos (.Parquet):"), 0, 0)
    formulario_tab1.addWidget(entrada_ruta, 0, 1)
    formulario_tab1.addWidget(boton_ruta, 0, 2)

    # Resto de los elementos de Tab 1
    entrada_fecha_inicio = QtWidgets.QLineEdit()
    entrada_fecha_inicio.setPlaceholderText("AAAA-MM")
    formulario_tab1.addWidget(QtWidgets.QLabel("Fecha Inicio: AAAA-MM"), 1, 0)
    formulario_tab1.addWidget(entrada_fecha_inicio, 1, 1)

    entrada_fecha_fin = QtWidgets.QLineEdit()
    entrada_fecha_fin.setPlaceholderText("AAAA-MM")
    formulario_tab1.addWidget(QtWidgets.QLabel("Fecha Fin: AAAA-MM"), 2, 0)
    formulario_tab1.addWidget(entrada_fecha_fin, 2, 1)

    entrada_cliente = QtWidgets.QLineEdit()
    entrada_cliente.setPlaceholderText("Cliente")
    boton_buscar_cliente = QtWidgets.QPushButton("Buscar Cliente")
    boton_buscar_cliente.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab1.addWidget(QtWidgets.QLabel("Cliente:"), 3, 0)
    formulario_tab1.addWidget(entrada_cliente, 3, 1)
    formulario_tab1.addWidget(boton_buscar_cliente, 3, 2)
    contador_clientes_label = QtWidgets.QLabel("Clientes Encontrados: ")
    formulario_tab1.addWidget(contador_clientes_label, 5, 0, 1, 3)  # Agregarlo al layout

    boton_mostrar_consumo_xlsx = QtWidgets.QPushButton("Exportar Consumo a Excel")
    boton_mostrar_consumo_xlsx.setFixedSize(220, 30)  # Ancho: 250px, Alto: 30px
    boton_mostrar_consumo_xlsx.setCursor(QCursor(Qt.PointingHandCursor))

    boton_mostrar_consumo_powerbi = QtWidgets.QPushButton("Exportar Consumo a Power BI")
    boton_mostrar_consumo_powerbi.setFixedSize(220, 30)  # Ancho: 280px, Alto: 45px
    boton_mostrar_consumo_powerbi.setCursor(QCursor(Qt.PointingHandCursor))

        # Agregar los botones al layout
    formulario_tab1.addWidget(boton_mostrar_consumo_powerbi)
    formulario_tab1.addWidget(boton_mostrar_consumo_xlsx)

    layout_tab1.addLayout(formulario_tab1)


        # Barra de estado específica de Tab 1
    mensaje_label_tab1 = QtWidgets.QLabel("Barra de estado:")
    mensaje_label_tab1.setFont(QtGui.QFont("Open Sans", 14))
    mensaje_label_tab1.setStyleSheet("margin-top: 15px; color: #4B5563;")
    layout_tab1.addWidget(mensaje_label_tab1)

    mensaje_estado_tab1 = QtWidgets.QTextEdit()
    mensaje_estado_tab1.setFont(QtGui.QFont("Open Sans", 10))
    mensaje_estado_tab1.setReadOnly(True)
    mensaje_estado_tab1.setFixedHeight(100)
    layout_tab1.addWidget(mensaje_estado_tab1)

    # Resultados específicos de Tab 1
    lista_resultados_tab1 = QtWidgets.QListWidget()
    layout_tab1.addWidget(QtWidgets.QLabel("Resultados:"))
    lista_resultados_tab1.setFont(QtGui.QFont("Open Sans", 10))
    layout_tab1.addWidget(lista_resultados_tab1)


    # Pestaña 2: Consultar costo marginal barra
    tab2 = QtWidgets.QWidget()
    tabs.addTab(tab2, "Consultar costo marginal barra")
    layout_tab2 = QtWidgets.QVBoxLayout(tab2)

    formulario_tab2 = QtWidgets.QGridLayout()
    formulario_tab2.setSpacing(15)

    entrada_ruta2 = QtWidgets.QLineEdit()
    entrada_ruta2.setReadOnly(False)  # Habilitar edición para los campos
    entrada_ruta2.setPlaceholderText("Ruta Carpeta Base de Datos (.Parquet)")
    entrada_ruta2.setText(str(script_dir))
    boton_ruta2 = QtWidgets.QPushButton("Cargar Carpeta")
    boton_ruta2.setCursor(QCursor(Qt.PointingHandCursor))

    entrada_fecha_inicio_tab2 = QtWidgets.QLineEdit()
    entrada_fecha_inicio_tab2.setPlaceholderText("AAAA-MM")

    entrada_fecha_fin_tab2 = QtWidgets.QLineEdit()
    entrada_fecha_fin_tab2.setPlaceholderText("AAAA-MM")

    entrada_barra = QtWidgets.QLineEdit()
    entrada_barra.setPlaceholderText("Barra")
    boton_buscar_barra = QtWidgets.QPushButton("Buscar Barra")
    boton_buscar_barra.setCursor(QCursor(Qt.PointingHandCursor))

    formulario_tab2.addWidget(QtWidgets.QLabel("Ruta Carpeta Base de Datos (.Parquet):"), 0, 0)
    formulario_tab2.addWidget(entrada_ruta2, 0, 1)
    formulario_tab2.addWidget(boton_ruta2, 0, 2)

    formulario_tab2.addWidget(QtWidgets.QLabel("Fecha Inicio:"), 1, 0)
    formulario_tab2.addWidget(entrada_fecha_inicio_tab2, 1, 1)

    formulario_tab2.addWidget(QtWidgets.QLabel("Fecha Fin:"), 2, 0)
    formulario_tab2.addWidget(entrada_fecha_fin_tab2, 2, 1)

    formulario_tab2.addWidget(QtWidgets.QLabel("Barra:"), 3, 0)
    formulario_tab2.addWidget(entrada_barra, 3, 1)
    formulario_tab2.addWidget(boton_buscar_barra, 3, 2)

    # Barra de estado específica de Tab 2
    mensaje_label_tab2 = QtWidgets.QLabel("Barra de estado:")
    mensaje_label_tab2.setFont(QtGui.QFont("Open Sans", 14))
    mensaje_label_tab2.setStyleSheet("margin-top: 15px; color: #4B5563;")

    mensaje_estado_tab2 = QtWidgets.QTextEdit()
    mensaje_estado_tab2.setFont(QtGui.QFont("Open Sans", 10))
    mensaje_estado_tab2.setReadOnly(True)
    mensaje_estado_tab2.setFixedHeight(100)

    # Resultados específicos de Tab 2
    lista_resultados_tab2 = QtWidgets.QListWidget()
    lista_resultados_tab2.setFont(QtGui.QFont("Open Sans", 10))

    boton_extraer_cmg = QtWidgets.QPushButton("Extraer Costos Marginales")
    boton_extraer_cmg.setFixedSize(220, 30)  # Ancho: 100px, Alto: 30px
    boton_extraer_cmg.setCursor(QCursor(Qt.PointingHandCursor))
    boton_ver_clientes = QtWidgets.QPushButton("Ver Clientes Conectados")
    boton_ver_clientes.setFixedSize(220, 30)  # Ancho: 100px, Alto: 30px
    boton_ver_clientes.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab2.addWidget(boton_extraer_cmg)
    formulario_tab2.addWidget(boton_ver_clientes)

    layout_tab2.addLayout(formulario_tab2)
    layout_tab2.addWidget(mensaje_label_tab2)
    layout_tab2.addWidget(mensaje_estado_tab2)
    layout_tab2.addWidget(QtWidgets.QLabel("Resultados:"))
    layout_tab2.addWidget(lista_resultados_tab2)

    # Pestaña 3: Barracli vs barra iny
    tab3 = QtWidgets.QWidget()
    tabs.addTab(tab3, "Desacople Barra-CLI vs Barra-Iny")
    layout_tab3 = QtWidgets.QVBoxLayout(tab3)
    # Contenedor del formulario
    formulario_tab3 = QtWidgets.QGridLayout()
    formulario_tab3.setSpacing(15)

    # Campos del formulario con botones alineados
    entrada_ruta3 = QtWidgets.QLineEdit()
    entrada_ruta3.setReadOnly(True)
    entrada_ruta3.setPlaceholderText("Ruta Carpeta Base de Datos (.Parquet)")
    entrada_ruta3.setText(str(script_dir))
    boton_ruta3 = QtWidgets.QPushButton("Cargar Carpeta")
    boton_ruta3.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab3.addWidget(QtWidgets.QLabel("Ruta Carpeta Base de Datos (.Parquet):"), 0, 0)
    formulario_tab3.addWidget(entrada_ruta3, 0, 1)
    formulario_tab3.addWidget(boton_ruta3, 0, 2)


    entrada_fecha_inicio_tab3 = QtWidgets.QLineEdit()
    entrada_fecha_inicio_tab3.setPlaceholderText("AAAA-MM")
    formulario_tab3.addWidget(QtWidgets.QLabel("Fecha Inicio: "), 1, 0)
    formulario_tab3.addWidget(entrada_fecha_inicio_tab3, 1, 1)

    entrada_fecha_fin_tab3 = QtWidgets.QLineEdit()
    entrada_fecha_fin_tab3.setPlaceholderText("AAAA-MM")
    formulario_tab3.addWidget(QtWidgets.QLabel("Fecha Fin: "), 2, 0)
    formulario_tab3.addWidget(entrada_fecha_fin_tab3, 2, 1)

    # Barra de estado específica de Tab 3
    mensaje_label_tab3 = QtWidgets.QLabel("Barra de estado:")
    mensaje_label_tab3.setFont(QtGui.QFont("Open Sans", 14))
    mensaje_label_tab3.setStyleSheet("margin-top: 15px; color: #4B5563;")
    

    mensaje_estado_tab3 = QtWidgets.QTextEdit()
    mensaje_estado_tab3.setFont(QtGui.QFont("Open Sans", 10))
    mensaje_estado_tab3.setReadOnly(True)
    mensaje_estado_tab3.setFixedHeight(100)

    # Resultados específicos de Tab 3
    lista_resultados_tab3 = QtWidgets.QListWidget()
    lista_resultados_tab3.setFont(QtGui.QFont("Open Sans", 10))

    entrada_barra_tab3 = QtWidgets.QLineEdit()
    entrada_barra_tab3.setPlaceholderText("Barra")
    boton_buscar_barra3 = QtWidgets.QPushButton("Buscar Barra")
    boton_buscar_barra3.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab3.addWidget(QtWidgets.QLabel("Barra:"), 3, 0)
    formulario_tab3.addWidget(entrada_barra_tab3, 3, 1)
    formulario_tab3.addWidget(boton_buscar_barra3, 3, 2)

    entrada_cliente3 = QtWidgets.QLineEdit()
    entrada_cliente3.setPlaceholderText("Cliente")
    boton_buscar_cliente3 = QtWidgets.QPushButton("Buscar Cliente")
    boton_buscar_cliente3.setCursor(QCursor(Qt.PointingHandCursor))
    formulario_tab3.addWidget(QtWidgets.QLabel("Cliente:"), 4, 0)
    formulario_tab3.addWidget(entrada_cliente3, 4, 1)
    formulario_tab3.addWidget(boton_buscar_cliente3, 4, 2)

    
    # Botón adicional para extracción de datos
    boton_obtener_data = QtWidgets.QPushButton("Comparación Barras (Extraer datos POWER BI)")
    boton_obtener_data.setCursor(QCursor(Qt.PointingHandCursor))

    layout_tab3.addLayout(formulario_tab3)
    layout_tab3.addWidget(boton_obtener_data, alignment=Qt.AlignCenter)
    layout_tab3.addWidget(mensaje_label_tab3)
    layout_tab3.addWidget(mensaje_estado_tab3)
    layout_tab3.addWidget(QtWidgets.QLabel("Resultados:"))
    layout_tab3.addWidget(lista_resultados_tab3)
    

    # Pestaña 4: Actualizar base de datos
    tab4 = QtWidgets.QWidget()
    tabs.addTab(tab4, "Actualizar Base de Datos")
    layout_tab4 = QtWidgets.QVBoxLayout(tab4)

    # Título
    titulo_tab4 = QtWidgets.QLabel("ACTUALIZAR BASE DE DATOS")
    titulo_tab4.setFont(QtGui.QFont("Open Sans", 16, QtGui.QFont.Bold))
    titulo_tab4.setAlignment(Qt.AlignCenter)

    # Descripción
    descripcion_tab4 = QtWidgets.QLabel(
        "Datos descargados directamente desde el coordinador. "
        "Se actualizarán los consumos de clientes hasta la fecha: "
    )
    descripcion_tab4.setFont(QtGui.QFont("Open Sans", 12))
    descripcion_tab4.setWordWrap(True)
    descripcion_tab4.setAlignment(Qt.AlignCenter)

    # Mostrar fecha actual
    fecha_actual = QtWidgets.QLabel(QtCore.QDate.currentDate().toString("dd/MM/yyyy"))
    fecha_actual.setFont(QtGui.QFont("Open Sans", 12, QtGui.QFont.Bold))
    fecha_actual.setAlignment(Qt.AlignCenter)

    # Resultados específicos de Tab 4 (mantener como lista, pero no seleccionable)
    lista_resultados_tab4 = QtWidgets.QListWidget()
    lista_resultados_tab4.setFont(QtGui.QFont("Open Sans", 10))

    # Deshabilitar la selección de elementos
    lista_resultados_tab4.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)

    # Barra de estado específica de Tab 4
    mensaje_label_tab4 = QtWidgets.QLabel("Barra de estado:")
    mensaje_label_tab4.setFont(QtGui.QFont("Open Sans", 14))
    mensaje_label_tab4.setStyleSheet("margin-top: 15px; color: #4B5563;")
        
    mensaje_estado_tab4 = QtWidgets.QTextEdit()
    mensaje_estado_tab4.setFont(QtGui.QFont("Open Sans", 10))
    mensaje_estado_tab4.setReadOnly(True)
    mensaje_estado_tab4.setFixedHeight(100)


    # Botón para actualizar
    boton_actualizar = QtWidgets.QPushButton("ACTUALIZAR DATOS")
    boton_actualizar.setFont(QtGui.QFont("Open Sans", 12))
    boton_actualizar.setCursor(QCursor(Qt.PointingHandCursor))
    boton_actualizar.setFixedSize(200, 40)

    layout_tab4.addStretch()  # Añade espacio para centrar mejor los elementos

    def actualizar_datos():
        """
        Ejecuta la actualización de datos descargando archivos en un hilo separado.
        """
        # Evitar iniciar múltiples hilos al presionar el botón varias veces
        if hasattr(tab4, "worker") and tab4.worker.isRunning():
            QtWidgets.QMessageBox.warning(tab4, "Proceso en ejecución", "Ya se está actualizando la base de datos.")
            return

        # Crear el hilo
        tab4.worker = WorkerThread()

        # Conectar señales del hilo a la UI
        tab4.worker.resultados_signal.connect(lista_resultados_tab4.addItem)
        tab4.worker.resultados_signal.connect(lambda: lista_resultados_tab4.scrollToBottom())
        tab4.worker.estado_signal.connect(lambda texto: mensaje_estado_tab4.append(texto + "\n"))
        tab4.worker.estado_signal.connect(lambda: mensaje_estado_tab4.ensureCursorVisible())

        # Conectar finalización del hilo
        def mostrar_resultados_finales(lista_resultados):
            resultado_final = "\n".join(lista_resultados)
            QtWidgets.QMessageBox.information(tab4, "Actualización Completa", "Base de datos actualizada")
            tab4.worker = None  # Limpiar referencia al hilo

        tab4.worker.finalizado_signal.connect(mostrar_resultados_finales)

        # Iniciar el hilo
        tab4.worker.start()
        mensaje_estado_tab4.append("Actualización iniciada...\n")
        lista_resultados_tab4.clear()

    # Conectar el botón
    boton_actualizar.clicked.connect(actualizar_datos)

    # Layout para la pestaña 4
    layout_tab4.addWidget(titulo_tab4)
    layout_tab4.addWidget(descripcion_tab4)
    layout_tab4.addWidget(fecha_actual)
    layout_tab4.addWidget(mensaje_label_tab4)
    layout_tab4.addWidget(mensaje_estado_tab4)
    layout_tab4.addWidget(QtWidgets.QLabel("Resultados:"))
    layout_tab4.addWidget(lista_resultados_tab4)
    layout_tab4.addWidget(boton_actualizar)
    layout_tab4.addStretch()

    # Variables locales
    barra_inyeccion = None
    barra_retiro = None

    def abrir_power_bi(archivo_pbix):
        # Asegúrate de que la ventana tenga un atributo para almacenar los hilos activos
        if not hasattr(ventana, "hilos_activos"):
            ventana.hilos_activos = []

        # Verificar si ya hay un hilo PowerBIThread en ejecución
        if any(isinstance(h, PowerBIThread) and h.isRunning() for h in ventana.hilos_activos):
            escribir_mensaje("Power BI ya se está abriendo...")
            return

        # Crear una nueva instancia del hilo, pasando el archivo pbix como argumento
        hilo = PowerBIThread(archivo_pbix)
        hilo.mensaje.connect(escribir_mensaje)  # Conectar la señal para actualizar la interfaz
    
        # Definir la función de limpieza
        def limpiar_hilo():
            if hilo in ventana.hilos_activos:
                ventana.hilos_activos.remove(hilo)

        # Conectar la señal finished del hilo a la función de limpieza
        hilo.finished.connect(limpiar_hilo)

        # Agregar el hilo a la lista de hilos activos y luego iniciarlo
        ventana.hilos_activos.append(hilo)
        hilo.start()
        escribir_mensaje("Cargando Power BI, por favor espere...")


    def manejar_seleccion():
        nonlocal barra_inyeccion, barra_retiro
        pestaña_activa = tabs.currentIndex()  # Obtener el índice de la pestaña activa
        
        # Dependiendo de la pestaña activa, asignamos la lista correspondiente
        if pestaña_activa == 0:  # Pestaña 1
            lista_resultados = lista_resultados_tab1  # Lista de la primera pestaña
        elif pestaña_activa == 1:  # Pestaña 2
            lista_resultados = lista_resultados_tab2  # Lista de la segunda pestaña
        elif pestaña_activa == 2:  # Pestaña 3
            lista_resultados = lista_resultados_tab3  # Lista de la tercera pestaña

        item_seleccionado = lista_resultados.currentItem()

        if item_seleccionado:
            texto_seleccionado = item_seleccionado.text()

            # Si el texto contiene "(Barra:", se extraen cliente y barra
            if "(Barra:" in texto_seleccionado:
                cliente_seleccionado = texto_seleccionado.split("(Barra:")[0].strip()  # Extraer el cliente
                barra_select = texto_seleccionado.split("(Barra:")[1].strip().replace(")", "")

                entrada_cliente.setText(cliente_seleccionado)  # Actualizar el cliente
                barra_retiro = barra_select  # Asignar barra de retiro

                # Actualizar el label de retiro con la barra seleccionada

            else:
                # Si no contiene "(Barra:", asumimos que el texto es una barra de inyección
                barra_seleccionada = texto_seleccionado.strip()
                entrada_barra.setText(barra_seleccionada)  # Actualizar la barra de inyección

                barra_inyeccion = barra_seleccionada  # Asignar la barra de inyección

    
    def cargar_carpeta():
        carpeta = QtWidgets.QFileDialog.getExistingDirectory(
            None, 
            "Seleccionar Carpeta Base de Datos", 
            str(script_dir)
        )
        if carpeta:  # Si se selecciona una carpeta
            entrada_ruta.setText(carpeta)  # Actualizar la entrada con la ruta seleccionada

    # Funciones para actualizar mensajes en la interfaz
    def escribir_mensaje(texto):
        pestaña_activa = tabs.currentIndex()  # Obtener el índice de la pestaña activa

        # Dependiendo de la pestaña activa, asignamos el mensaje a la lista correspondiente
        if pestaña_activa == 0:  # Pestaña 1
            mensaje_estado_tab1.append(texto)  # Lista de mensajes de la primera pestaña
        elif pestaña_activa == 1:  # Pestaña 2
            mensaje_estado_tab2.append(texto)  # Lista de mensajes de la segunda pestaña
        elif pestaña_activa == 2:  # Pestaña 3
            mensaje_estado_tab3.append(texto)  # Lista de mensajes de la tercera pestaña

    def limpiar_mensaje_estado():
        pestaña_activa = tabs.currentIndex()  # Obtener el índice de la pestaña activa

        # Dependiendo de la pestaña activa, limpiamos el mensaje correspondiente
        if pestaña_activa == 0:  # Pestaña 1
            mensaje_estado_tab1.clear()  # Limpiar los mensajes de la primera pestaña
        elif pestaña_activa == 1:  # Pestaña 2
            mensaje_estado_tab2.clear()  # Limpiar los mensajes de la segunda pestaña
        elif pestaña_activa == 2:  # Pestaña 3
            mensaje_estado_tab3.clear()  # Limpiar los mensajes de la tercera pestaña


    def manejarSeleccionBarra():
        """
        Maneja la selección de una barra desde la interfaz, busca clientes asociados
        y muestra los resultados en el área de resultados.
        """
        try:
            # Limpiar los resultados anteriores en la interfaz
            lista_resultados_tab3.clear()

            # Obtener datos de entrada desde la interfaz
            ruta = Path(entrada_ruta.text())
            fecha_inicio = entrada_fecha_inicio.text()
            fecha_fin = entrada_fecha_fin.text()
            barra_seleccionada = entrada_barra.text().strip()  # Obtener la barra seleccionada desde la interfaz

            # Validar que se haya ingresado una barra válida
            if not barra_seleccionada:
                escribir_mensaje("Por favor, ingresa una barra válida.")
                return

            # Llamar a la función buscarClientesPorBarra
            start_time = time.time()
            clientes_asociados = buscarClientesPorBarra(
                folder=ruta,
                barra_seleccionada=barra_seleccionada,
                date_i=fecha_inicio,
                date_f=fecha_fin
            )
            end_time = time.time()
            escribir_mensaje(f"Clientes mostrados en {end_time - start_time:.2f} segundos.")
            # Mostrar los resultados en el área de resultados
            if clientes_asociados and not (len(clientes_asociados) == 1 and "Error" in clientes_asociados[0]):
                for cliente in clientes_asociados:
                    lista_resultados_tab3.addItem(cliente)  # Agregar a la lista de resultados visual
            else:
                escribir_mensaje("No se encontraron clientes para la barra seleccionada.")
        except Exception as e:
            escribir_mensaje(f"Error al manejar la selección de la barra: {str(e)}")


    def obtener_lista_resultados():
        pestaña_activa = tabs.currentIndex()  # Obtener el índice de la pestaña activa
        if pestaña_activa == 0:  # Pestaña 1
            return lista_resultados_tab1
        elif pestaña_activa == 1:  # Pestaña 2
            return lista_resultados_tab2
        elif pestaña_activa == 2:  # Pestaña 3
            return lista_resultados_tab3


    # Conectar botones a funciones
    boton_buscar_barra.clicked.connect(
    lambda: buscar_barra(
        entrada_ruta2.text(),
        entrada_fecha_inicio_tab2.text(),
        entrada_fecha_fin_tab2.text(),
        entrada_barra.text(),
        obtener_lista_resultados(),
        escribir_mensaje))
    
    # Conexión del botón al método buscar_cliente
    boton_buscar_cliente.clicked.connect(
        lambda: buscar_cliente(
            entrada_ruta.text(),  # Ruta
            entrada_fecha_inicio.text(),  # Fecha Inicio
            entrada_fecha_fin.text(),  # Fecha Fin
            entrada_cliente.text(),  # Cliente
            obtener_lista_resultados(),  # Resultados
            escribir_mensaje,  # Método para escribir en la barra de estado
            contador_clientes_label  # Añadir el contador de clientes encontrados
        )
    )
    boton_obtener_data.clicked.connect(
        lambda: (
            obtener_data(
                barra_seleccionada=barra_retiro,
                barra_inyec=barra_inyeccion,
                cliente_seleccionado=entrada_cliente.text(),
                ruta_original=entrada_ruta3.text(),
                fecha_inicio=entrada_fecha_inicio_tab3.text(),
                fecha_fin=entrada_fecha_fin_tab3.text(),
                lista_resultados=obtener_lista_resultados(),
                escribir_mensaje=escribir_mensaje
            ),
            abrir_power_bi(powerExtraerData)
        )
    )

    boton_extraer_cmg.clicked.connect(
        lambda: extraer_cmg(
            lista_resultados=obtener_lista_resultados(),
            entrada_ruta=entrada_ruta.text(),
            entrada_fecha_inicio=entrada_fecha_inicio.text(),
            entrada_fecha_fin=entrada_fecha_fin.text(),
            escribir_mensaje=escribir_mensaje
        )
    )
    boton_mostrar_consumo_xlsx.clicked.connect(
        lambda: mostrar_consumo(
            lista_resultados=obtener_lista_resultados(),
            entrada_ruta=entrada_ruta.text(),
            entrada_fecha_inicio=entrada_fecha_inicio.text(),
            entrada_fecha_fin=entrada_fecha_fin.text(),
            escribir_mensaje=escribir_mensaje,
            tipo_exportacion="excel"  # Exportar a Excel
        )
    )

    boton_mostrar_consumo_powerbi.clicked.connect(
        lambda: (
            mostrar_consumo(
                lista_resultados=obtener_lista_resultados(),
                entrada_ruta=entrada_ruta.text(),
                entrada_fecha_inicio=entrada_fecha_inicio.text(),
                entrada_fecha_fin=entrada_fecha_fin.text(),
                escribir_mensaje=escribir_mensaje,
                tipo_exportacion="powerbi"  # Exportar a Power BI
            ),
            abrir_power_bi(powerConsumoCli)  # Pasar archivo.pbix como argumento
        )
    )

    obtener_lista_resultados().itemSelectionChanged.connect(manejar_seleccion)
    boton_ruta.clicked.connect(cargar_carpeta)
    boton_ver_clientes.clicked.connect(manejarSeleccionBarra)

    # Mostrar la ventana
    ventana.setLayout(layout_principal)
    ventana.show()
    app.exec_()

# Ejecutar la función para crear la interfaz
crear_interfaz()
