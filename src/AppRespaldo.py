import time
from PyQt5 import QtWidgets, QtGui, QtCore
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QVBoxLayout, QPushButton, QLineEdit, QTextEdit, QListWidget, QFormLayout, QGridLayout, QProgressBar
from PyQt5.QtGui import QFont, QCursor
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from pathlib import Path
from bbdd_cmg import busca_barra_cmg, busca_cliente_bdd, get_cmg_barra, get_ivt_cliente, buscarClientesPorBarra
from PyQt5.QtWidgets import QTabWidget
import pandas as pd
from pathlib import Path
from openpyxl import Workbook
import subprocess
import os
from Metodos_Busqueda import buscar_barra, buscar_cliente
from Metodos_Extraccion import obtener_data, extraer_cmg, mostrar_consumo


class PowerBIThread(QThread):
    mensaje = pyqtSignal(str)  # Señal para enviar mensajes a la interfaz

    def run(self):
        archivo_pbix = r"C:\Users\elynnz\OneDrive - Grupo CGE\General CGE Cx\BaseDatos_CEN (CMg)\Practica\CGE_Automatización\BBDD_CMg_Cli Eliseo.pbix"
        ruta_power_bi = r"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe"

        # Verificar si el archivo .pbix y el ejecutable de Power BI existen
        if not os.path.isfile(archivo_pbix):
            self.mensaje.emit(f"El archivo .pbix no existe: {archivo_pbix}")
            return

        if not os.path.isfile(ruta_power_bi):
            self.mensaje.emit(f"El ejecutable de Power BI Desktop no se encuentra en la ruta: {ruta_power_bi}")
            return

        try:
            # Ejecutar Power BI Desktop con el archivo .pbix
            subprocess.run([ruta_power_bi, archivo_pbix], check=True)
            self.mensaje.emit("Archivo .pbix abierto correctamente con Power BI Desktop.")
        except Exception as e:
            self.mensaje.emit(f"Error al intentar abrir el archivo .pbix: {e}")

def crear_interfaz():
    # Obtener la ruta del directorio del script
    script_dir = Path(__file__).parent

    # Crear la aplicación
    app = QtWidgets.QApplication([]) 
    ventana = QtWidgets.QWidget()
    ventana.setWindowTitle("Automatización y Gestión de Datos Energéticos")
    ventana.setFixedSize(1200, 900)

    # Estilo general
    app.setStyleSheet('''
        QWidget {
            font-family: "Open Sans";
            background-color: #E6EEF5; /* Fondo claro por defecto */
        }
        QLabel {
            font-size: 14px;
            color: #1F2937; /* Texto oscuro */
        }
        QPushButton {
            padding: 8px 12px;
            font-size: 14px;
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
    titulo.setStyleSheet(''' ... ''')
    layout_principal.addWidget(titulo, alignment=Qt.AlignCenter)

    # Crear sistema de pestañas
    tabs = QtWidgets.QTabWidget()
    layout_principal.addWidget(tabs)

    # Pestaña 1: Formulario y datos principales
    tab1 = QtWidgets.QWidget()
    tabs.addTab(tab1, "Formulario Principal")
    layout_tab1 = QtWidgets.QVBoxLayout(tab1)

    # Contenedor del formulario
    formulario = QtWidgets.QGridLayout()
    formulario.setSpacing(15)

    # Campos del formulario con botones alineados
    entrada_ruta = QtWidgets.QLineEdit()
    entrada_ruta.setReadOnly(True)
    entrada_ruta.setPlaceholderText("Ruta Carpeta Base de Datos (.Parquet)")
    entrada_ruta.setText(str(script_dir))
    boton_ruta = QtWidgets.QPushButton("Cargar Carpeta")
    boton_ruta.setCursor(QCursor(Qt.PointingHandCursor))
    formulario.addWidget(QtWidgets.QLabel("Ruta Carpeta Base de Datos (.Parquet):"), 0, 0)
    formulario.addWidget(entrada_ruta, 0, 1)
    formulario.addWidget(boton_ruta, 0, 2)

    entrada_fecha_inicio = QtWidgets.QLineEdit()
    entrada_fecha_inicio.setPlaceholderText("AAAA-MM")
    formulario.addWidget(QtWidgets.QLabel("Fecha Inicio: AAAA-MM"), 1, 0)
    formulario.addWidget(entrada_fecha_inicio, 1, 1)

    entrada_fecha_fin = QtWidgets.QLineEdit()
    entrada_fecha_fin.setPlaceholderText("AAAA-MM")
    formulario.addWidget(QtWidgets.QLabel("Fecha Fin: AAAA-MM"), 2, 0)
    formulario.addWidget(entrada_fecha_fin, 2, 1)

    entrada_barra = QtWidgets.QLineEdit()
    entrada_barra.setPlaceholderText("Barra")
    boton_buscar_barra = QtWidgets.QPushButton("Buscar Barra")
    boton_buscar_barra.setCursor(QCursor(Qt.PointingHandCursor))
    formulario.addWidget(QtWidgets.QLabel("Barra:"), 3, 0)
    formulario.addWidget(entrada_barra, 3, 1)
    formulario.addWidget(boton_buscar_barra, 3, 2)

    entrada_cliente = QtWidgets.QLineEdit()
    entrada_cliente.setPlaceholderText("Cliente")
    boton_buscar_cliente = QtWidgets.QPushButton("Buscar Cliente")
    boton_buscar_cliente.setCursor(QCursor(Qt.PointingHandCursor))
    formulario.addWidget(QtWidgets.QLabel("Cliente:"), 4, 0)
    formulario.addWidget(entrada_cliente, 4, 1)
    formulario.addWidget(boton_buscar_cliente, 4, 2)

    layout_tab1.addLayout(formulario)

    # Botón adicional para extracción de datos
    boton_obtener_data = QtWidgets.QPushButton("Comparación Barras (Extraer datos POWER BI)")
    boton_obtener_data.setCursor(QCursor(Qt.PointingHandCursor))
    layout_tab1.addWidget(boton_obtener_data, alignment=Qt.AlignCenter)

    # Barra de estado
    mensaje_label = QtWidgets.QLabel("Barra de estado:")
    mensaje_label.setFont(QtGui.QFont("Open Sans", 14))
    mensaje_label.setStyleSheet("margin-top: 15px; color: #4B5563;")
    layout_tab1.addWidget(mensaje_label)

    mensaje_estado = QtWidgets.QTextEdit()
    mensaje_estado.setReadOnly(True)
    mensaje_estado.setFixedHeight(100)
    layout_tab1.addWidget(mensaje_estado)

    # Agregar una sección para mostrar las barras seleccionadas
    layout_barras_seleccionadas = QtWidgets.QVBoxLayout()
    layout_tab1.addLayout(layout_barras_seleccionadas)

    label_inyeccion = QtWidgets.QLabel("Barra de inyección: Ninguna")
    label_inyeccion.setStyleSheet("font-size: 14px; font-weight: bold; color: #2563EB;")
    layout_barras_seleccionadas.addWidget(label_inyeccion)

    label_retiro = QtWidgets.QLabel("Barra de retiro: Ninguna")
    label_retiro.setStyleSheet("font-size: 14px; font-weight: bold; color: #EF4444;")
    layout_barras_seleccionadas.addWidget(label_retiro)

    # Lista para resultados
    lista_resultados = QtWidgets.QListWidget()
    layout_tab1.addWidget(lista_resultados)

    # Layout para botones dinámicos
    layout_botones_dinamicos = QtWidgets.QVBoxLayout()
    layout_tab1.addLayout(layout_botones_dinamicos)

    # Pestaña 2: Espacio reservado para funcionalidades futuras
    tab2 = QtWidgets.QWidget()
    tabs.addTab(tab2, "Descargar Base De Datos")
    layout_tab2 = QtWidgets.QVBoxLayout(tab2)

    label_resultados = QtWidgets.QLabel("Espacio para resultados y análisis")
    label_resultados.setAlignment(Qt.AlignCenter)
    layout_tab2.addWidget(label_resultados)


    # Contenedor para el botón en la esquina superior derecha
    contenedor_boton = QtWidgets.QWidget()
    layout_boton = QtWidgets.QHBoxLayout(contenedor_boton)
    layout_boton.setContentsMargins(0, 0, 10, 0)  # Alineación a la derecha
    layout_boton.setAlignment(Qt.AlignRight)
    
    # Botón para activar el modo oscuro
    boton_modo_oscuro = QtWidgets.QPushButton("Activar Modo Oscuro")
    boton_modo_oscuro.setCursor(QCursor(Qt.PointingHandCursor))
    layout_boton.addWidget(boton_modo_oscuro)
    # Botón para abrir Power BI
    boton_abrir_power_bi = QPushButton("Abrir Archivo Power BI")
    boton_abrir_power_bi.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
    layout_principal.addWidget(boton_abrir_power_bi, alignment=QtCore.Qt.AlignCenter)

    # Añadir el contenedor del botón al layout principal (a la parte superior)
    layout_principal.addWidget(contenedor_boton)

    # Variable de control para saber si está activado el modo oscuro
    modo_oscuro = False

    # Función para activar/desactivar el modo oscuro
    def activar_modo_oscuro():
        nonlocal modo_oscuro
        if modo_oscuro:
            # Modo claro
            app.setStyleSheet('''
                QWidget {
                    font-family: "Open Sans";
                    background-color: #E6EEF5; /* Fondo claro */
                }
                QLabel {
                    font-size: 14px;
                    color: #1F2937; /* Texto oscuro */
                }
                QPushButton {
                    padding: 8px 12px;
                    font-size: 14px;
                    background-color: #F97316;
                    color: #FFFFFF;
                    border: none;
                    border-radius: 6px;
                    min-width: 120px;
                }
                QPushButton:hover {
                    background-color: #EA580C;
                }
                QLineEdit, QTextEdit, QListWidget {
                    background-color: #FFFFFF;
                    color: #1F2937;
                    border: 1px solid #CBD5E1;
                    padding: 5px;
                    border-radius: 4px;
                }
            ''')
            boton_modo_oscuro.setText("Activar Modo Oscuro")
        else:
            # Modo oscuro
            app.setStyleSheet('''
                QWidget {
                    font-family: "Open Sans";
                    background-color: #333333; /* Fondo oscuro */
                }
                QLabel {
                    font-size: 14px;
                    color: #FFFFFF; /* Texto claro */
                }
                QPushButton {
                    padding: 8px 12px;
                    font-size: 14px;
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
                    background-color: #555555; /* Fondo oscuro */
                    color: #FFFFFF; /* Texto claro */
                    border: 1px solid #CBD5E1;
                    padding: 5px;
                    border-radius: 4px;
                }
            ''')
            boton_modo_oscuro.setText("Desactivar Modo Oscuro")
        
        modo_oscuro = not modo_oscuro



    # Variables locales
    barra_inyeccion = None
    barra_retiro = None
    ctrl_presionado = False


    def abrir_power_bi():
        # Asegúrate de que la ventana tenga un atributo para almacenar los hilos activos
        if not hasattr(ventana, "hilos_activos"):
            ventana.hilos_activos = []

        # Crear una nueva instancia del hilo
        hilo = PowerBIThread()
        hilo.mensaje.connect(escribir_mensaje)  # Conectar la señal para actualizar la interfaz

        # Conectar la señal `finished` para limpiar el hilo de la lista cuando termine
        def limpiar_hilo():
            if hilo in ventana.hilos_activos:
                ventana.hilos_activos.remove(hilo)
        hilo.finished.connect(limpiar_hilo)

        # Agregar el hilo a la lista de hilos activos y luego iniciarlo
        ventana.hilos_activos.append(hilo)
        hilo.start()


    def manejar_seleccion():
        nonlocal barra_inyeccion, barra_retiro
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
                label_retiro.setText(f"Barra de retiro: {barra_retiro}")

            else:
                # Si no contiene "(Barra:", asumimos que el texto es una barra de inyección
                barra_seleccionada = texto_seleccionado.strip()
                entrada_barra.setText(barra_seleccionada)  # Actualizar la barra de inyección

                barra_inyeccion = barra_seleccionada  # Asignar la barra de inyección
                label_inyeccion.setText(f"Barra de inyección: {barra_inyeccion}")
    
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
        mensaje_estado.append(texto)

    def limpiar_mensaje_estado():
        mensaje_estado.clear()

    def agregar_botones(tipo_busqueda):
        # Limpiar botones previos
        for i in reversed(range(layout_botones_dinamicos.count())):
            layout_botones_dinamicos.itemAt(i).widget().setParent(None)

        if tipo_busqueda == "barra":
            boton_extraer_cmg = QtWidgets.QPushButton("Extraer Costos Marginales")
            boton_extraer_cmg.setFixedSize(100, 30)  # Ancho: 100px, Alto: 30px
            boton_extraer_cmg.setCursor(QCursor(Qt.PointingHandCursor))
            layout_botones_dinamicos.addWidget(boton_extraer_cmg)
            boton_extraer_cmg.clicked.connect(
                lambda: extraer_cmg(
                    lista_resultados=lista_resultados,
                    entrada_ruta=entrada_ruta.text(),
                    entrada_fecha_inicio=entrada_fecha_inicio.text(),
                    entrada_fecha_fin=entrada_fecha_fin.text(),
                    escribir_mensaje=escribir_mensaje
                )
            )
            boton_ver_clientes = QtWidgets.QPushButton("Ver Clientes Conectados")
            boton_ver_clientes.setFixedSize(100, 30)  # Ancho: 100px, Alto: 30px
            boton_ver_clientes.setCursor(QCursor(Qt.PointingHandCursor))
            layout_botones_dinamicos.addWidget(boton_ver_clientes)
            boton_ver_clientes.clicked.connect(manejarSeleccionBarra)

        elif tipo_busqueda == "cliente":
            boton_mostrar_consumo = QtWidgets.QPushButton("Mostrar Consumo")
            boton_mostrar_consumo.setFixedSize(100, 30)  # Ancho: 100px, Alto: 30px
            boton_mostrar_consumo.setCursor(QCursor(Qt.PointingHandCursor))
            layout_botones_dinamicos.addWidget(boton_mostrar_consumo)
            boton_mostrar_consumo.clicked.connect(
                lambda: mostrar_consumo(
                    lista_resultados=lista_resultados,
                    entrada_ruta=entrada_ruta.text(),
                    entrada_fecha_inicio=entrada_fecha_inicio.text(),
                    entrada_fecha_fin=entrada_fecha_fin.text(),
                    escribir_mensaje=escribir_mensaje
                )
            )


    def manejarSeleccionBarra():
        """
        Maneja la selección de una barra desde la interfaz, busca clientes asociados
        y muestra los resultados en el área de resultados.
        """
        try:
            # Limpiar los resultados anteriores en la interfaz
            lista_resultados.clear()

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
                    lista_resultados.addItem(cliente)  # Agregar a la lista de resultados visual
            else:
                escribir_mensaje("No se encontraron clientes para la barra seleccionada.")
        except Exception as e:
            escribir_mensaje(f"Error al manejar la selección de la barra: {str(e)}")


    # Conectar botones a funciones
    boton_buscar_barra.clicked.connect(
    lambda: buscar_barra(
        entrada_ruta.text(),
        entrada_fecha_inicio.text(),
        entrada_fecha_fin.text(),
        entrada_barra.text(),
        lista_resultados,
        escribir_mensaje,
        agregar_botones))

    boton_buscar_cliente.clicked.connect(
        lambda: buscar_cliente(
            entrada_ruta.text(),
            entrada_fecha_inicio.text(),
            entrada_fecha_fin.text(),
            entrada_cliente.text(),
            lista_resultados,
            escribir_mensaje,
            agregar_botones
        )
    )

    boton_obtener_data.clicked.connect(
        lambda: obtener_data(
            barra_seleccionada=barra_retiro,
            barra_inyec=barra_inyeccion,
            cliente_seleccionado=entrada_cliente.text(),
            ruta_original=entrada_ruta.text(),
            fecha_inicio=entrada_fecha_inicio.text(),
            fecha_fin=entrada_fecha_fin.text(),
            lista_resultados=lista_resultados,
            escribir_mensaje=escribir_mensaje
        )
    )
    lista_resultados.itemSelectionChanged.connect(manejar_seleccion)
    boton_modo_oscuro.clicked.connect(activar_modo_oscuro)
    boton_ruta.clicked.connect(cargar_carpeta)
    boton_abrir_power_bi.clicked.connect(abrir_power_bi)


    # Mostrar la ventana
    ventana.setLayout(layout_principal)
    ventana.show()
    app.exec_()

# Ejecutar la función para crear la interfaz
crear_interfaz()
