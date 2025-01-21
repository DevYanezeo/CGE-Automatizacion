import urllib.request
import urllib.error
import tqdm
from pathlib import Path
from datetime import datetime, timedelta

# Configuración inicial
BASE_URL = "https://www.coordinador.cl/wp-content/uploads"
DOWNLOAD_PATH = Path(r'C:\Users\elynnz\OneDrive - Grupo CGE\General CGE Cx\BaseDatos_CEN (CMg)\Practica\CGE_Automatización\CarpetaBDD')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Accept-Encoding": "gzip, deflate, br, zstd"
}
def descargar_archivos(escribir_mensaje, lista_resultados, actualizar_estado):
    """Descarga archivos CMG e IVT con progreso mostrado en lista_resultados."""

    def check_file_exists(url):
        """Verifica si un archivo existe mediante una solicitud HEAD."""
        req = urllib.request.Request(url, headers=HEADERS, method="HEAD")
        try:
            response = urllib.request.urlopen(req)
            return response.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            escribir_mensaje(f"Error al verificar archivo en {url}: {e}")
            return False
        except urllib.error.URLError as e:
            escribir_mensaje(f"Error de conexión al intentar acceder a {url}: {e}")
            return False

    def download_with_progress(url, file_name, folder):
        """Descarga un archivo desde una URL con barra de progreso reflejada en lista_resultados."""
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            response = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            escribir_mensaje(f"Error en la descarga de: {url}")
            escribir_mensaje(str(e))
            return False
        except urllib.error.URLError as e:
            escribir_mensaje(f"Error de conexión al descargar {url}: {e}")
            return False

        total_size = int(response.info().get('Content-Length', 0))
        block_size = 2 * 1024 * 1024

        progress_bar = tqdm.tqdm(total=total_size, unit='B', unit_scale=True, desc="Descargando")
        full_file = folder / file_name
        with open(full_file, 'wb') as file:
            while True:
                data = response.read(block_size)
                if not data:
                    break
                progress_bar.update(len(data))
                file.write(data)
                # Actualiza progreso en la lista de resultados
                progress = progress_bar.n / total_size * 100
                lista_resultados.append(f"Descargando {file_name}: {progress:.2f}%")
                actualizar_estado(f"Descargando {file_name}: {progress:.2f}%")  # Actualiza la barra de progreso
        progress_bar.close()
        escribir_mensaje(f"Archivo descargado exitosamente: {full_file}")
        return True

    def build_cmg_url(year, month, day):
        """Construye la URL para el archivo CMG para una fecha específica."""
        formatted_date = f"{year % 100:02d}{month:02d}{day:02d}"  # Formato AAMMDD
        return f"{BASE_URL}/{year}/{month:02d}/Antecedentes_CMG_Real_def_{formatted_date}.zip"

    def find_cmg_file():
        """Busca el archivo CMG más reciente desde la fecha actual hacia atrás."""
        today = datetime.now()
        date = today

        while date.year >= 2021:
            url = build_cmg_url(date.year, date.month, date.day)
            escribir_mensaje(f"Verificando archivo CMG en: {url}")

            if check_file_exists(url):
                escribir_mensaje(f"Archivo CMG encontrado: {url}")
                return url

            escribir_mensaje(f"Archivo CMG no encontrado para la fecha {date.strftime('%d/%m/%Y')}. Retrocediendo...")
            date -= timedelta(days=1)

        escribir_mensaje("No se encontró un archivo CMG válido en el rango de fechas.")
        return None

    def build_ivt_url(year, month):
        """Construye la URL para el archivo IVT para un año y mes específicos."""
        prev_month = (datetime(year, month, 1) - timedelta(days=1)).date()
        formatted_date = f"{prev_month.year % 100:02d}{prev_month.month:02d}"
        return f"{BASE_URL}/{year}/{month:02d}/03-Bases-de-Datos_{formatted_date}_BD01-2.zip"

    def find_ivt_file():
        """Busca un archivo IVT válido desde la fecha actual hacia atrás."""
        date = datetime.now()

        while date.year >= 2021:
            url = build_ivt_url(date.year, date.month)
            escribir_mensaje(f"Verificando archivo IVT en: {url}")

            if check_file_exists(url):
                escribir_mensaje(f"Archivo IVT encontrado: {url}")
                return url

            escribir_mensaje(f"Archivo IVT no encontrado para la fecha {date.strftime('%d/%m/%Y')}. Retrocediendo...")
            date -= timedelta(days=30)

        escribir_mensaje("No se encontró un archivo IVT válido en el rango de fechas.")
        return None

    # --- Descarga de CMG ---
    cmg_url = find_cmg_file()
    if cmg_url:
        cmg_file_name = cmg_url.split("/")[-1]
        if (DOWNLOAD_PATH / cmg_file_name).exists():
            escribir_mensaje(f"El archivo CMG ya existe: {DOWNLOAD_PATH / cmg_file_name}")
        else:
            if download_with_progress(cmg_url, cmg_file_name, DOWNLOAD_PATH):
                lista_resultados.append(cmg_file_name)

    # --- Descarga de IVT ---
    ivt_url = find_ivt_file()
    if ivt_url:
        ivt_file_name = ivt_url.split("/")[-1]
        if (DOWNLOAD_PATH / ivt_file_name).exists():
            escribir_mensaje(f"El archivo IVT ya existe: {DOWNLOAD_PATH / ivt_file_name}")
        else:
            if download_with_progress(ivt_url, ivt_file_name, DOWNLOAD_PATH):
                lista_resultados.append(ivt_file_name)
