import os
import time
import json
from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class WebScraping:
    """
    Scraping dinámico universal con Playwright.
    - Funciona para cualquier página (estática o dinámica)
    - Extrae enlaces PDF/ASPX/PHP/ etc. 
    - No requiere interacción del usuario
    """

    def __init__(self, headless=True):
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None

    # PLAYWRIGHT
    def _start(self):
        """Inicia Playwright y navegador."""
        self.p = sync_playwright().start()
        self.browser = self.p.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(accept_downloads=True)
        self.page = self.context.new_page()

    def _stop(self):
        """Cierra Playwright."""
        try:
            if self.browser:
                self.browser.close()
            if self.p:
                self.p.stop()
        except:
            pass

    # EXTRACCIÓN DE ENLACES
    def extraer_links(self, url, selector_contenedor=None, extensiones=None):
        """
        Carga dinámicamente la página y extrae enlaces válidos.

        Args:
            url: URL a analizar.
            selector_contenedor: CSS selector opcional para limitar búsqueda.
            extensiones: lista como ["pdf", "aspx", "php"].

        Returns:
            Lista de diccionarios: { "url": "...", "type": "pdf" }
        """
        if extensiones is None:
            extensiones = ["pdf"]

        self._start()

        try:
            self.page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(1)

            html = self.page.content()
            soup = BeautifulSoup(html, "lxml")

            # Si hay selector de contenedor
            if selector_contenedor:
                contenedor = soup.select_one(selector_contenedor)
                if not contenedor:
                    print(f"[Advertencia] Contenedor '{selector_contenedor}' no encontrado. Se usa toda la página.")
                    contenedor = soup
            else:
                contenedor = soup

            enlaces = []

            # Buscar enlaces
            for link in contenedor.find_all("a", href=True):
                href = link["href"].strip()
                full_url = urljoin(url, href).lower()

                for ext in extensiones:
                    if full_url.endswith(f".{ext.lower()}"):
                        enlaces.append({
                            "url": full_url,
                            "type": ext.lower()
                        })
                        break

            # Quitar duplicados
            unicos = []
            vistos = set()

            for e in enlaces:
                if e["url"] not in vistos:
                    vistos.add(e["url"])
                    unicos.append(e)

            return unicos

        except Exception as e:
            print(f"Error durante el scraping: {e}")
            return []

        finally:
            self._stop()

    # DESCARGA DE ARCHIVOS
    def descargar_archivo(self, url_archivo, carpeta_destino):
        """
        Descarga con Playwright cualquier archivo accionado por una navegación.
        No usa Requests. La descarga proviene de un evento real del navegador.

        Args:
            url_archivo: URL del PDF o archivo a descargar.
            carpeta_destino: carpeta donde guardar archivo temporal.

        Return:
            Ruta absoluta del archivo guardado o None si falla.
        """
        self._start()

        try:
            # Navega directamente al archivo PDF
            with self.page.expect_download() as download_info:
                self.page.goto(url_archivo, wait_until="networkidle")

            download = download_info.value

            # Nombre sugerido por el servidor
            nombre = download.suggested_filename
            if not nombre.lower().endswith(".pdf"):
                nombre += ".pdf"

            # Crear carpeta si no existe
            os.makedirs(carpeta_destino, exist_ok=True)

            ruta_final = os.path.join(carpeta_destino, nombre)

            # Guardar archivo
            download.save_as(ruta_final)

            return ruta_final

        except Exception as e:
            print(f"Error descargando archivo desde {url_archivo}: {e}")
            return None

        finally:
            self._stop()