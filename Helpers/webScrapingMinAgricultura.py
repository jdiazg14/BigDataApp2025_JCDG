import os
import time
import requests
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

class WebScrapingMinAgricultura:

    def __init__(self, base_url, headless=True):
        self.base_url = base_url.rstrip("/") + "/"
        self.headless = headless

        # Categorías reales en el sitio del MinAgricultura
        self.categorias = {
            1: "conpes",
            2: "leyes",
            3: "resoluciones",
            4: "decretos",
            5: "jurisprudencia"
        }

    # ---------------------------
    # Iniciar Playwright una vez
    # ---------------------------
    def start(self):
        self.play = sync_playwright().start()
        self.browser = self.play.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context()
        self.page = self.context.new_page()

    # ---------------------------
    # Cerrar Playwright
    # ---------------------------
    def stop(self):
        self.context.close()
        self.browser.close()
        self.play.stop()

    # --------------------------------------------
    # Extraer enlaces PDF de una categoría del MinAgricultura
    # --------------------------------------------
    def _extraer_enlaces_categoria(self, tipo_id):
        if tipo_id not in self.categorias:
            print(f" ❌ ERROR: Categoría inválida: {tipo_id}")
            return []
        if tipo_id == 5:
            url = f"{self.base_url}SitePages/NormativaJurisprudencia.aspx"
        else:
            url = f"{self.base_url}SitePages/buscador-general-normas.aspx?t={tipo_id}"

        print(f"\n=== CATEGORÍA {tipo_id} ===")
        print(f"Visitando: {url}")

        # Bloque de navegación con manejo de errores
        try:
            print(" → Intentando cargar la página...", url)
            self.page.goto(url, timeout=60000, wait_until="load")       # Esperar a que la página cargue completamente
            print(" → Página cargada correctamente.")
        except Exception as e:
            print(" ❌ ERROR en goto():", e)
            raise e  # <-- Propaga el error real

        # Bloque de espera del contenido dinámico
        if tipo_id in [1, 2, 3, 4]:                         # Categorías 1 a 4
            selector = "div.export a[href$='.pdf']"
        elif tipo_id == 5:
            selector = "td.ms-vb a[href$='.pdf'], td.ms-vb a[href*='.pdf']"   # Categoría 5 (Jurisprudencia)
        else:
            print(f"⚠ Tipo_id {tipo_id} no soportado.")
            return []
        try:
            self.page.wait_for_selector(selector, timeout=30000)  # Esperar hasta 30 segundos
            #print(" → Selector encontrado:", selector)
        except TimeoutError as e:
            print(f" ⚠ Selector no apareció: {selector} — continuando igual…")

        enlaces = []
        links = self.page.locator(selector).all()   # Obtener todos los enlaces que coinciden con el selector

        # Conteo de enlaces encontrados para depuración
        total = len(links)
        print(f" → PDF en div.export encontrados: {total}")
        
        for link in links:
            href = link.get_attribute("href")
            print(" → HREF encontrado: ", href)
            if not href:                            # Si no hay href, saltar
                continue
            pdf_url = urljoin(url, href)            # Construir URL absoluta
            if "/Normatividad/" not in pdf_url:     # Filtrar enlaces que no pertenezcan a la sección de Normatividad
                print("   [Descartado por filtro 'Normatividad']", pdf_url)
                continue
            print("   [AGREGADO]", pdf_url)
            enlaces.append(urljoin(url, href))       # Añadir enlace a la lista

        return enlaces

    # --------------------------------------------
    # Extraer enlaces de todas las categorías
    # --------------------------------------------
    def extraer_todos_los_enlaces(self):
        self.start()
        enlaces = []

        '''
        # Iterar sobre todas las categorías
        for tipo_id in self.categorias.keys():
            encontrados = self._extraer_enlaces_categoria(tipo_id)
            enlaces.extend(encontrados)
        '''

        # Una sola categoría (para pruebas)
        enlaces = self._extraer_enlaces_categoria(5)

        self.stop()

        # eliminar duplicados
        enlaces = list(set(enlaces))

        return enlaces

    # --------------------------------------------
    # Descargar archivos PDF con requests
    # --------------------------------------------
    def descargar_archivos(self, enlaces, upload_dir):
        total = len(enlaces)
        descargados = 0
        errores = 0

        print("\n===== INICIANDO DESCARGAS =====")
        print(f"Total de enlaces a procesar: {total}\n")

        for url in enlaces:
            try:
                nombre_archivo = url.split("/")[-1].replace("%20", "_")
                ruta_destino = os.path.join(upload_dir, nombre_archivo)

                print(f"[DESCARGANDO] {url}")
                print(f" → Guardar como: {nombre_archivo}")
                
                # --- DESCARGA ROBUSTA CON STREAM ---
                with requests.get(url, stream=True, timeout=180) as r:
                    if r.status_code != 200:
                        print(f"   ✖ ERROR HTTP {r.status_code}")
                        errores += 1
                        continue

                    with open(ruta_destino, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 32):
                            if chunk:
                                f.write(chunk)

                # --- VALIDAR TAMAÑO ---
                tamaño = os.path.getsize(ruta_destino)
                if tamaño < 5000:
                    print(f"   ✖ Archivo demasiado pequeño ({tamaño} bytes). Posible descarga incompleta.")
                    errores += 1
                    continue

                # --- VALIDAR EOF MARKER ---
                with open(ruta_destino, "rb") as f:
                    f.seek(-2048, os.SEEK_END)   # Leer últimos 2KB
                    final = f.read()

                if b"%%EOF" not in final:
                    print("   ✖ Archivo sin marcador EOF → descarga truncada.")
                    errores += 1
                    continue

                # --- OK ---
                print(f"   ✔ DESCARGADO ({tamaño} bytes)")
                descargados += 1                

            except Exception as e:
                print(f"   ✖ ERROR EXCEPCIÓN: {e}")
                errores += 1

        print("\n===== DESCARGAS FINALIZADAS =====")
        print(f"Total: {total}")
        print(f"Descargados: {descargados}")
        print(f"Errores: {errores}")

        # devolver conteos al backend
        return {
            "total": total,
            "descargados": descargados,
            "errores": errores
        }