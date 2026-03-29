# Guía de Instalación — BigDataApp2025

Esta guía describe el proceso completo para configurar y ejecutar el proyecto en Windows.
Incluye configuración para desarrollo local y para despliegues en producción/cloud.

---

## Requisitos Previos

- Windows 10 / 11 (64 bits)
- Python 3.10 o superior
- Git

---

## 0. Requisitos del Sistema (Python)

Antes de continuar, verifique que tiene instalado **Python 3.10 o superior**.

Descarga oficial:
[https://www.python.org/downloads/](https://www.python.org/downloads/)

> **Importante (Windows):** Durante la instalación, marque obligatoriamente la casilla **"Add Python to PATH"**. Si no lo hace, los comandos `python` y `pip` no funcionarán en la terminal.

---

## 1. Requisitos de Software

### 1.1 MongoDB Community Server

1. Descargue el instalador MSI desde el sitio oficial:
   [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community)
   - Seleccione: **Version → 7.x**, **Platform → Windows**, **Package → MSI**

2. Ejecute el instalador y siga el asistente de instalación.
   - En el paso de configuración, seleccione **"Install MongoD as a Service"**.
   - Deje el directorio de datos por defecto: `C:\Program Files\MongoDB\Server\7.0\data\`.

3. Confirme que **MongoDB Compass** se instale junto al servidor (opción incluida en el instalador).

### 1.2 Elasticsearch

1. Descargue el paquete ZIP desde el sitio oficial:
   [https://www.elastic.co/downloads/elasticsearch](https://www.elastic.co/downloads/elasticsearch)
   - Seleccione la versión **8.x** compatible con el cliente instalado (`elasticsearch==8.11.0`).

2. Extraiga el contenido del ZIP en un directorio de su elección, por ejemplo:
   ```
   C:\elasticsearch-8.x.x\
   ```

3. Elasticsearch no requiere instalación adicional. Se ejecuta directamente desde su directorio extraído.

---

## 2. Configuración de Bases de Datos

### 2.1 Iniciar MongoDB como Servicio

MongoDB queda registrado como servicio de Windows durante la instalación. Para verificar su estado e iniciarlo manualmente si es necesario, ejecute en PowerShell con privilegios de administrador:

```powershell
# Verificar el estado del servicio
Get-Service -Name MongoDB

# Iniciar el servicio si está detenido
Start-Service -Name MongoDB
```

Para configurar el inicio automático con Windows:

```powershell
Set-Service -Name MongoDB -StartupType Automatic
```

La instancia estará disponible en `mongodb://localhost:27017` por defecto.

### 2.2 Ejecutar Elasticsearch

Navegue al directorio donde extrajo Elasticsearch y ejecute el script de inicio:

```cmd
cd C:\elasticsearch-8.x.x\bin
elasticsearch.bat
```

Espere a que el proceso indique que el nodo está listo. Elasticsearch estará disponible en `http://localhost:9200` por defecto.

> **Nota de seguridad:** En la primera ejecución, Elasticsearch genera automáticamente credenciales y un token de enrollment. Guarde la contraseña del usuario `elastic` que se muestra en la consola, ya que se solicita una única vez. Configúrela en el archivo `.env` del proyecto.

Para deshabilitar la autenticación en un entorno estrictamente local y de desarrollo, edite `C:\elasticsearch-8.x.x\config\elasticsearch.yml` y agregue:

```yaml
xpack.security.enabled: false
```

---

## 3. Entorno de Python

Ejecute los siguientes comandos desde la raíz del proyecto en PowerShell.

### 3.1 Crear el entorno virtual

```powershell
python -m venv venv
```

### 3.2 Activar el entorno virtual

```powershell
.\venv\Scripts\activate
```

El prefijo `(venv)` en el prompt indica que el entorno está activo.

### 3.3 Actualizar pip

Antes de instalar dependencias, actualice `pip` para asegurar que el gestor de paquetes pueda resolver correctamente versiones y metadatos recientes.

```powershell
python -m pip install --upgrade pip
```

### 3.4 Instalar las dependencias

```powershell
pip install -r requirements.txt
```

> **Nota:** El paquete `torch` puede tardar varios minutos en descargarse. Asegúrese de contar con conexión estable y al menos 5 GB de espacio libre en disco.

### 3.5 Instalar los navegadores de Playwright

El módulo `playwright` requiere un paso adicional para descargar los binarios de los navegadores:

```powershell
playwright install chromium
```

---

## 4. Modelos de NLP

### 4.1 Modelo de Spacy en Español

Con el entorno virtual activo, descargue el modelo `es_core_news_sm`:

```powershell
python -m spacy download es_core_news_sm
```

Verifique la instalación ejecutando:

```powershell
python -c "import spacy; nlp = spacy.load('es_core_news_sm'); print('Modelo cargado correctamente')"
```

---

## 5. Variables de Entorno (.env)

Cree un archivo `.env` en la raíz del proyecto. Use uno de los siguientes perfiles según su entorno.
Copie y ajuste estos valores en su archivo `.env` local.

> **Nota de seguridad:** El archivo `.env` contiene credenciales y secretos. **Nunca** debe subirse al repositorio.

### 5.1 Configuración para Desarrollo Local

```env
# Flask
SECRET_KEY=<TU_LLAVE_SECRETA_ALEATORIA>
UPLOAD_DIR=static/uploads

# MongoDB local
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=proyecto_bigData
MONGO_COLECCION=usuario_roles

# Elasticsearch local
ELASTIC_URL=http://localhost:9200
ELASTIC_USER=elastic
ELASTIC_PASSWORD=tu_password_local
ELASTIC_INDEX_DEFAULT=index_minagricultura

# Usuario administrador inicial
APP_USER_ADMIN=<TU_USUARIO>
APP_USER_ADMIN_PASSWORD=<TU_CONTRASEÑA>
```

### 5.2 Configuración para Producción/Cloud

```env
# Flask
SECRET_KEY=<TU_LLAVE_SECRETA_ALEATORIA>
UPLOAD_DIR=static/uploads

# MongoDB Atlas
MONGO_URI=mongodb+srv://usuario:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=proyecto_bigData
MONGO_COLECCION=usuario_roles

# Elastic Cloud
ELASTIC_URL=https://cluster-id.region.provider.elastic-cloud.com:9243
ELASTIC_USER=elastic
ELASTIC_PASSWORD=tu_password_cloud
ELASTIC_INDEX_DEFAULT=index_minagricultura

# Usuario administrador inicial
APP_USER_ADMIN=<TU_USUARIO>
APP_USER_ADMIN_PASSWORD=<TU_CONTRASEÑA>
```

### 5.3 Detección automática de entorno

La aplicación detecta automáticamente el entorno en tiempo de ejecución usando las variables anteriores:

- Si `MONGO_URI` contiene `localhost` o `127.0.0.1`, mostrará `✅ MongoDB Local: Conectado`; en caso contrario, `✅ MongoDB Atlas (Cloud): Conectado`.
- Si `ELASTIC_URL` contiene `localhost` o `127.0.0.1`, mostrará `✅ Elasticsearch Local: Conectado (vX.Y.Z)`; en caso contrario, `✅ Elasticsearch Cloud: Conectado (vX.Y.Z)`.

No es necesario cambiar el código al pasar de local a cloud: basta con ajustar el `.env`.

---

## 6. Inicializar la Base de Datos

Antes del primer inicio de la aplicación, ejecute el script de inicialización para crear el usuario administrador en MongoDB con la contraseña encriptada:

```powershell
python init_db.py
```

El script leerá la configuración de `APP_USER_ADMIN` y `APP_USER_ADMIN_PASSWORD` desde el archivo `.env` y creará ese usuario con contraseña almacenada con hash seguro (`werkzeug.security`). Si el usuario ya existe, el script no realiza ningún cambio (es idempotente).

> **Importante:** Use credenciales seguras y únicas para el usuario administrador desde el primer arranque.

---

## 7. Ejecutar la Aplicación

Con los servicios de MongoDB y Elasticsearch activos y el entorno virtual activado:

```powershell
python app.py
```

La aplicación estará disponible en `http://127.0.0.1:5000`.

---

## Resumen de Verificación

| Componente        | Verificación rápida                                      |
|-------------------|----------------------------------------------------------|
| MongoDB           | `Get-Service -Name MongoDB` → estado `Running`           |
| Elasticsearch     | `Invoke-RestMethod http://localhost:9200` en PowerShell  |
| Python / venv     | `python --version` con prefijo `(venv)` activo           |
| Spacy             | `python -c "import spacy; spacy.load('es_core_news_sm')"` |
| Aplicación Flask  | Acceder a `http://127.0.0.1:5000` en el navegador        |
