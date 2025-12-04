import pandas as pd
import numpy as np
import re
import spacy
import nltk
from nltk.corpus import stopwords
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer
from transformers import pipeline
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Descargar recursos de NLTK si no están disponibles
try:
    nltk.download('stopwords', quiet=True)
    nltk.download('punkt', quiet=True)
except Exception as e:
    print(f"Advertencia al descargar recursos NLTK: {e}")


class PLN:
    """Clase para procesamiento de lenguaje natural en español"""
    
    def __init__(self, modelo_spacy: str = 'es_core_news_lg', 
                 modelo_embeddings: str = 'paraphrase-multilingual-MiniLM-L12-v2',
                 cargar_modelos: bool = True):
        """
        Inicializa la clase PLN con los modelos necesarios
        
        Args:
            modelo_spacy: Nombre del modelo de spaCy a cargar
            modelo_embeddings: Nombre del modelo de SentenceTransformer
            cargar_modelos: Si True, carga los modelos al inicializar (puede tardar)
        """
        self.modelo_spacy_nombre = modelo_spacy
        self.modelo_embeddings_nombre = modelo_embeddings
        self.nlp = None
        self.model_embeddings = None
        self.stopwords_es = None
        self.ner_legal = None
        self.embedder = None
        
        if cargar_modelos:
            self._cargar_modelos()
    
    def _cargar_modelos(self):
        """Carga los modelos de PLN necesarios"""
        try:
            print("Cargando modelo de spaCy...")
            self.nlp = spacy.load(self.modelo_spacy_nombre)
            self.nlp.max_length = 3_000_000  # permite textos largos
            print(f"Modelo spaCy '{self.modelo_spacy_nombre}' cargado correctamente")
        except OSError:
            print(f"Error: Modelo '{self.modelo_spacy_nombre}' no encontrado.")
            print(f"Ejecuta: python -m spacy download {self.modelo_spacy_nombre}")
            print("Usando modelo básico de spaCy...")
            try:
                self.nlp = spacy.load('es_core_news_sm')
            except OSError:
                print("Error: No se pudo cargar ningún modelo de spaCy")
                self.nlp = None
        
        try:
            print("Cargando modelo de embeddings...")
            self.model_embeddings = SentenceTransformer(self.modelo_embeddings_nombre)
            print(f"Modelo de embeddings '{self.modelo_embeddings_nombre}' cargado correctamente")
        except Exception as e:
            print(f"Error al cargar modelo de embeddings: {e}")
            self.model_embeddings = None
        
        try:
            self.stopwords_es = set(stopwords.words('spanish'))
        except LookupError:
            nltk.download('stopwords', quiet=True)
            self.stopwords_es = set(stopwords.words('spanish'))

        # Embeddings para encabezado
        try:
            self.embedder = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        except:
            print("Error cargando embedder para encabezado")
            self.embedder = None
    
    def extraer_entidades(self, texto: str) -> Dict[str, List[str]]:
        """
        Extrae entidades nombradas del texto usando spaCy.
        
        Args:
            texto: Texto a analizar
            
        Returns:
            Diccionario con entidades clasificadas por tipo
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        
        entidades = {
            'personas': [],
            'lugares': [],
            'organizaciones': [],
            'fechas': [],
            'leyes': [],
            'otros': []
        }
        
        for ent in doc.ents:
            if ent.label_ == 'PER':
                entidades['personas'].append(ent.text)
            elif ent.label_ == 'LOC':
                entidades['lugares'].append(ent.text)
            elif ent.label_ == 'ORG':
                entidades['organizaciones'].append(ent.text)
            elif ent.label_ == 'DATE':
                entidades['fechas'].append(ent.text)
            elif ent.label_ == 'LAW' or 'ley' in ent.text.lower():
                entidades['leyes'].append(ent.text)
            else:
                entidades['otros'].append(f"{ent.text} ({ent.label_})")
        
        # Eliminar duplicados manteniendo orden
        for key in entidades:
            entidades[key] = list(dict.fromkeys(entidades[key]))
        
        return entidades
    
    def extraer_temas(self, texto: str, top_n: int = 10) -> List[Tuple[str, float]]:
        """
        Extrae los temas/palabras clave más importantes del texto.
        
        Args:
            texto: Texto a analizar
            top_n: Número de temas a extraer
            
        Returns:
            Lista de tuplas (palabra, relevancia)
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        
        # Filtrar stopwords y tokens no relevantes
        palabras_relevantes = []
        
        for token in doc:
            if (not token.is_stop and
                not token.is_punct and
                not token.is_space and
                len(token.text) > 3 and
                token.pos_ in ['NOUN', 'PROPN', 'ADJ', 'VERB']):
                palabras_relevantes.append(token.lemma_.lower())
        
        # Contar frecuencias
        contador = Counter(palabras_relevantes)
        temas = contador.most_common(top_n)
        
        # Convertir frecuencias a porcentajes para consistencia con el tipo de retorno
        total_palabras = len(palabras_relevantes)
        if total_palabras > 0:
            temas = [(palabra, (freq / total_palabras) * 100) for palabra, freq in temas]
        else:
            temas = [(palabra, 0.0) for palabra, freq in temas]
        
        return temas
    
    def generar_resumen(self, texto: str, num_oraciones: int = 3) -> str:
        """
        Genera un resumen extractivo del texto usando TF-IDF.
        
        Args:
            texto: Texto a resumir
            num_oraciones: Número de oraciones en el resumen
            
        Returns:
            Resumen del texto
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        oraciones = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 20]
        
        if len(oraciones) <= num_oraciones:
            return ' '.join(oraciones)
        
        if len(oraciones) == 0:
            return texto[:200] + "..." if len(texto) > 200 else texto
        
        # Calcular importancia usando TF-IDF
        try:
            vectorizer = TfidfVectorizer(stop_words=list(self.stopwords_es))
            tfidf_matrix = vectorizer.fit_transform(oraciones)
            
            # Sumar puntuaciones TF-IDF por oración
            puntuaciones = np.array(tfidf_matrix.sum(axis=1)).flatten()
            
            # Obtener índices de las oraciones más importantes
            indices_importantes = puntuaciones.argsort()[-num_oraciones:][::-1]
            indices_importantes = sorted(indices_importantes)
            
            resumen = ' '.join([oraciones[i] for i in indices_importantes])
            return resumen
        except Exception as e:
            print(f"Error al generar resumen: {e}")
            # Fallback: devolver primeras oraciones
            return ' '.join(oraciones[:num_oraciones])
    
    def calcular_similitud_semantica(self, textos: List[str]) -> pd.DataFrame:
        """
        Calcula similitud semántica usando embeddings de transformers.
        Método más avanzado que captura mejor el significado.
        
        Args:
            textos: Lista de textos a comparar
            
        Returns:
            DataFrame con matriz de similitud
        """
        if not self.model_embeddings:
            raise ValueError("Modelo de embeddings no está cargado. Llama a _cargar_modelos() primero.")
        
        if len(textos) < 2:
            raise ValueError("Se necesitan al menos 2 textos para calcular similitud")
        
        # Generar embeddings
        embeddings = self.model_embeddings.encode(textos)
        
        # Calcular similitud del coseno
        similitud = cosine_similarity(embeddings)
        
        # Crear DataFrame
        df = pd.DataFrame(
            similitud,
            columns=[f'Texto {i+1}' for i in range(len(textos))],
            index=[f'Texto {i+1}' for i in range(len(textos))]
        )
        
        return df
    
    def preprocesar_texto(self, texto: str, 
                          remover_stopwords: bool = True,
                          lematizar: bool = True,
                          remover_numeros: bool = False,
                          min_longitud: int = 3) -> str:
        """
        Preprocesa un texto aplicando diferentes transformaciones.
        
        Args:
            texto: Texto a preprocesar
            remover_stopwords: Si True, remueve palabras vacías
            lematizar: Si True, lematiza las palabras
            remover_numeros: Si True, remueve números
            min_longitud: Longitud mínima de las palabras a conservar
            
        Returns:
            Texto preprocesado
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        palabras_procesadas = []
        
        for token in doc:
            # Filtrar por longitud
            if len(token.text) < min_longitud:
                continue
            
            # Remover stopwords
            if remover_stopwords and token.is_stop:
                continue
            
            # Remover puntuación y espacios
            if token.is_punct or token.is_space:
                continue
            
            # Remover números
            if remover_numeros and token.like_num:
                continue
            
            # Lematizar o usar texto original
            if lematizar:
                palabra = token.lemma_.lower()
            else:
                palabra = token.text.lower()
            
            palabras_procesadas.append(palabra)
        
        return ' '.join(palabras_procesadas)
    
    def analizar_sentimiento(self, texto: str, modelo: str = 'nlptown/bert-base-multilingual-uncased-sentiment') -> Dict:
        """
        Analiza el sentimiento de un texto usando transformers.
        
        Args:
            texto: Texto a analizar
            modelo: Modelo de sentimiento a usar
            
        Returns:
            Diccionario con el análisis de sentimiento
        """
        try:
            classifier = pipeline('sentiment-analysis', 
                                model=modelo,
                                tokenizer=modelo)
            resultado = classifier(texto)
            return {
                'sentimiento': resultado[0]['label'],
                'score': resultado[0]['score']
            }
        except Exception as e:
            print(f"Error al analizar sentimiento: {e}")
            return {
                'sentimiento': 'ERROR',
                'score': 0.0,
                'error': str(e)
            }
    
    def extraer_nombres_propios(self, texto: str) -> List[str]:
        """
        Extrae nombres propios (PROPN) del texto.
        
        Args:
            texto: Texto a analizar
            
        Returns:
            Lista de nombres propios encontrados
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        nombres_propios = []
        
        for token in doc:
            if token.pos_ == 'PROPN' and len(token.text) > 2:
                nombres_propios.append(token.text)
        
        # Eliminar duplicados manteniendo orden
        return list(dict.fromkeys(nombres_propios))
    
    def contar_palabras(self, texto: str, unicas: bool = False) -> int:
        """
        Cuenta las palabras en un texto.
        
        Args:
            texto: Texto a analizar
            unicas: Si True, cuenta solo palabras únicas
            
        Returns:
            Número de palabras
        """
        if not self.nlp:
            raise ValueError("Modelo de spaCy no está cargado. Llama a _cargar_modelos() primero.")
        
        doc = self.nlp(texto)
        palabras = [token.text.lower() for token in doc 
                   if not token.is_punct and not token.is_space and not token.is_stop]
        
        if unicas:
            return len(set(palabras))
        return len(palabras)
    
    def close(self):
        """Libera recursos de los modelos"""
        # Los modelos de spaCy y transformers se liberan automáticamente
        # pero podemos agregar limpieza aquí si es necesario
        pass

    def dividir_en_chunks(self, texto: str, max_chars: int = 800000) -> List[str]:
        """Divide el texto en bloques para evitar límite de spaCy."""
        chunks = []
        for i in range(0, len(texto), max_chars):
            chunks.append(texto[i:i + max_chars])
        return chunks
    
    def procesar_texto_largo(self, texto: str) -> Dict:
        """Procesa textos largos dividiéndolos en chunks."""
        if len(texto) <= 900000:
            # No requiere chunking
            return {
                "entidades": self.extraer_entidades(texto),
                "temas": self.extraer_temas(texto),
                "resumen": self.generar_resumen(texto)
            }

        print(f" → Texto demasiado largo ({len(texto)} chars). Dividiendo en chunks…")
        chunks = self.dividir_en_chunks(texto)

        entidades_total = {
            "personas": [],
            "lugares": [],
            "organizaciones": [],
            "fechas": [],
            "leyes": [],
            "otros": []
        }
        resumen_total = ""

        for i, parte in enumerate(chunks):
            print(f"   → Procesando chunk {i+1}/{len(chunks)}")

            # ENTIDADES
            ents = self.extraer_entidades(parte)
            for key in entidades_total:
                entidades_total[key].extend(ents[key])

            # RESUMEN (simple pero funcional)
            resumen_total += self.generar_resumen(parte, num_oraciones=2) + "\n"

        # Eliminar duplicados de entidades
        for key in entidades_total:
            entidades_total[key] = list(dict.fromkeys(entidades_total[key]))

        return {
            "entidades": entidades_total,
            "temas": self.extraer_temas(texto[:500000]),  # temas solo sobre parte inicial
            "resumen": resumen_total.strip()
        }
    
    def _normalizar_fecha(self, texto):
        if not texto:
            return None

        import re
        t = texto.lower().replace("/", "-").strip()

        MESES = {
            "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
            "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
            "noviembre":11,"diciembre":12,
            "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
            "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12
        }

        # Formato “07 jul 2025”
        m = re.match(r"(\d{1,2})\s+([a-záéíóú]+)\s+(\d{2,4})", t)
        if m:
            d = int(m.group(1))
            mes = MESES.get(m.group(2), None)
            y = int(m.group(3))
            if y < 100: y += 2000
            if mes:
                return f"{y:04d}-{mes:02d}-{d:02d}"

        # Formato “20-marzo-26”
        m = re.match(r"(\d{1,2})-(\w+)-(\d{1,4})", t)
        if m:
            d = int(m.group(1))
            mes = MESES.get(m.group(2), None)
            y = int(m.group(3))
            if y < 100: y += 2000
            if mes:
                return f"{y:04d}-{mes:02d}-{d:02d}"

        return None
    
    def extraer_metadatos_norma(self, texto):
        """
        Extrae tipo, número, fecha, año y entidad emisora de una norma usando:
        - spaCy NER (es_core_news_lg)
        - Heurísticas legales robustas
        """

        # Tomar las primeras líneas (encabezado típico de normas)
        lineas = texto.split("\n")
        encabezado = "\n".join(lineas[:20]).upper()

        meta = {
            "tipo_norma": None,
            "numero_norma": None,
            "anio_norma": None,
            "entidad_emisora": None,
            "fecha_documento": None,
            "titulo_norma": None
        }

        # NER spaCy
        doc = self.nlp(encabezado)
        ents = [(ent.label_, ent.text) for ent in doc.ents]

        # ENTIDAD EMISORA
        for label, val in ents:
            if label == "ORG" and len(val) > 5:
                meta["entidad_emisora"] = val.upper()
                break

        # FECHAS por NER
        fechas_detectadas = [val for (lab, val) in ents if lab == "DATE"]

        for f in fechas_detectadas:
            print(" → Fecha detectada (NER):", f)
            fecha_norm = self._normalizar_fecha(f)
            if fecha_norm:
                print("   → Fecha normalizada:", fecha_norm)
                meta["fecha_documento"] = fecha_norm
                meta["anio_norma"] = int(fecha_norm[:4])
                break

        #  BÚSQUEDA GLOBAL DE FECHA
        if not meta["fecha_documento"]:
            # Buscar formato "26 de marzo de 2019"
            m = re.search(r"\b(\d{1,2})\s+de\s+([a-zA-ZÁÉÍÓÚáéíóú]+)\s+de\s+(\d{4})", texto, re.IGNORECASE)
            if m:
                dia = m.group(1)
                mes = m.group(2)
                anio = m.group(3)
                meta["fecha_documento"] = self._normalizar_fecha(f"{dia}-{mes}-{anio}")
                meta["anio_norma"] = int(anio)

        # Segundo patrón tipo "Bogotá, D.C., 26 de marzo de 2019"
        if not meta["fecha_documento"]:
            m = re.search(r"\b(\d{1,2}\s+de\s+[a-zA-ZÁÉÍÓÚáéíóú]+\s+\d{4})", texto, re.IGNORECASE)
            if m:
                meta["fecha_documento"] = self._normalizar_fecha(m.group(1))

        # Tercer patrón: fechas largas tipo "BOGOTÁ, D.C, 26 DE MARZO DE 2019"
        if not meta["fecha_documento"]:
            m = re.search(r"(\d{1,2}\s+DE\s+[A-ZÁÉÍÓÚ]+\s+DE\s+\d{4})", texto)
            if m:
                meta["fecha_documento"] = self._normalizar_fecha(m.group(1))
        
        # TIPO DE NORMA
        tipos = ["DECRETO", "RESOLUCIÓN", "RESOLUCION", "LEY", "CIRCULAR", "ACUERDO"]
        for t in tipos:
            if t in encabezado:
                meta["tipo_norma"] = "RESOLUCIÓN" if t == "RESOLUCION" else t
                break

        # NÚMERO DE NORMA
        m = re.search(r"(DECRETO|RESOLUCIÓN|RESOLUCION|LEY)\s+N?O?\.?\s*(\d{2,5})", encabezado)
        if m:
            meta["numero_norma"] = int(m.group(2))
        else:
            m = re.search(r"\b(\d{2,5})\b", encabezado)
            if m:
                meta["numero_norma"] = int(m.group(1))

        # AÑO DE NORMA si falta
        if not meta["anio_norma"]:
            m = re.search(r"\b(19\d{2}|20\d{2})\b", encabezado)
            if m:
                meta["anio_norma"] = int(m.group(1))

        # TÍTULO
        for l in lineas[:30]:
            if l.strip().lower().startswith(("por el cual", "por la cual")):
                meta["titulo_norma"] = l.strip()
                break

        return meta