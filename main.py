from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import io
import re
import unicodedata
import os
import json
import logging
import psycopg2
import requests

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TIV Ingest API", version="2.0.0")

# Variables de entorno para PostgreSQL y n8n
POSTGRES_CONN_STR = os.environ.get("POSTGRES_CONNECTION_STRING")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL")

# ----------------------------
# Utilidades
# ----------------------------

def _normalize(s: str) -> str:
    """Normaliza strings para comparación."""
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _to_bool(x: Any) -> Optional[bool]:
    """Convierte valores a booleano."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = _normalize(str(x))
    if s in {"si","sí","true","1","x","y","yes"}:
        return True
    if s in {"no","false","0"}:
        return False
    return None

def _to_number(x: Any) -> Optional[float]:
    """Convierte valores a número, retorna None para 0 o valores inválidos."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        val = float(x)
        return val if val != 0 else None
    s = _normalize(str(x))
    s = s.replace(",", "")
    try:
        val = float(s)
        return val if val != 0 else None
    except:
        return None

def _read_table(file_bytes: bytes, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Lee un archivo Excel o CSV y retorna un DataFrame sin procesar."""
    name = filename.lower()
    
    if name.endswith(".xlsx") or name.endswith(".xlsm"):
        # Si no se especifica hoja, buscar la correcta
        if sheet_name is None:
            xl_file = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
            sheet_names = xl_file.sheet_names
            
            # Buscar hoja que contenga datos TIV (palabras clave)
            for sheet in sheet_names:
                sheet_lower = sheet.lower()
                if any(keyword in sheet_lower for keyword in 
                      ['tiv', 'valores', 'ubicaciones', 'schedule', 'bienes', 'riesgos']):
                    sheet_name = sheet
                    break
            
            # Si no se encontró, usar la primera hoja
            if sheet_name is None:
                sheet_name = sheet_names[0]
        
        # Leer el archivo completo sin header (para detectar formato)
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", 
                          sheet_name=sheet_name, header=None)
        return df
    
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), header=None)
    
    raise HTTPException(status_code=400, detail="Formato no soportado. Usa XLSX/XLSM/CSV.")

# ----------------------------
# Modelos (Pydantic)
# ----------------------------

class TIVItem(BaseModel):
    id_ubicacion: str = Field(..., json_schema_extra={"example": "U-001"})
    moneda: str = Field(..., json_schema_extra={"example": "USD"})
    valor_edificio: Optional[float] = Field(None, json_schema_extra={"example": 35000000})
    valor_maquinaria_contenidos: Optional[float] = Field(None, json_schema_extra={"example": 12000000})
    valor_stock: Optional[float] = Field(None, json_schema_extra={"example": 5000000})
    valor_mejoras: Optional[float] = Field(None, json_schema_extra={"example": 300000})
    incluye_bi: Optional[bool] = Field(None, json_schema_extra={"example": True})
    direccion: Optional[str] = Field(None, json_schema_extra={"example": "Cll 10 #20-30"})
    ciudad: Optional[str] = Field(None, json_schema_extra={"example": "Medellín"})
    pais: Optional[str] = Field(None, json_schema_extra={"example": "COL"})
    actividad: Optional[str] = Field(None, json_schema_extra={"example": "Envasado"})

class TIVResponse(BaseModel):
    documento: str = "tiv_excel"
    descripcion: str = "Schedule económico por ubicación y clase"
    items: List[TIVItem]
    columnas_detectadas: Dict[str, str]
    resumen: Dict[str, Any]

class ColumnMap(BaseModel):
    """Permite override opcional de mapeos de columnas."""
    mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="{'columna_en_excel':'clave_canonica'}"
    )

class PricingSheets(BaseModel):
    Valores: List[Dict[str, Any]]
    Ubicaciones: List[Dict[str, Any]]

class SiniestroItem(BaseModel):
    """Modelo para un registro de siniestro."""
    num_poliza: str
    fecha_siniestro: Optional[str] = None
    liquidado: Optional[float] = None
    reserva_actual: Optional[float] = None
    incurrido: Optional[float] = None
    producto: Optional[str] = None
    ramo_tecnico: Optional[str] = None
    anio_siniestro: Optional[int] = None

class SiniestrosResponse(BaseModel):
    """Respuesta con datos de siniestralidad consolidados."""
    items: List[SiniestroItem]
    total_incurrido: float
    total_liquidado: float
    total_reserva: float
    resumen_por_anio: Dict[int, Dict[str, float]]

class AnalisisTecnicoRequest(BaseModel):
    """Request para análisis técnico completo."""
    tipo_negocio: Optional[str] = Field(None, description="'nuevo' o 'renovacion'")
    slip_data: Optional[Dict[str, Any]] = Field(default_factory=dict)

class AnalisisTecnicoResponse(BaseModel):
    """Respuesta del análisis técnico."""
    analysis_id: Optional[int] = None
    tipo_negocio: str
    tiv_total: float
    burning_cost: float
    items_tiv: List[TIVItem]
    siniestros_procesados: int
    resumen_siniestros: Optional[Dict[str, Any]] = None
    slip_info: Dict[str, Any]
    n8n_triggered: bool
    mensaje: str

# Diccionario de alias para formato estándar
ALIAS_MAP = {
    "id ubicacion": "id_ubicacion",
    "id_ubicacion": "id_ubicacion",
    "codigo": "id_ubicacion",
    "site id": "id_ubicacion",
    "moneda": "moneda", "currency": "moneda",
    "edificio": "valor_edificio",
    "valor edificio": "valor_edificio",
    "building": "valor_edificio",
    "contenidos": "valor_maquinaria_contenidos",
    "maquinaria": "valor_maquinaria_contenidos",
    "maquinaria/contenidos": "valor_maquinaria_contenidos",
    "stock": "valor_stock", "inventario": "valor_stock",
    "mejoras": "valor_mejoras", "mejoras locativas": "valor_mejoras",
    "incluye bi": "incluye_bi", "bi": "incluye_bi", "alop": "incluye_bi",
    "direccion": "direccion", "dirección": "direccion", "address": "direccion",
    "ciudad": "ciudad", "city": "ciudad",
    "pais": "pais", "país": "pais", "country": "pais",
    "actividad": "actividad", "ocupacion": "actividad", "ocupación": "actividad"
}

CANONICAL_KEYS = set([
    "id_ubicacion","moneda","valor_edificio","valor_maquinaria_contenidos",
    "valor_stock","valor_mejoras","incluye_bi",
    "direccion","ciudad","pais","actividad"
])

# Mapeo de ciudades comunes colombianas
CIUDADES_COMUNES = {
    "medellin": "Medellín",
    "bogota": "Bogotá",
    "cali": "Cali",
    "barranquilla": "Barranquilla",
    "cartagena": "Cartagena",
    "cucuta": "Cúcuta",
    "bucaramanga": "Bucaramanga",
    "pereira": "Pereira",
    "manizales": "Manizales",
    "ibague": "Ibagué",
    "itagui": "Itagüí",
    "bello": "Bello",
    "envigado": "Envigado",
    "caucasia": "Caucasia",
    "sonson": "Sonsón",
    "rionegro": "Rionegro",
    "armenia": "Armenia",
    "pasto": "Pasto",
    "monteria": "Montería",
    "valledupar": "Valledupar"
}

# ----------------------------
# Parser para formato TRANSPUESTO
# ----------------------------

def parse_transposed_tiv(df: pd.DataFrame) -> List[TIVItem]:
    """
    Parsea formato TIV transpuesto donde:
    - Columnas = ubicaciones diferentes
    - Filas = tipos de bienes (Edificios, Maquinaria, Stock, etc.)
    
    Este formato es común en pólizas TRDM (Todo Riesgo Daño Material).
    """
    items = []
    
    # Buscar filas clave (usar columna 1, ya que columna 0 suele estar vacía)
    ubicaciones_row = None
    rows_map = {}
    
    for idx, row in df.iterrows():
        # La columna 1 contiene las etiquetas de las filas
        label = str(row.iloc[1]).lower().strip() if pd.notna(row.iloc[1]) else ""
        
        # Identificar fila de Edificios
        if "edificio" in label and "mueble" not in label and "asegurable" not in label:
            rows_map["edificio"] = idx
            # La fila de ubicaciones está 2 filas antes (típicamente)
            if ubicaciones_row is None and idx >= 2:
                ubicaciones_row = idx - 2
        
        # Identificar Muebles/Mejoras Locativas
        elif ("mueble" in label or "mejora" in label) and "locativa" in label:
            rows_map["mejoras"] = idx
        
        # Identificar Maquinaria
        elif "maquinaria" in label and "equipo" in label:
            rows_map["maquinaria"] = idx
        
        # Identificar Equipos Electrónicos
        elif "equipo" in label and ("electrico" in label or "electronico" in label):
            rows_map["equipos_electronicos"] = idx
        
        # Identificar Stock/Inventarios
        elif "stock" in label or "inventario" in label or ("elemento" in label and "almacen" in label):
            rows_map["stock"] = idx
    
    # Validar que se encontraron las filas necesarias
    if not rows_map or ubicaciones_row is None:
        return []
    
    # Obtener los nombres de las ubicaciones
    ubicaciones_data = df.iloc[ubicaciones_row]
    
    # Procesar cada ubicación (columnas desde la 3 en adelante)
    for col_idx in range(3, len(df.columns)):
        ubicacion_name = ubicaciones_data.iloc[col_idx]
        
        # Saltar columnas vacías o columna TOTAL
        if pd.isna(ubicacion_name) or _normalize(str(ubicacion_name)) == "total":
            continue
        
        ubicacion_str = str(ubicacion_name)
        
        # Extraer ID de ubicación y dirección
        direccion = None
        id_ubicacion = ubicacion_str
        
        # Si tiene salto de línea, la primera parte es el ID y la segunda la dirección
        if "\n" in ubicacion_str:
            parts = ubicacion_str.split("\n")
            id_ubicacion = parts[0].strip()
            if len(parts) > 1:
                direccion = parts[1].strip()
        
        # Limitar longitud del ID
        if len(id_ubicacion) > 100:
            id_ubicacion = id_ubicacion[:100]
        
        # Extraer valores de cada tipo de bien
        valor_edificio = None
        valor_maquinaria = None
        valor_mejoras = None
        valor_stock = None
        
        if "edificio" in rows_map:
            valor_edificio = _to_number(df.iloc[rows_map["edificio"]].iloc[col_idx])
        
        if "maquinaria" in rows_map:
            valor_maquinaria = _to_number(df.iloc[rows_map["maquinaria"]].iloc[col_idx])
        
        # Sumar equipos electrónicos a maquinaria
        if "equipos_electronicos" in rows_map:
            val_elec = _to_number(df.iloc[rows_map["equipos_electronicos"]].iloc[col_idx])
            if val_elec:
                valor_maquinaria = (valor_maquinaria or 0) + val_elec
        
        if "mejoras" in rows_map:
            valor_mejoras = _to_number(df.iloc[rows_map["mejoras"]].iloc[col_idx])
        
        if "stock" in rows_map:
            valor_stock = _to_number(df.iloc[rows_map["stock"]].iloc[col_idx])
        
        # Validar que tenga al menos un valor
        if not any([valor_edificio, valor_maquinaria, valor_stock, valor_mejoras]):
            continue
        
        # Intentar extraer ciudad del nombre de la ubicación
        ciudad = None
        pais = "COL"  # Por defecto Colombia
        
        ubicacion_lower = _normalize(id_ubicacion + " " + (direccion or ""))
        for clave, nombre in CIUDADES_COMUNES.items():
            if clave in ubicacion_lower:
                ciudad = nombre
                break
        
        # Crear item
        item = TIVItem(
            id_ubicacion=id_ubicacion,
            moneda="COP",  # Por defecto COP para formato colombiano
            valor_edificio=valor_edificio,
            valor_maquinaria_contenidos=valor_maquinaria,
            valor_stock=valor_stock,
            valor_mejoras=valor_mejoras,
            incluye_bi=None,  # No se puede determinar del formato transpuesto
            direccion=direccion,
            ciudad=ciudad,
            pais=pais,
            actividad=None  # No se puede determinar del formato transpuesto
        )
        items.append(item)
    
    return items

# ----------------------------
# Parser para formato ESTÁNDAR
# ----------------------------

def detect_columns(df: pd.DataFrame, user_map: Dict[str,str]) -> Dict[str,str]:
    """Detecta automáticamente las columnas del formato estándar."""
    mapping = {}
    
    # 1) Aplicar overrides de usuario
    for src, key in user_map.items():
        mapping[src] = key
    
    # 2) Detección automática por alias
    for col in df.columns:
        norm = _normalize(str(col))
        if col in mapping:
            continue
        if norm in ALIAS_MAP:
            mapping[col] = ALIAS_MAP[norm]
        elif norm in CANONICAL_KEYS:
            mapping[col] = norm
    
    return mapping

def apply_mapping(df: pd.DataFrame, mapping: Dict[str,str]) -> pd.DataFrame:
    """Aplica el mapeo de columnas detectadas."""
    cols = {}
    for src, tgt in mapping.items():
        if tgt in CANONICAL_KEYS and src in df.columns:
            cols[src] = tgt
    
    if not cols:
        raise HTTPException(status_code=422, 
                          detail="No se detectaron columnas válidas en el archivo.")
    
    return df.rename(columns=cols)

def coerce_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte una fila al formato esperado."""
    out = {}
    out["id_ubicacion"] = (str(row.get("id_ubicacion")).strip() 
                          if row.get("id_ubicacion") not in [None, "nan"] 
                          else None)
    out["moneda"] = (str(row.get("moneda")).upper().strip()
                     if row.get("moneda") not in [None,"nan"] 
                     else None)
    out["valor_edificio"] = _to_number(row.get("valor_edificio"))
    out["valor_maquinaria_contenidos"] = _to_number(row.get("valor_maquinaria_contenidos"))
    out["valor_stock"] = _to_number(row.get("valor_stock"))
    out["valor_mejoras"] = _to_number(row.get("valor_mejoras"))
    out["incluye_bi"] = _to_bool(row.get("incluye_bi"))
    
    # Campos de texto
    for k in ["direccion","ciudad","pais","actividad"]:
        v = row.get(k)
        out[k] = (None if (v is None or (isinstance(v, float) and pd.isna(v))) 
                 else str(v).strip())
    
    return out

def parse_standard_tiv(df: pd.DataFrame, user_map: Dict[str,str]) -> List[TIVItem]:
    """Parsea formato TIV estándar (filas = ubicaciones, columnas = campos)."""
    mapping = detect_columns(df, user_map)
    
    if not mapping:
        return []
    
    df = apply_mapping(df, mapping)
    canon_cols = list(set(mapping.values()))
    df = df[canon_cols]
    
    records = []
    for _, r in df.iterrows():
        coerced = coerce_row(r.to_dict())
        
        # Validaciones mínimas
        if not coerced["id_ubicacion"] or not coerced["moneda"]:
            continue
        
        records.append(TIVItem(**coerced))
    
    return records

# ----------------------------
# Función resumen
# ----------------------------

def summarize(items: List[TIVItem]) -> Dict[str, Any]:
    """Genera resumen de los items procesados."""
    df = pd.DataFrame([x.dict() for x in items])
    
    totales = {}
    for k in ["valor_edificio","valor_maquinaria_contenidos","valor_stock","valor_mejoras"]:
        if k in df.columns:
            totales[k] = float(df[k].fillna(0).sum())
    
    return {
        "n_items": len(items),
        "monedas_detectadas": sorted(list(set([x.moneda for x in items if x.moneda]))),
        "totales": totales
    }

# ----------------------------
# Funciones para Base de Datos y n8n
# ----------------------------

def get_db_connection():
    """Establece conexión con PostgreSQL."""
    if not POSTGRES_CONN_STR:
        logger.error("POSTGRES_CONNECTION_STRING no está configurada.")
        return None
    try:
        conn = psycopg2.connect(POSTGRES_CONN_STR)
        logger.info("Conexión a PostgreSQL establecida.")
        return conn
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        return None

def insert_analysis_results(conn, results_dict):
    """Inserta resultados del análisis técnico en la BD."""
    if not conn:
        logger.error("No hay conexión a BD.")
        return None
    
    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO analisis_tecnico_inicial 
        (tipo_negocio, tiv_total, burning_cost, otros_datos_json, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        RETURNING id;
        """
        tipo_negocio = results_dict.get('tipo_negocio', 'desconocido')
        tiv = results_dict.get('tiv_total', 0.0)
        bc = results_dict.get('burning_cost', 0.0)
        
        otros_datos = {k: v for k, v in results_dict.items() 
                      if k not in ['tipo_negocio', 'tiv_total', 'burning_cost']}
        
        cursor.execute(sql, (tipo_negocio, tiv, bc, json.dumps(otros_datos)))
        inserted_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        logger.info(f"Resultados insertados en BD con ID: {inserted_id}")
        return inserted_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error insertando en BD: {e}")
        if cursor:
            cursor.close()
        return None

def trigger_n8n_webhook(analysis_id):
    """Llama al webhook de n8n."""
    if not N8N_WEBHOOK_URL:
        logger.warning("N8N_WEBHOOK_URL no configurada.")
        return False
    if analysis_id is None:
        logger.error("analysis_id es None.")
        return False
    
    try:
        payload = {"analysis_id": analysis_id}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(N8N_WEBHOOK_URL, json=payload, 
                               headers=headers, timeout=15)
        response.raise_for_status()
        logger.info(f"Webhook n8n llamado para analysis_id: {analysis_id}")
        return True
    except requests.exceptions.Timeout:
        logger.error("Timeout llamando a n8n webhook.")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error llamando a n8n webhook: {e}")
        return False

# ----------------------------
# Funciones para Siniestralidad
# ----------------------------

def process_siniestralidad_file(file_content: bytes, filename: str) -> pd.DataFrame:
    """Procesa un archivo de siniestralidad y retorna DataFrame."""
    try:
        # Detectar encoding
        try:
            content_str = file_content.decode('utf-8')
        except UnicodeDecodeError:
            content_str = file_content.decode('latin-1', errors='ignore')
            logger.warning(f"Usando Latin-1 para {filename}")
        
        # Leer CSV
        df = pd.read_csv(io.StringIO(content_str), sep=None, engine='python',
                        on_bad_lines='warn')
        
        # Mapeo de columnas comunes
        column_mapping = {
            'num. poliza': 'Num. Poliza',
            'número póliza': 'Num. Poliza',
            'poliza': 'Num. Poliza',
            'fec. sini': 'Fec. Sini',
            'fecha siniestro': 'Fec. Sini',
            'liquidado': 'Liquidado',
            'rva. actual': 'Rva. Actual',
            'reserva': 'Rva. Actual',
            'nom. procucto': 'Nom. Procucto',
            'producto': 'Nom. Procucto',
            'ramo técnico': 'Ramo Técnico',
            'ramo tecnico': 'Ramo Técnico'
        }
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.lower()
        df.rename(columns=column_mapping, inplace=True)
        
        # Verificar columnas requeridas
        required = ['Num. Poliza', 'Liquidado', 'Rva. Actual']
        missing = [col for col in required if col not in df.columns]
        
        if missing:
            logger.warning(f"Columnas faltantes en {filename}: {missing}")
            return pd.DataFrame()
        
        # Convertir a numérico
        df['Liquidado'] = pd.to_numeric(df['Liquidado'], errors='coerce').fillna(0)
        df['Rva. Actual'] = pd.to_numeric(df['Rva. Actual'], errors='coerce').fillna(0)
        
        # Calcular Incurrido
        df['Incurrido'] = df['Liquidado'] + df['Rva. Actual']
        
        # Extraer año del siniestro
        if 'Fec. Sini' in df.columns:
            df['Fec. Sini'] = pd.to_datetime(df['Fec. Sini'], errors='coerce')
            df['año_siniestro'] = df['Fec. Sini'].dt.year
        
        # Filtrar por TRDM (Todo Riesgo Daño Material)
        if 'Ramo Técnico' in df.columns:
            df = df[df['Ramo Técnico'].str.contains('TRDM|Todo Riesgo|Daño Material', 
                                                    case=False, na=False)]
        
        logger.info(f"Procesado {filename}: {len(df)} registros")
        return df
        
    except Exception as e:
        logger.error(f"Error procesando {filename}: {e}")
        return pd.DataFrame()

def consolidar_siniestralidad(files: List[tuple]) -> pd.DataFrame:
    """
    Consolida múltiples archivos de siniestralidad.
    files: lista de tuplas (filename, file_content_bytes)
    """
    lista_dfs = []
    
    for filename, content in files:
        if 'siniestralidad' in filename.lower() or 'siniestro' in filename.lower():
            df = process_siniestralidad_file(content, filename)
            if not df.empty:
                lista_dfs.append(df)
    
    if not lista_dfs:
        logger.warning("No se encontraron datos de siniestralidad válidos.")
        return pd.DataFrame()
    
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    logger.info(f"Siniestralidad consolidada: {len(df_consolidado)} registros")
    return df_consolidado


def procesar_tiv_bytes(file_content: bytes, filename: str):
    """
    Procesa un archivo TIV desde bytes y retorna (df_tiv, tiv_total)
    Soporta CSV o Excel. Implementa la lógica de extracción robusta similar
    a la versión de Azure Function: intenta extraer una celda objetivo y
    hace fallback sumando columnas numéricas.
    """
    tiv_total = 0.0
    df_tiv = pd.DataFrame()

    name = filename.lower() if filename else ""
    try:
        if name.endswith('.csv'):
            # Intentar leer como CSV con detección de encoding
            try:
                content_str = file_content.decode('utf-8')
                detected_encoding = 'utf-8'
            except UnicodeDecodeError:
                content_str = file_content.decode('latin-1', errors='ignore')
                detected_encoding = 'latin-1'
            lines = content_str.splitlines()

            # Buscar encabezado por 'Bienes Asegurables'
            header_row_index = -1
            for i, line in enumerate(lines):
                if 'Bienes Asegurables' in line:
                    header_row_index = i
                    break
            if header_row_index == -1:
                header_row_index = 7

            try:
                df_tiv = pd.read_csv(io.StringIO('\n'.join(lines[header_row_index:])), sep=None, engine='python', encoding=detected_encoding, on_bad_lines='skip')
            except Exception:
                df_tiv = pd.read_csv(io.StringIO('\n'.join(lines[header_row_index:])), engine='python', on_bad_lines='skip')

        else:
            # Intentar leer como Excel
            try:
                # Reutilizar _read_table para leer con header=None
                df_raw = _read_table(file_content, filename)
                # intentar encontrar la hoja/encabezado mediante heurística
                # convertir a DataFrame con header=0 desde fila 6/7 si procede
                if len(df_raw) > 8:
                    df_tiv = pd.read_excel(io.BytesIO(file_content), engine='openpyxl', header=6)
                else:
                    df_tiv = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
            except Exception:
                # Fallback: intentar leer primera hoja con pandas
                df_tiv = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')

        # Normalizar columnas
        try:
            df_tiv.columns = df_tiv.columns.astype(str).str.strip().str.replace('\r','').str.replace('\n','')
        except Exception:
            pass

        # Intentar extraer valor objetivo (etiqueta conocida)
        target_row_label = "Valor Total Asegurado + I.V."
        label_column_index = 1
        value_column_index = 22

        found_row = None
        try:
            if df_tiv.shape[1] > label_column_index:
                found = df_tiv[df_tiv.iloc[:, label_column_index].astype(str).str.strip() == target_row_label]
                if not found.empty:
                    found_row = found.iloc[0]
        except Exception:
            found_row = None

        if found_row is not None:
            try:
                if value_column_index < df_tiv.shape[1]:
                    tiv_total_str = found_row.iloc[value_column_index]
                    cleaned_str = str(tiv_total_str).replace('$','').strip()
                    if ',' in cleaned_str and '.' in cleaned_str:
                        if cleaned_str.rfind('.') > cleaned_str.rfind(','):
                            cleaned_str = cleaned_str.replace(',','')
                        else:
                            cleaned_str = cleaned_str.replace('.','').replace(',','.')
                    else:
                        cleaned_str = cleaned_str.replace(',','.')
                    tiv_total = pd.to_numeric(cleaned_str, errors='coerce')
                    if pd.isna(tiv_total):
                        tiv_total = 0.0
                else:
                    tiv_total = 0.0
            except Exception:
                tiv_total = 0.0

        # Fallback: sumar columnas numéricas
        if tiv_total == 0.0:
            numeric_cols = []
            for col in df_tiv.columns[-15:]:
                try:
                    col_series = df_tiv[col].astype(str).str.replace('$','').str.strip()
                    # limpieza básica
                    col_series = col_series.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    numeric = pd.to_numeric(col_series, errors='coerce')
                    if numeric.notna().sum() > 0:
                        df_tiv[col] = numeric.fillna(0)
                        numeric_cols.append(col)
                except Exception:
                    continue

            if numeric_cols:
                # intentar ignorar subtotales buscando labels
                end_row = len(df_tiv)
                try:
                    label_col = df_tiv.columns[label_column_index]
                    for i, val in enumerate(df_tiv[label_col].astype(str)):
                        if any(k in val for k in ['Subtotales','Índice variable','Valor Total Asegurado']):
                            end_row = i
                            break
                except Exception:
                    end_row = len(df_tiv)

                try:
                    tiv_total = float(df_tiv.iloc[:end_row][numeric_cols].sum().sum())
                except Exception:
                    tiv_total = 0.0

    except Exception as e:
        logger.exception(f"Error procesando TIV bytes: {e}")

    if pd.isna(tiv_total):
        tiv_total = 0.0

    return df_tiv, float(tiv_total)

def calcular_burning_cost(df_siniestros: pd.DataFrame, tiv_total: float) -> float:
    """Calcula el Burning Cost (Incurrido / TIV)."""
    if tiv_total is None or pd.isna(tiv_total) or tiv_total == 0:
        logger.warning(f"TIV Total inválido: {tiv_total}")
        return 0.0
    
    if df_siniestros is None or df_siniestros.empty or 'Incurrido' not in df_siniestros.columns:
        logger.info("No hay siniestros para calcular Burning Cost.")
        return 0.0
    
    total_incurrido = df_siniestros['Incurrido'].sum()
    if pd.isna(total_incurrido):
        return 0.0
    
    burning_cost = total_incurrido / tiv_total if tiv_total != 0 else 0
    logger.info(f"Burning Cost: {burning_cost:.4%} (Incurrido: {total_incurrido}, TIV: {tiv_total})")
    return burning_cost

def generar_resumen_siniestros(df: pd.DataFrame) -> Dict[str, Any]:
    """Genera resumen estadístico de siniestros."""
    if df.empty:
        return {}
    
    resumen = {
        "total_siniestros": len(df),
        "total_incurrido": float(df['Incurrido'].sum()),
        "total_liquidado": float(df['Liquidado'].sum()),
        "total_reserva": float(df['Rva. Actual'].sum()),
    }
    
    # Resumen por año
    if 'año_siniestro' in df.columns:
        por_anio = df.groupby('año_siniestro').agg({
            'Incurrido': 'sum',
            'Liquidado': 'sum',
            'Rva. Actual': 'sum',
            'Num. Poliza': 'count'
        }).to_dict('index')
        
        resumen['por_anio'] = {
            int(year): {
                'incurrido': float(data['Incurrido']),
                'liquidado': float(data['Liquidado']),
                'reserva': float(data['Rva. Actual']),
                'cantidad': int(data['Num. Poliza'])
            }
            for year, data in por_anio.items() if not pd.isna(year)
        }
    
    return resumen

# ----------------------------
# Endpoints
# ----------------------------

@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}

@app.post("/tiv/parse", response_model=TIVResponse)
async def parse_tiv(
    file: UploadFile = File(..., description="Excel TIV (XLSX/XLSM/CSV)")
):
    """
    Parsea un archivo TIV en formato transpuesto o estándar.
    
    - Formato transpuesto: columnas = ubicaciones, filas = tipos de bienes
    - Formato estándar: filas = ubicaciones, columnas = campos
    """
    data = await file.read()
    
    try:
        raw = _read_table(data, file.filename)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, 
                          detail=f"No se pudo leer el archivo: {e}")
    
    # Intentar formato transpuesto primero (más común en pólizas TRDM)
    records = parse_transposed_tiv(raw)
    columnas_detectadas = {}
    
    if records:
        columnas_detectadas = {"formato": "transpuesto (columnas = ubicaciones)"}
    else:
        # Intentar formato estándar
        records = parse_standard_tiv(raw, {})
        
        if records:
            mapping = detect_columns(raw, {})
            columnas_detectadas = {c: mapping[c] for c in mapping}
        else:
            raise HTTPException(
                status_code=422, 
                detail="No se pudo detectar el formato del archivo. Verifica que sea un TIV válido."
            )
    
    if not records:
        raise HTTPException(
            status_code=422, 
            detail="El archivo no contiene filas válidas (id_ubicacion/moneda requeridos)."
        )
    
    return TIVResponse(
        items=records,
        columnas_detectadas=columnas_detectadas,
        resumen=summarize(records)
    )

@app.post("/tiv/to-pricing", response_model=PricingSheets)
async def tiv_to_pricing(
    file: UploadFile = File(..., description="Excel TIV (XLSX/XLSM/CSV)")
):
    """
    Convierte un TIV a formato de pricing con dos hojas:
    - Valores: información económica por ubicación
    - Ubicaciones: información geográfica y de actividad
    """
    # Reutilizar parseo
    resp: TIVResponse = await parse_tiv(file)
    
    valores, ubicaciones = [], []
    
    for it in resp.items:
        valores.append({
            "ID_Ubicacion": it.id_ubicacion,
            "Moneda": it.moneda,
            "Valor_Edificio": it.valor_edificio,
            "Valor_Maquinaria_Contenidos": it.valor_maquinaria_contenidos,
            "Valor_Stock": it.valor_stock,
            "Valor_Mejoras": it.valor_mejoras,
            "Incluye_BI": it.incluye_bi
        })
        
        ubicaciones.append({
            "ID_Ubicacion": it.id_ubicacion,
            "Direccion": it.direccion,
            "Ciudad": it.ciudad,
            "Pais": it.pais,
            "Actividad/Ocupacion": it.actividad
        })
    
    # Deduplicar ubicaciones (por si vienen repetidas)
    df_u = pd.DataFrame(ubicaciones)
    df_u = df_u.drop_duplicates(subset=["ID_Ubicacion"], keep="first")
    
    return PricingSheets(
        Valores=valores, 
        Ubicaciones=df_u.to_dict(orient="records")
    )

@app.post("/siniestralidad/parse")
async def parse_siniestralidad(
    files: List[UploadFile] = File(..., description="Archivos CSV de siniestralidad")
):
    """
    Procesa uno o más archivos de siniestralidad y los consolida.
    Filtra automáticamente por TRDM (Todo Riesgo Daño Material).
    """
    files_data = []
    
    for file in files:
        content = await file.read()
        files_data.append((file.filename, content))
    
    # Consolidar
    df_consolidado = consolidar_siniestralidad(files_data)
    
    if df_consolidado.empty:
        raise HTTPException(
            status_code=422,
            detail="No se pudieron procesar los archivos de siniestralidad."
        )
    
    # Generar resumen
    resumen = generar_resumen_siniestros(df_consolidado)
    
    # Convertir a items
    items = []
    for _, row in df_consolidado.iterrows():
        items.append(SiniestroItem(
            num_poliza=str(row.get('Num. Poliza', '')),
            fecha_siniestro=str(row.get('Fec. Sini', '')) if pd.notna(row.get('Fec. Sini')) else None,
            liquidado=float(row.get('Liquidado', 0)),
            reserva_actual=float(row.get('Rva. Actual', 0)),
            incurrido=float(row.get('Incurrido', 0)),
            producto=str(row.get('Nom. Procucto', '')) if pd.notna(row.get('Nom. Procucto')) else None,
            ramo_tecnico=str(row.get('Ramo Técnico', '')) if pd.notna(row.get('Ramo Técnico')) else None,
            anio_siniestro=int(row.get('año_siniestro')) if pd.notna(row.get('año_siniestro')) else None
        ))
    
    return SiniestrosResponse(
        items=items,
        total_incurrido=resumen.get('total_incurrido', 0),
        total_liquidado=resumen.get('total_liquidado', 0),
        total_reserva=resumen.get('total_reserva', 0),
        resumen_por_anio=resumen.get('por_anio', {})
    )

@app.post("/analisis-tecnico", response_model=AnalisisTecnicoResponse)
async def analisis_tecnico_completo(
    tiv_file: UploadFile = File(..., description="Archivo TIV"),
    siniestros_files: List[UploadFile] = File(default=[], description="Archivos de siniestralidad (opcional)"),
    request_data: str = Body(default='{}', description="Datos adicionales en JSON")
):
    """
    Realiza un análisis técnico completo:
    1. Procesa el TIV
    2. Procesa siniestralidad (si se proporciona)
    3. Calcula Burning Cost
    4. Determina si es negocio nuevo o renovación
    5. Guarda en BD (si está configurada)
    6. Dispara webhook n8n (si está configurado)
    """
    try:
        # Parsear datos adicionales
        try:
            extra_data = json.loads(request_data)
        except:
            extra_data = {}
        
        # 1. Procesar TIV
        logger.info("Procesando archivo TIV...")
        # Leer bytes del archivo para permitir varios pasos de parseo
        tiv_bytes = await tiv_file.read()
        # Intentar parsear TIV en formato transpuesto o estándar
        try:
            raw_df = _read_table(tiv_bytes, tiv_file.filename)
        except Exception as e:
            logger.warning(f"No se pudo leer TIV como tabla: {e}. Intentando procesamiento por bytes.")
            raw_df = None

        records = []
        if raw_df is not None:
            records = parse_transposed_tiv(raw_df)
            if not records:
                records = parse_standard_tiv(raw_df, {})

        # Si no se pudieron obtener records con los parsers, fallback a parse_tiv endpoint
        if not records:
            # reconstruir UploadFile-like leyendo desde bytes: llamar al endpoint parse_tiv
            # Crear un objeto temporal UploadFile no trivial; en su lugar, llamar a procesar_tiv_bytes
            df_tiv_fallback, tiv_total = procesar_tiv_bytes(tiv_bytes, tiv_file.filename)
        else:
            # Calcular TIV total sumando los totales detectados
            tiv_total = 0
            resumen_tot = summarize(records).get('totales', {})
            for key in ['valor_edificio', 'valor_maquinaria_contenidos', 'valor_stock', 'valor_mejoras']:
                tiv_total += resumen_tot.get(key, 0) or 0
            # Además intentar extraer TIV objetivo con procesar_tiv_bytes y usar si mayor a 0
            _, tiv_total_bytes = procesar_tiv_bytes(tiv_bytes, tiv_file.filename)
            if tiv_total_bytes and tiv_total_bytes > 0:
                tiv_total = tiv_total_bytes
        
        # Construir un objeto similar a parse_tiv response para reuse
        tiv_response = type('Tmp', (), {})()
        tiv_response.items = records
        tiv_response.resumen = {'totales': summarize(records).get('totales', {})} if records else {'totales': {}}
        
        # 2. Procesar siniestralidad
        df_siniestros = pd.DataFrame()
        resumen_siniestros = None
        
        if siniestros_files and len(siniestros_files) > 0:
            logger.info(f"Procesando {len(siniestros_files)} archivos de siniestralidad...")
            files_data = []
            for file in siniestros_files:
                content = await file.read()
                files_data.append((file.filename, content))
            
            df_siniestros = consolidar_siniestralidad(files_data)
            if not df_siniestros.empty:
                resumen_siniestros = generar_resumen_siniestros(df_siniestros)
        
        # 3. Determinar tipo de negocio
        tipo_negocio_explicit = extra_data.get('tipo_negocio', '').lower()
        tipo_negocio = tipo_negocio_explicit if tipo_negocio_explicit in ['nuevo', 'renovacion'] else (
            'renovacion' if not df_siniestros.empty else 'nuevo'
        )
        
        logger.info(f"Tipo de negocio detectado: {tipo_negocio}")
        
        # 4. Calcular Burning Cost
        burning_cost = calcular_burning_cost(df_siniestros, tiv_total)
        
        # 5. Preparar resultados
        results = {
            'tipo_negocio': tipo_negocio,
            'tiv_total': tiv_total,
            'burning_cost': burning_cost,
            'num_ubicaciones': len(tiv_response.items),
            'siniestros_procesados': len(df_siniestros),
            'slip_info': extra_data.get('slip_data', {}),
            'resumen_siniestros': resumen_siniestros
        }
        
        # 6. Guardar en BD (si está configurado)
        analysis_id = None
        if POSTGRES_CONN_STR:
            conn = get_db_connection()
            if conn:
                analysis_id = insert_analysis_results(conn, results)
                conn.close()
            else:
                logger.warning("No se pudo conectar a BD, continuando sin guardar.")
        else:
            logger.info("BD no configurada, saltando guardado.")
        
        # 7. Disparar n8n (si está configurado y se guardó en BD)
        n8n_triggered = False
        if analysis_id and N8N_WEBHOOK_URL:
            n8n_triggered = trigger_n8n_webhook(analysis_id)
        
        # 8. Preparar respuesta
        mensaje = f"Análisis técnico '{tipo_negocio}' completado."
        if analysis_id:
            mensaje += f" ID: {analysis_id}."
        if n8n_triggered:
            mensaje += " Flujo n8n iniciado."
        elif N8N_WEBHOOK_URL and analysis_id:
            mensaje += " Advertencia: fallo al iniciar flujo n8n."
        
        return AnalisisTecnicoResponse(
            analysis_id=analysis_id,
            tipo_negocio=tipo_negocio,
            tiv_total=tiv_total,
            burning_cost=burning_cost,
            items_tiv=tiv_response.items,
            siniestros_procesados=len(df_siniestros),
            resumen_siniestros=resumen_siniestros,
            slip_info=extra_data.get('slip_data', {}),
            n8n_triggered=n8n_triggered,
            mensaje=mensaje
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error en análisis técnico: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error interno durante el análisis técnico: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)