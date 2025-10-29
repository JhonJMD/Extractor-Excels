from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import io
import re
import unicodedata

app = FastAPI(title="TIV Ingest API", version="1.0.0")

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)