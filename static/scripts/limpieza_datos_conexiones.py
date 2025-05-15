import pandas as pd
from datetime import datetime
import os
import locale
import re

# =============================================
# VARIABLES GLOBALES (configuradas por Flask)
# =============================================
INPUT_FOLDER = '.'  # Será configurado por Flask
OUTPUT_FOLDER = '.'  # Será configurado por Flask

# =============================================
# DICCIONARIO COMPLETO DE CAMBIOS PARA LA COLUMNA LOB
# =============================================
CAMBIOS_LOB = {
    "CS Customer Account": "CS_Customer Account",
    "CS Fraude": "CS_Fraude",
    "CS Legales": "CS_Legales",
    "CS Local Cerrado": "CS_Local Cerrado",
    "CS Overnight": "CS_Overnight",
    "CS QCommerce&RiderIssues": "CS_QCommerce&RiderIssues",
    "CS Recovery Team": "CS_Recovery",
    "CS Social Media": "CS_Social Media",
    "Customer Live": "CS_Live Chat",
    "CS PDI": "CS_PDI",
    "PS B2X": "PS_B2X",
    "PS Billing Supp": "PS_Billing Support",
    "PS Catalogo": "PS_Catalogo",
    "PS Content Offline": "PS_Content Offline",
    "PS Content Online": "PS_Content Online",
    "PS Live Chat": "PS_Live Chat",
    "PS Onboarding Altas": "PS_Onboarding Altas",
    "PS Onboarding Quality Check": "PS_Onboarding Quality Check",
    "PS Phone": "PS_Phone",
    "PS PICS Edicion": "PS_PICS Edicion",
    "PS PICS Moderacion": "PS_PICS Moderacion",
    "PS QA Content": "PS_QA Content",
    "PS QA Onboarding": "PS_QA Onboarding",
    "PS QA Pics": "PS_QA Pics",
    "PS QA SalesSupport": "PS_QA SalesSupport",
    "PS SalesSupport": "PS_SalesSupport",
    "PS Shopper Support": "PS_Shopper Support",
    "Recupero POS Norte": "PI04_Recupero POS Norte",
    "PS Recupero POS CL": "PI03_Recupero POS CL",
    "RS Boost Channel": "RS_Boost Channel",
    "RS DS Accionables": "RS_DS Accionables",
    "RS Live Chat": "RS_Live Chat",
    "RS Offshift": "RS_Offshift",
    "RS Onboarding": "RS_Onboarding",
    "Recupero POS CL": "PS_Recupero POS CL",
    "PS Onboarding": "PS_Onboarding",
    "RS Overnight": "RS_Overnight",
    "CS Invoice Missing": "CS_Invoice Missing",
    "CS Across Journey": "CS_Across Journey",
    "CS Across Journey Offline": "CS_Across Journey Offline",
    "CS PDI Offline": "CS_PDI Offline",
    "PS CDD y RS": "PS_CDD y RS",
    "PS Curación": "PS_Curacion"
}

# =============================================
# FUNCIÓN PRINCIPAL QUE EJECUTARÁ FLASK
# =============================================
def procesar_archivos():
    """Función estándar que Flask ejecutará automáticamente"""
    print("="*50)
    print("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV - CONEXIONES")
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    print(f"📂 Carpeta de salida: {OUTPUT_FOLDER}")
    print("="*50 + "\n")
    
    # Configuración regional para español
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except:
        try:
            locale.setlocale(locale.LC_TIME, 'spanish')
        except:
            print("⚠️ Advertencia: No se pudo configurar el locale en español. Se usará un método alternativo.")

    # Procesar todos los archivos CSV
    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith('.csv'):
            input_path = os.path.join(INPUT_FOLDER, filename)
            output_path = os.path.join(OUTPUT_FOLDER, f"limpio_{filename}")
            
            print(f"\n📄 Procesando archivo: {filename}")
            
            try:
                df = pd.read_csv(input_path)
                
                if len(df.columns) < 11:
                    print(f"❌ El archivo no tiene 11 columnas. Saltando...")
                    continue
                
                # Aplicar cambios a la columna LOB
                columna_lob = df.columns[8]
                df[columna_lob] = df[columna_lob].replace(CAMBIOS_LOB)
                print(f"✅ Columna '{columna_lob}' actualizada según diccionario")
                
                # Convertir columna FECHA
                columna_fecha = df.columns[10]
                print(f"🔍 Ejemplo de fechas antes de conversión: {df[columna_fecha].head(2).values}")
                
                df[columna_fecha] = df[columna_fecha].astype(str).apply(convertir_fecha)
                print(f"📅 Ejemplo de fechas después de conversión: {df[columna_fecha].head(2).values}")
                
                # Renombrar columnas
                nuevos_nombres = {
                    df.columns[0]: "status_start_time",
                    df.columns[1]: "status_end_time",
                    df.columns[2]: "agent_email",
                    df.columns[3]: "agent_status",
                    df.columns[4]: "interval_start_at",
                    df.columns[5]: "duration_hrs",
                    df.columns[6]: "bpo",
                    df.columns[7]: "Service",
                    df.columns[8]: "lob",
                    df.columns[9]: "ID_LOB",
                    df.columns[10]: "fecha"
                }
                df = df.rename(columns=nuevos_nombres)
                
                # Guardar archivo procesado
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"💾 Guardado como: limpio_{filename}")
                
            except Exception as e:
                print(f"❌ Error procesando el archivo: {e}")

    print("\n" + "="*50)
    print("✅ PROCESAMIENTO COMPLETADO - CONEXIONES")
    print("="*50)

def convertir_fecha(fecha_str):
    """Convierte fechas en español al formato YYYY-MM-DD"""
    try:
        fecha_str = str(fecha_str).strip().strip("'").strip()
        
        meses_es = {
            'ene': '01', 'enero': '01',
            'feb': '02', 'febrero': '02',
            'mar': '03', 'marzo': '03',
            'abr': '04', 'abril': '04',
            'may': '05', 'mayo': '05',
            'jun': '06', 'junio': '06',
            'jul': '07', 'julio': '07',
            'ago': '08', 'agosto': '08',
            'sep': '09', 'septiembre': '09',
            'oct': '10', 'octubre': '10',
            'nov': '11', 'noviembre': '11',
            'dic': '12', 'diciembre': '12'
        }

        patron = r'(\d{1,2})\s+([a-zA-Z]{3,9})\s+(\d{4})'
        match = re.match(patron, fecha_str, re.IGNORECASE)
        
        if match:
            dia = match.group(1).zfill(2)
            mes_abrev = match.group(2).lower()[:3]
            anio = match.group(3)
            mes_num = meses_es.get(mes_abrev, '01')
            return f"{anio}-{mes_num}-{dia}"
        
        formatos = [
            '%d %b %Y', '%d %B %Y', '%d/%m/%Y', 
            '%d-%m-%Y', '%Y-%m-%d'
        ]
        
        for fmt in formatos:
            try:
                dt = datetime.strptime(fecha_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except:
                continue
        
        return fecha_str
    
    except Exception as e:
        print(f"⚠️ Error al convertir fecha '{fecha_str}': {e}")
        return fecha_str

# =============================================
# BLOQUE PARA PRUEBAS LOCALES
# =============================================
if __name__ == "__main__":
    # Configuración para pruebas locales
    INPUT_FOLDER = "entrada_pruebas_conexiones"
    OUTPUT_FOLDER = "salida_pruebas_conexiones"
    
    # Crear carpetas si no existen
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Ejecutar el procesamiento
    procesar_archivos()