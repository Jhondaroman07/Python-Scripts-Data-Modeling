import os
import pandas as pd
from datetime import datetime
import re

# =============================================
# VARIABLES GLOBALES (configuradas por Flask)
# =============================================
INPUT_FOLDER = '.'  # Será configurado por Flask
OUTPUT_FOLDER = '.'  # Será configurado por Flask

# Diccionario para conversión de meses
MESES = {
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

# =============================================
# FUNCIÓN PRINCIPAL QUE EJECUTARÁ FLASK
# =============================================
def procesar_archivos():
    """Función estándar que Flask ejecutará automáticamente"""
    print("="*50)
    print("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV - RQ")
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    print(f"📂 Carpeta de salida: {OUTPUT_FOLDER}")
    print("="*50 + "\n")
    
    # Procesar todos los archivos CSV en la carpeta de entrada
    for filename in os.listdir(INPUT_FOLDER):
        if filename.endswith('.csv'):
            input_path = os.path.join(INPUT_FOLDER, filename)
            output_path = os.path.join(OUTPUT_FOLDER, f"limpio_{filename}")
            
            print(f"\n📄 Procesando archivo: {filename}")
            try:
                procesar_archivo(input_path, output_path)
            except Exception as e:
                print(f"❌ Error al procesar {filename}: {str(e)}")

    print("\n" + "="*50)
    print("✅ PROCESAMIENTO COMPLETADO - RQ")
    print("="*50)

def procesar_archivo(input_path, output_path):
    # Leer el archivo CSV
    df = pd.read_csv(input_path)
    
    # Procesar columna 1 y 2 (índices 0 y 1) - Reemplazar "," por "."
    for col in [0, 1]:
        if len(df.columns) > col:
            col_name = df.columns[col]
            df.iloc[:, col] = df.iloc[:, col].astype(str).str.replace(',', '.')
            print(f"✅ Columna '{col_name}' - Reemplazadas ',' por '.'")
    
    # Procesar columna 3 (índice 2) - Formato de fecha
    if len(df.columns) > 2:
        col_name = df.columns[2]
        # Mostrar ejemplos antes de la conversión
        ejemplos_antes = df.iloc[:2, 2].astype(str).tolist()
        print(f"🔍 Ejemplo de fechas antes de conversión: {ejemplos_antes}")
        
        # Convertir fechas usando nuestra función mejorada
        df.iloc[:, 2] = df.iloc[:, 2].apply(convertir_fecha)
        
        # Mostrar ejemplos después de la conversión
        ejemplos_despues = df.iloc[:2, 2].astype(str).tolist()
        print(f"📅 Ejemplo de fechas después de conversión: {ejemplos_despues}")
    
    # Resto del procesamiento
    # Procesar columna 4 (índice 3) - Reemplazar "," por "."
    if len(df.columns) > 3:
        col_name = df.columns[3]
        df.iloc[:, 3] = df.iloc[:, 3].astype(str).str.replace(',', '.')
        print(f"✅ Columna '{col_name}' - Reemplazadas ',' por '.'")
    
    # Procesar columna 5 (índice 4) - Formato de hora HH:MM:SS a HH:MM
    if len(df.columns) > 4:
        col_name = df.columns[4]
        try:
            df.iloc[:, 4] = pd.to_datetime(df.iloc[:, 4], format='%H:%M:%S').dt.strftime('%H:%M')
            print(f"⏰ Columna '{col_name}' - Formato cambiado a HH:MM")
        except:
            print(f"⚠️ No se pudo convertir el formato de hora en columna {col_name}, manteniendo original")
    
    # Columnas 6-10 (índices 5-9) - Se mantienen igual
    
    # Procesar columna 11 (índice 10) - Reemplazar "," por "."
    if len(df.columns) > 10:
        col_name = df.columns[10]
        df.iloc[:, 10] = df.iloc[:, 10].astype(str).str.replace(',', '.')
        print(f"✅ Columna '{col_name}' - Reemplazadas ',' por '.'")
    
    # Columna 12 (índice 11) - Se mantiene exactamente igual
    if len(df.columns) > 11:
        col_name = df.columns[11]
        print(f"➡️ Columna '{col_name}' - Se mantiene sin cambios")
    
    # Guardar el archivo procesado
    df.to_csv(output_path, index=False)
    print(f"💾 Guardado como: {os.path.basename(output_path)}")

def convertir_fecha(fecha_str):
    """Convierte una fecha en varios formatos al formato YYYY-MM-DD"""
    if pd.isna(fecha_str):
        return None
    
    fecha_str = str(fecha_str).strip().lower()
    
    # Intenta primero con el parser de pandas
    try:
        fecha = pd.to_datetime(fecha_str, dayfirst=True)
        return fecha.strftime('%Y-%m-%d')
    except:
        pass
    
    # Si falla, intenta con el diccionario de meses
    try:
        # Patrón para fechas como "01 abr 2025" o "01-abr-2025"
        patron = r'(\d{1,2})[\s\-/]?([a-z]+)[\s\-/]?(\d{2,4})'
        match = re.search(patron, fecha_str)
        if match:
            dia = match.group(1).zfill(2)
            mes_abrev = match.group(2)
            mes = MESES.get(mes_abrev)  # Busca en el diccionario
            año = match.group(3)
            
            if mes:  # Si encontramos el mes en el diccionario
                if len(año) == 2:  # Si el año tiene solo 2 dígitos
                    año = f'20{año}'  # Asumimos siglo XXI
                return f"{año}-{mes}-{dia}"
    except Exception as e:
        print(f"⚠️ Error al convertir fecha {fecha_str}: {str(e)}")
    
    # Si todo falla, devuelve la fecha original
    print(f"⚠️ No se pudo convertir la fecha: {fecha_str}")
    return fecha_str

# =============================================
# BLOQUE PARA PRUEBAS LOCALES
# =============================================
if __name__ == "__main__":
    # Configuración para pruebas locales
    INPUT_FOLDER = "entrada_pruebas_rq"
    OUTPUT_FOLDER = "salida_pruebas_rq"
    
    # Crear carpetas si no existen
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Ejecutar el procesamiento
    procesar_archivos()