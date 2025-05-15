import os
import csv
import re
from datetime import datetime

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
    print("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV - METRICS NEWSCHEME")
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    print(f"📂 Carpeta de salida: {OUTPUT_FOLDER}")
    print("="*50 + "\n")
    
    # Procesar todos los archivos CSV en la carpeta de entrada
    archivos_procesados = 0
    
    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith('.csv'):
            input_path = os.path.join(INPUT_FOLDER, filename)
            output_path = os.path.join(OUTPUT_FOLDER, f"limpio_{filename}")
            
            try:
                limpiar_archivo(input_path, output_path)
                archivos_procesados += 1
            except Exception as e:
                print(f"Error procesando archivo {filename}: {str(e)}")
    
    print("\n" + "="*50)
    print(f"✅ PROCESAMIENTO COMPLETADO - {archivos_procesados} ARCHIVOS")
    print("="*50)

def limpiar_archivo(input_path, output_path):
    """Procesa un archivo CSV según los requerimientos"""
    print(f"\nProcesando archivo: {os.path.basename(input_path)}")
    
    with open(input_path, mode='r', encoding='utf-8', newline='') as infile, \
         open(output_path, mode='w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Leer encabezados y verificar columnas
        headers = next(reader)
        total_columnas = len(headers)
        
        if total_columnas > 40:
            print(f"  Advertencia: El archivo tiene {total_columnas} columnas. Se conservarán solo las primeras 40.")
            headers = headers[:40]
        
        writer.writerow(headers)
        filas_procesadas = 0
        filas_con_errores = 0
        
        for row in reader:
            try:
                # Asegurar que la fila tenga exactamente 40 columnas
                if len(row) > 40:
                    row = row[:40]
                elif len(row) < 40:
                    row.extend([''] * (40 - len(row)))
                
                # Procesar primera columna (fecha)
                if row:
                    row[0] = convertir_fecha(row[0])
                
                # Eliminar "null" en todas las columnas
                row = ['' if str(cell).strip().lower() == 'null' else cell for cell in row]
                
                # Limpiar columna 20 (índice 19)
                if len(row) > 19:
                    row[19] = ''
                
                # Procesar columna 35 (índice 34)
                if len(row) > 34:
                    row[34] = formatear_columna_35(row[34])
                
                writer.writerow(row)
                filas_procesadas += 1
            except Exception as e:
                filas_con_errores += 1
                print(f"  Error en fila {filas_procesadas + filas_con_errores + 1}: {str(e)}")
                continue
    
    print(f"  Procesamiento completado: {filas_procesadas} filas procesadas")
    if filas_con_errores > 0:
        print(f"  Advertencia: {filas_con_errores} filas tuvieron errores y fueron omitidas")

def convertir_fecha(fecha_original):
    """Convierte la fecha de DD-MM-YYYY a YYYY-MM-DD"""
    try:
        # Intentar parsear fecha en formato DD-MM-YYYY
        partes = re.split(r'[-/\s]', fecha_original.strip())
        if len(partes) == 3:
            dia, mes, anio = partes
            
            # Convertir mes textual a numérico si es necesario
            mes = mes.lower()
            if mes in MESES:
                mes = MESES[mes]
            
            # Validar y formatear
            dia = dia.zfill(2)
            mes = mes.zfill(2)
            
            # Verificar si el año tiene 2 dígitos y convertirlo a 4
            if len(anio) == 2:
                anio = f"20{anio}" if int(anio) < 50 else f"19{anio}"
            
            # Validar que la fecha sea correcta
            datetime.strptime(f"{anio}-{mes}-{dia}", "%Y-%m-%d")
            return f"{anio}-{mes}-{dia}"
    except (ValueError, AttributeError, KeyError):
        pass
    
    # Si no se puede convertir, devolver la original
    return fecha_original

def formatear_columna_35(valor):
    """Formatea la columna 35 para BigQuery:
    1. Reemplaza comas por puntos
    2. Limita a 2 decimales
    3. Convierte a entero si no tiene decimales
    """
    if not isinstance(valor, str):
        return valor
    
    valor = valor.strip()
    
    # Reemplazar comas por puntos
    valor = valor.replace(',', '.')
    
    # Verificar si es un número decimal
    try:
        # Intentar convertir a float
        num = float(valor)
        
        # Redondear a 2 decimales
        num_redondeado = round(num, 2)
        
        # Convertir a entero si no tiene parte decimal
        if num_redondeado.is_integer():
            return str(int(num_redondeado))
        
        # Formatear a 2 decimales (eliminando ceros innecesarios)
        return "{0:.2f}".format(num_redondeado).rstrip('0').rstrip('.') if "{0:.2f}".format(num_redondeado).endswith('0') else "{0:.2f}".format(num_redondeado)
    except ValueError:
        # Si no es un número, devolver el valor original
        return valor

# =============================================
# BLOQUE PARA PRUEBAS LOCALES
# =============================================
if __name__ == "__main__":
    # Configuración para pruebas locales
    INPUT_FOLDER = "entrada_pruebas_metrics"
    OUTPUT_FOLDER = "salida_pruebas_metrics"
    
    # Crear carpetas si no existen
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Ejecutar el procesamiento
    procesar_archivos()