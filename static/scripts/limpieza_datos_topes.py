import os
import pandas as pd
from datetime import datetime

# =============================================
# VARIABLES GLOBALES (configuradas por Flask)
# =============================================
INPUT_FOLDER = '.'  # Será configurado por Flask
OUTPUT_FOLDER = '.'  # Será configurado por Flask

# =============================================
# FUNCIÓN PRINCIPAL QUE EJECUTARÁ FLASK
# =============================================
def procesar_archivos():
    """Función estándar que Flask ejecutará automáticamente"""
    print("="*50)
    print("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV - TOPE")
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    print(f"📂 Carpeta de salida: {OUTPUT_FOLDER}")
    print("="*50 + "\n")
    
    limpiar_horas_tope(INPUT_FOLDER, OUTPUT_FOLDER)
    
    print("\n" + "="*50)
    print("✅ PROCESAMIENTO COMPLETADO - TOPE")
    print("="*50)

# =============================================
# FUNCIÓN ORIGINAL ADAPTADA
# =============================================
def limpiar_horas_tope(input_path, output_path):
    # Definir los nombres de las columnas
    column_titles = [
        'SM',
        'agent_email',
        'LOB',
        'Week',
        'fecha',
        'Inicio_Turno',
        'Salida_Turno',
        'Horario_Rooster',
        'Total_horas'
    ]

    # Cargar todos los archivos CSV en la carpeta de entrada
    archivos = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    
    for archivo in archivos:
        try:
            input_file = os.path.join(input_path, archivo)
            output_file = os.path.join(output_path, f"limpio_{archivo}")
            
            # Leer el archivo CSV ignorando cualquier encabezado existente
            df = pd.read_csv(input_file, header=None)
            
            # Verificar que tenga al menos 9 columnas
            if df.shape[1] < 9:
                print(f"El archivo {archivo} no tiene suficientes columnas. Se omitirá.")
                continue
            
            # Eliminar filas que puedan contener encabezados antiguos
            mask = ~df.apply(lambda row: any(str(cell).strip() in column_titles for cell in row), axis=1)
            df = df[mask].reset_index(drop=True)
            
            # Procesamiento de cada columna según los requisitos
            
            # Columnas 1-3 (índices 0-2): Mantener solo texto
            for col in [0, 1, 2]:
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and isinstance(x, (str, bool, int, float)) else None)
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) and not any(char.isdigit() for char in str(x)) and x not in ['True', 'False', 'TRUE', 'FALSE'] else None)
            
            # Columna 4 (índice 3): Mantener solo números enteros
            df[3] = pd.to_numeric(df[3], errors='coerce').astype('Int64')
            
            # Columna 5 (índice 4): Convertir fechas y cambiar formato
            def parse_date(date_str):
                try:
                    if pd.isna(date_str):
                        return None
                    # Intentar parsear en formato DD/MM/AAAA
                    date_obj = datetime.strptime(str(date_str), '%d/%m/%Y')
                    return date_obj.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    return None
            
            df[4] = df[4].apply(parse_date)
            
            # Columnas 6-7 (índices 5-6): Limpiar datos pero mantener columnas
            for col in [5, 6]:
                df[col] = None
            
            # Columna 8 (índice 7): Mantener solo formato HH:MM - HH:MM
            def validate_time_range(time_str):
                if pd.isna(time_str):
                    return None
                try:
                    time_str = str(time_str).strip()
                    if ' - ' in time_str:
                        start, end = time_str.split(' - ')
                        
                        # Convertir 24:00 a 00:00 en ambas partes del rango
                        start = '00:00' if start == '24:00' else start
                        end = '00:00' if end == '24:00' else end
                        
                        # Validar formato HH:MM para ambas partes
                        datetime.strptime(start, '%H:%M')
                        datetime.strptime(end, '%H:%M')
                        
                        return f"{start} - {end}"
                    return None
                except (ValueError, TypeError):
                    return None
            
            df[7] = df[7].apply(validate_time_range)
            
            # Columna 9 (índice 8): Mantener números, reemplazar , por .
            def clean_numeric(value):
                if pd.isna(value):
                    return None
                try:
                    if isinstance(value, str):
                        value = value.replace(',', '.')
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            df[8] = df[8].apply(clean_numeric)
            
            # Eliminar columnas adicionales si existen (después de la 9)
            if df.shape[1] > 9:
                df = df.iloc[:, :9]
            
            # Asignar los nombres de las columnas
            df.columns = column_titles[:df.shape[1]]
            
            # Guardar el archivo procesado
            df.to_csv(output_file, index=False)
            print(f"Archivo {archivo} procesado y guardado como {output_file}")
            
        except Exception as e:
            print(f"Error al procesar el archivo {archivo}: {str(e)}")

# =============================================
# BLOQUE PARA PRUEBAS LOCALES
# =============================================
if __name__ == '__main__':
    # Configuración para pruebas locales
    INPUT_FOLDER = "entrada_pruebas_tope"
    OUTPUT_FOLDER = "salida_pruebas_tope"
    
    # Crear carpetas si no existen
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Ejecutar el procesamiento
    procesar_archivos()