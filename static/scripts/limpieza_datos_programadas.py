import pandas as pd
import os
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
    print("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV")
    print(f"📂 Carpeta de entrada: {INPUT_FOLDER}")
    print(f"📂 Carpeta de salida: {OUTPUT_FOLDER}")
    print("="*50 + "\n")
    
    limpiar_horas_programadas(INPUT_FOLDER, OUTPUT_FOLDER)
    
    print("\n" + "="*50)
    print("✅ PROCESAMIENTO COMPLETADO")
    print("="*50)

# =============================================
# FUNCIÓN ORIGINAL COMPLETA (sin cambios en la lógica)
# =============================================
def limpiar_horas_programadas(input_path, output_path):
    # Cargar todos los archivos CSV en la carpeta de entrada
    archivos = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    
    for archivo in archivos:
        try:
            # Leer el archivo CSV
            df = pd.read_csv(os.path.join(input_path, archivo))
            
            # Eliminar columnas adicionales si existen (más de 27 columnas)
            if len(df.columns) > 27:
                columnas_a_eliminar = df.columns[27:]
                df = df.drop(columns=columnas_a_eliminar)
                print(f"Advertencia: Se eliminaron {len(columnas_a_eliminar)} columnas adicionales en {archivo}")
            
            # Verificar que el DataFrame tenga exactamente 27 columnas
            if len(df.columns) != 27:
                print(f"El archivo {archivo} no tiene 27 columnas. Tiene {len(df.columns)}. Se omitirá.")
                continue
            
            # Asignar nombres a las columnas según lo especificado
            column_names = [
                "SM", "agent_email", "CapCasos", "LOB", "Week", "fecha", "Inicio_Turno", 
                "Salida_Turno", "Horario_Roster", "Inicio_Break", "Fin_Break", "Condicion_break", 
                "Asistencia", "Estado", "Novedades", "Observaciones", "Presenta_soporte", 
                "Ausencia_Cubierta", "Observaciones_ausencia", "Tipo_Gestion", "BPO", 
                "Experiencia_CRM", "Total_horas", "Inicio_Break_Prog", "Fin_Break_Prog", 
                "Tiempo_Break", "Segundo_Break"
            ]
            
            # Asignar los nombres de columnas
            df.columns = column_names
            
            # Eliminar filas donde agent_email esté vacío
            df = df.dropna(subset=['agent_email'], how='any')
            
            # Limpieza columna por columna según las especificaciones
            
            # Columnas 1-3: Mantener solo texto
            for col in ["SM", "agent_email", "CapCasos"]:
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else None)
            
            # Columna 4 (LOB): Mantener solo texto, eliminar fechas, emails, números
            def es_texto_valido(x):
                if not isinstance(x, str):
                    return False
                # Verificar que no sea fecha, email o número
                try:
                    datetime.strptime(x, "%d/%m/%Y")
                    return False
                except:
                    pass
                try:
                    datetime.strptime(x, "%Y-%m-%d")
                    return False
                except:
                    pass
                if "@" in x and "." in x:  # Simple check for email
                    return False
                try:
                    float(x)
                    return False
                except:
                    pass
                return True
            
            df["LOB"] = df["LOB"].apply(lambda x: x if es_texto_valido(x) else None)
            
            # Columna 5 (Week): Mantener solo enteros (modificado para quitar .0)
            df["Week"] = pd.to_numeric(df["Week"], errors='coerce')
            df["Week"] = df["Week"].dropna().astype('Int64')  # Usar Int64 que permite NaN
            
            # Columna 6 (fecha): Formato DD/MM/AAAA a AAAA-MM-DD (formato SQL)
            def convertir_fecha(x):
                try:
                    if isinstance(x, str):
                        # Primero intentamos convertir desde el formato original DD/MM/AAAA
                        try:
                            fecha_obj = datetime.strptime(x, "%d/%m/%Y")
                            return fecha_obj.strftime("%Y-%m-%d")
                        except:
                            # Si ya está en formato AAAA/MM/DD, lo convertimos
                            if "/" in x and len(x.split("/")[0]) == 4:
                                fecha_obj = datetime.strptime(x, "%Y/%m/%d")
                                return fecha_obj.strftime("%Y-%m-%d")
                            return None
                    elif isinstance(x, datetime):
                        return x.strftime("%Y-%m-%d")
                    return None
                except:
                    return None
            
            df["fecha"] = df["fecha"].apply(convertir_fecha)
            
            # Columnas 7-8 (Inicio_Turno, Salida_Turno): Eliminar datos pero mantener columnas
            df["Inicio_Turno"] = None
            df["Salida_Turno"] = None
            
            # Columna 9 (Horario_Roster): Formato HH:MM - HH:MM, ajustar 24:00 a 00:00
            def validar_horario(x):
                if not isinstance(x, str):
                    return None
                partes = x.split(" - ")
                if len(partes) != 2:
                    return None
                
                def ajustar_hora(hora):
                    if hora == "24:00":
                        return "00:00"
                    try:
                        datetime.strptime(hora, "%H:%M")
                        return hora
                    except:
                        return None
                
                inicio = ajustar_hora(partes[0])
                fin = ajustar_hora(partes[1])
                
                if inicio and fin:
                    return f"{inicio} - {fin}"
                return None
            
            df["Horario_Roster"] = df["Horario_Roster"].apply(validar_horario)
            
            # Columnas 10-11 (Inicio_Break, Fin_Break): Eliminar datos pero mantener columnas
            df["Inicio_Break"] = None
            df["Fin_Break"] = None
            
            # Columna 12 (Condicion_break): Mantener solo texto
            df["Condicion_break"] = df["Condicion_break"].apply(lambda x: x if isinstance(x, str) else None)
            
            # Columna 13 (Asistencia): Mantener solo booleanos
            def validar_booleano(x):
                if isinstance(x, bool):
                    return x
                if isinstance(x, str):
                    x = x.upper().strip()
                    if x in ["TRUE", "VERDADERO", "1", "SI"]:
                        return True
                    if x in ["FALSE", "FALSO", "0", "NO"]:
                        return False
                return None
            
            df["Asistencia"] = df["Asistencia"].apply(validar_booleano)
            
            # Columnas 14-21: Mantener solo texto
            for col in ["Estado", "Novedades", "Observaciones", "Presenta_soporte", 
                       "Ausencia_Cubierta", "Observaciones_ausencia", "Tipo_Gestion", "BPO"]:
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else None)
            
            # Reemplazar comas por espacios en columnas de texto críticas
            df["Observaciones"] = df["Observaciones"].str.replace(',', ' ', regex=False)
            df["Observaciones_ausencia"] = df["Observaciones_ausencia"].str.replace(',', ' ', regex=False)
            
            # Columna 22 (Experiencia_CRM): Eliminar datos pero mantener columna
            df["Experiencia_CRM"] = None
            
            # Columna 23 (Total_horas): Reemplazar "," por ".", mantener solo numéricos
            def limpiar_numerico(x):
                if pd.isna(x):
                    return None
                if isinstance(x, str):
                    x = x.replace(",", ".")
                    try:
                        return float(x)
                    except:
                        return None
                try:
                    return float(x)
                except:
                    return None
            
            df["Total_horas"] = df["Total_horas"].apply(limpiar_numerico)
            
            # Columnas 24-26: Eliminar datos pero mantener columnas
            for col in ["Inicio_Break_Prog", "Fin_Break_Prog", "Tiempo_Break"]:
                df[col] = None
            
            # Columna 27 (Segundo_Break): Si está vacío, copiar de Asistencia, mantener solo booleanos
            df["Segundo_Break"] = df["Segundo_Break"].apply(validar_booleano)
            mask = df["Segundo_Break"].isna() & df["Asistencia"].notna()
            df.loc[mask, "Segundo_Break"] = df.loc[mask, "Asistencia"]
            
            # Eliminar columna "CapCasos" (columna 3) al final del proceso
            df = df.drop(columns=["CapCasos"])
            
            # Guardar el archivo limpio
            output_file = os.path.join(output_path, f"limpio_{archivo}")
            df.to_csv(output_file, index=False)
            print(f"Archivo {archivo} procesado y guardado como {output_file}")
            
        except Exception as e:
            print(f"Error al procesar el archivo {archivo}: {str(e)}")

# =============================================
# BLOQUE PARA PRUEBAS LOCALES
# =============================================
if __name__ == '__main__':
    # Configuración para pruebas locales
    INPUT_FOLDER = "entrada_pruebas"
    OUTPUT_FOLDER = "salida_pruebas"
    
    # Crear carpetas si no existen
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Ejecutar el procesamiento
    procesar_archivos()