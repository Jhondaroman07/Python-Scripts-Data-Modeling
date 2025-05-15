import os
from flask import Flask, render_template, request, send_file
import importlib.util
from werkzeug.utils import secure_filename
import tempfile
import atexit
import shutil
from datetime import datetime
import zipfile
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuración con rutas relativas dentro del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_FOLDER = os.path.join(BASE_DIR, 'static', 'scripts')
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploaded_files')
DOWNLOAD_FOLDER = os.path.join(BASE_DIR, 'processed_files')

# Asegurar que las carpetas existan
os.makedirs(SCRIPT_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Configuración de la aplicación
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB límite
app.config['ALLOWED_EXTENSIONS'] = {'csv'}
app.config['TEMPLATES_AUTO_RELOAD'] = True

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def allowed_script(filename):
    """Valida que los scripts sean seguros"""
    return filename.endswith('.py') and not any(bad in filename for bad in ['..', '/', '\\'])

def get_scripts_list():
    """Obtiene la lista de scripts disponibles"""
    scripts = []
    try:
        for file in os.listdir(SCRIPT_FOLDER):
            if allowed_script(file):
                scripts.append(file[:-3])  # Quitar extensión .py
    except FileNotFoundError:
        logger.error("Carpeta de scripts no encontrada, creando...")
        os.makedirs(SCRIPT_FOLDER, exist_ok=True)
    return scripts

def execute_script(script_name, input_files):
    """Ejecuta script para múltiples archivos y devuelve lista de archivos procesados"""
    script_path = os.path.join(SCRIPT_FOLDER, f"{script_name}.py")
    processed_files = []
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"No se encontró el script: {script_name}.py")
    
    try:
        # Cargar módulo dinámicamente con validación adicional
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        for input_path in input_files:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                original_filename = secure_filename(os.path.basename(input_path))
                output_filename = f"procesado_({timestamp})_{original_filename}"
                output_path = os.path.join(DOWNLOAD_FOLDER, output_filename)
                
                # Configurar variables en el script
                module.INPUT_FOLDER = os.path.dirname(input_path)
                module.OUTPUT_FOLDER = os.path.dirname(output_path)
                
                # Ejecutar función principal con validación
                if hasattr(module, 'procesar_archivos'):
                    logger.info(f"Ejecutando procesar_archivos() en {script_name}")
                    module.procesar_archivos()
                elif hasattr(module, 'main'):
                    logger.info(f"Ejecutando main() en {script_name}")
                    module.main()
                else:
                    raise AttributeError("No se encontró función ejecutable (procesar_archivos o main)")
                
                # Verificar salida
                expected_output = os.path.join(module.OUTPUT_FOLDER, f"limpio_{os.path.basename(input_path)}")
                if not os.path.exists(expected_output):
                    raise FileNotFoundError(f"El script no generó el archivo esperado: {expected_output}")
                if os.path.getsize(expected_output) == 0:
                    raise ValueError("El archivo de salida está vacío")
                
                # Mover archivo procesado
                os.rename(expected_output, output_path)
                processed_files.append(output_path)
                
            except Exception as e:
                logger.error(f"Error procesando {input_path}: {str(e)}")
                continue
                
        return processed_files
    
    except Exception as e:
        # Limpiar archivos en caso de error
        for file in processed_files:
            if os.path.exists(file):
                os.remove(file)
        raise e

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'files[]' not in request.files:
            return render_template('index.html', 
                               error="No se seleccionaron archivos",
                               scripts=get_scripts_list(),
                               selected_script=None)
        
        files = request.files.getlist('files[]')
        script_name = request.form.get('script_name')
        
        if not files or all(file.filename == '' for file in files):
            return render_template('index.html', 
                               error="No se seleccionaron archivos válidos",
                               scripts=get_scripts_list(),
                               selected_script=None)
        
        if not script_name:
            return render_template('index.html', 
                               error="No se seleccionó script",
                               scripts=get_scripts_list(),
                               selected_script=None)
        
        valid_files = []
        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                input_path = os.path.join(UPLOAD_FOLDER, filename)
                file.save(input_path)
                valid_files.append(input_path)
                logger.info(f"Archivo guardado: {input_path}")
        
        if not valid_files:
            return render_template('index.html', 
                               error="Ningún archivo permitido",
                               scripts=get_scripts_list(),
                               selected_script=None)
        
        try:
            output_files = execute_script(script_name, valid_files)
            
            if not output_files:
                return render_template('index.html', 
                                     error="No se procesaron archivos correctamente",
                                     scripts=get_scripts_list(),
                                     selected_script=script_name)
            
            # Crear ZIP
            zip_filename = f"resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = os.path.join(DOWNLOAD_FOLDER, zip_filename)
            
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file in output_files:
                    zipf.write(file, os.path.basename(file))
            
            return render_template('index.html', 
                                 success=f"Se procesaron {len(output_files)} archivos!",
                                 download_file=zip_filename,
                                 scripts=get_scripts_list(),
                                 selected_script=script_name)
                
        except Exception as e:
            logger.error(f"Error al procesar: {str(e)}", exc_info=True)
            return render_template('index.html', 
                                error=f"Error al procesar: {str(e)}",
                                scripts=get_scripts_list(),
                                selected_script=script_name)
        finally:
            # Limpieza
            for file in valid_files:
                if os.path.exists(file):
                    os.remove(file)
            if 'output_files' in locals():
                for file in output_files:
                    if os.path.exists(file):
                        os.remove(file)
    else:
        return render_template('index.html', 
                            scripts=get_scripts_list(),
                            selected_script=None)

@app.route('/download/<filename>')
def download_file(filename):
    file_path = os.path.join(DOWNLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        logger.warning(f"Archivo no encontrado: {filename}")
        return "Archivo no encontrado", 404
    
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype='application/zip' if filename.endswith('.zip') else 'text/csv'
    )
    
    @response.call_on_close
    def remove_file():
        try:
            os.remove(file_path)
            logger.info(f"Archivo eliminado después de descarga: {filename}")
        except Exception as e:
            logger.error(f"Error eliminando archivo: {str(e)}")
    
    return response

def cleanup():
    """Limpieza programada de archivos temporales"""
    try:
        for folder in [UPLOAD_FOLDER, DOWNLOAD_FOLDER]:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    logger.error(f"Error eliminando {file_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Error en cleanup: {str(e)}")

atexit.register(cleanup)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False)