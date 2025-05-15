import os
from flask import Flask, render_template, request, send_file
import importlib.util
from werkzeug.utils import secure_filename
import tempfile
import atexit
import shutil
from datetime import datetime
import zipfile

app = Flask(__name__)

# Configuración con rutas absolutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_FOLDER = os.path.join(BASE_DIR, 'static', 'scripts')

# Usar directorios temporales
UPLOAD_FOLDER = tempfile.mkdtemp(prefix='upload_')
DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix='download_')

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 10  # 160MB límite
app.config['ALLOWED_EXTENSIONS'] = {'csv'}

# Crear carpeta de scripts si no existe
os.makedirs(SCRIPT_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def get_scripts_list():
    """Obtiene la lista de scripts disponibles"""
    scripts = []
    try:
        for file in os.listdir(SCRIPT_FOLDER):
            if file.endswith('.py'):
                scripts.append(file[:-3])  # Quitar extensión .py
    except FileNotFoundError:
        os.makedirs(SCRIPT_FOLDER, exist_ok=True)
    return scripts

def execute_script(script_name, input_files):
    """Ejecuta script para múltiples archivos y devuelve lista de archivos procesados"""
    script_path = os.path.join(SCRIPT_FOLDER, f"{script_name}.py")
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"No se encontró el script: {script_name}.py")
    
    processed_files = []
    
    try:
        # Cargar módulo dinámicamente
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        for input_path in input_files:
            try:
                # Crear nombre de archivo con fecha y hora legible
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                original_filename = secure_filename(os.path.basename(input_path))
                output_filename = f"procesado_({timestamp})_{original_filename}"
                output_path = os.path.join(DOWNLOAD_FOLDER, output_filename)
                
                # Configurar variables en el script
                module.INPUT_FOLDER = os.path.dirname(input_path)
                module.OUTPUT_FOLDER = os.path.dirname(output_path)
                
                # Ejecutar función principal
                if hasattr(module, 'procesar_archivos'):
                    module.procesar_archivos()
                elif hasattr(module, 'main'):
                    module.main()
                else:
                    raise Exception("No se encontró función ejecutable")
                
                # Verificar que el archivo de salida existe
                expected_output = os.path.join(module.OUTPUT_FOLDER, f"limpio_{os.path.basename(input_path)}")
                if not os.path.exists(expected_output):
                    raise Exception(f"El script no generó el archivo esperado: {expected_output}")
                if os.path.getsize(expected_output) == 0:
                    raise Exception("El archivo de salida está vacío")
                
                # Renombrar el archivo al formato deseado
                final_output = os.path.join(DOWNLOAD_FOLDER, output_filename)
                os.rename(expected_output, final_output)
                
                processed_files.append(final_output)
                
            except Exception as e:
                print(f"Error procesando {input_path}: {str(e)}")
                continue
                
        return processed_files
    
    except Exception as e:
        # Limpiar archivos de salida si hay error
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
                                error="No se seleccionaron archivos",
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
        
        if not valid_files:
            return render_template('index.html', 
                                error="Ningún archivo permitido",
                                scripts=get_scripts_list(),
                                selected_script=None)
        
        try:
            # Ejecutar script y obtener rutas de resultados
            output_files = execute_script(script_name, valid_files)
            
            if not output_files:
                return render_template('index.html', 
                                    error="No se procesaron archivos correctamente",
                                    scripts=get_scripts_list(),
                                    selected_script=script_name)
            
            # Crear archivo ZIP con los resultados
            zip_filename = f"resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = os.path.join(DOWNLOAD_FOLDER, zip_filename)
            
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file in output_files:
                    zipf.write(file, os.path.basename(file))
            
            return render_template('index.html', 
                                success=f"Se procesaron {len(output_files)} archivos correctamente!",
                                download_file=zip_filename,
                                scripts=get_scripts_list(),
                                selected_script=script_name)
                
        except Exception as e:
            return render_template('index.html', 
                                error=f"Error al procesar: {str(e)}",
                                scripts=get_scripts_list(),
                                selected_script=script_name)
        finally:
            # Limpieza de archivos temporales
            for file in valid_files:
                if os.path.exists(file):
                    os.remove(file)
            for file in output_files if 'output_files' in locals() else []:
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
        return "Archivo no encontrado", 404
    
    # Configurar para descarga directa
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype='application/zip' if filename.endswith('.zip') else 'text/csv'
    )
    
    # Eliminar archivo después de descargar
    @response.call_on_close
    def remove_file():
        try:
            os.remove(file_path)
        except:
            pass
    
    return response

def cleanup():
    """Limpieza de archivos temporales"""
    try:
        shutil.rmtree(UPLOAD_FOLDER, ignore_errors=True)
        shutil.rmtree(DOWNLOAD_FOLDER, ignore_errors=True)
    except:
        pass

atexit.register(cleanup)

if __name__ == '__main__':
    app.run(debug=True)