import os
import tempfile
import dropbox
from dropbox.exceptions import ApiError, AuthError
import pdfplumber
import pandas as pd
import logging
import sys
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Leer token desde variables de entorno
DROPBOX_TOKEN = os.environ.get('DROPBOX_TOKEN')

# Validar que el token exista
if not DROPBOX_TOKEN:
    logging.error("DROPBOX_TOKEN no está configurado. Verificar variables de entorno.")
    sys.exit(1)

# Carpeta en Dropbox que quieres monitorear
DROPBOX_FOLDER = '/PDFs_a_Convertir'

# Lista para mantener un registro de archivos ya procesados
processed_files = set()

def make_unique_columns(df):
    """
    Garantiza que los nombres de las columnas sean únicos.
    Si se encuentran duplicados, se les añade un sufijo numérico.
    """
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        indices = cols[cols == dup].index.tolist()
        for i, idx in enumerate(indices):
            if i == 0:
                continue  # Se deja la primera ocurrencia intacta
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

def procesar_pdf(pdf_path, excel_path):
    """
    Procesa un archivo PDF y lo convierte a Excel.
    """
    try:
        logging.info(f"Procesando el archivo: {pdf_path}")
        dataframes = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for i in range(0, len(pdf.pages), 2):
                # Procesar página izquierda
                tabla_left = pdf.pages[i].extract_table()
                if tabla_left:
                    df_left = pd.DataFrame(tabla_left[1:], columns=tabla_left[0])
                    # Eliminar columnas vacías
                    df_left = df_left.loc[:, df_left.columns != '']
                    df_left = make_unique_columns(df_left)
                
                # Procesar página derecha si existe
                if i + 1 < len(pdf.pages):
                    tabla_right = pdf.pages[i+1].extract_table()
                    if tabla_right:
                        df_right = pd.DataFrame(tabla_right[1:], columns=tabla_right[0])
                        # Eliminar columnas vacías y duplicadas
                        df_right = df_right.loc[:, df_right.columns != '']
                        if 'Nombre' in df_right.columns:
                            df_right = df_right.drop(columns=['Nombre'])
                        if 'Nº' in df_right.columns:
                            df_right = df_right.drop(columns=['Nº'])
                        df_right = make_unique_columns(df_right)
                        
                        # Combinar las páginas
                        combinado = pd.concat([df_left, df_right], axis=1)
                    else:
                        combinado = df_left
                else:
                    combinado = df_left
                
                dataframes.append(combinado)
        
        # Combinar todos los pares de páginas
        resultado_final = pd.concat(dataframes, ignore_index=True)
        resultado_final = resultado_final.replace('', pd.NA).dropna(how='all')
        
        # Exportar a Excel
        resultado_final.to_excel(excel_path, index=False)
        logging.info(f"✓ Archivo Excel guardado exitosamente como: {excel_path}")
        logging.info(f"Total de filas procesadas: {len(resultado_final)}")
        return True
        
    except Exception as e:
        logging.error(f"Error al procesar PDF: {str(e)}")
        return False

def get_dropbox_client():
    """
    Inicializa y devuelve un cliente de Dropbox.
    """
    try:
        logging.info(f"Token recibido (primeros 10 caracteres): {DROPBOX_TOKEN[:10]}")
        
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        # Verificar que el token es válido
        account = dbx.users_get_current_account()
        logging.info(f"Conectado como: {account.name.display_name}")
        return dbx
    except AuthError as e:
        logging.error(f"Error de autenticación detallado: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado al conectar con Dropbox: {str(e)}")
        return None

def check_for_new_pdfs(dbx):
    """
    Revisa si hay nuevos archivos PDF en la carpeta de Dropbox.
    """
    try:
        result = dbx.files_list_folder(DROPBOX_FOLDER)
        
        new_files = []
        for entry in result.entries:
            # Solo procesar archivos PDF que no hayan sido procesados antes
            if (isinstance(entry, dropbox.files.FileMetadata) and 
                entry.path_lower.endswith('.pdf') and 
                entry.path_lower not in processed_files):
                new_files.append(entry)
                
        return new_files
    except ApiError as e:
        logging.error(f"Error al listar archivos: {str(e)}")
        return []

def process_pdf_file(dbx, file_metadata):
    """
    Procesa un archivo PDF de Dropbox y sube el resultado Excel.
    """
    try:
        file_path = file_metadata.path_lower
        file_name = os.path.basename(file_path)
        excel_name = os.path.splitext(file_name)[0] + '.xlsx'
        
        # Crear directorios temporales para los archivos
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_local_path = os.path.join(temp_dir, file_name)
            excel_local_path = os.path.join(temp_dir, excel_name)
            
            # Descargar el archivo PDF
            logging.info(f"Descargando {file_path}...")
            dbx.files_download_to_file(pdf_local_path, file_path)
            
            # Procesar el PDF a Excel
            if procesar_pdf(pdf_local_path, excel_local_path):
                # Subir el archivo Excel resultante
                excel_dropbox_path = os.path.join(os.path.dirname(file_path), excel_name)
                
                logging.info(f"Subiendo resultado a {excel_dropbox_path}...")
                with open(excel_local_path, 'rb') as f:
                    dbx.files_upload(
                        f.read(), 
                        excel_dropbox_path,
                        mode=dropbox.files.WriteMode.overwrite
                    )
                
                # Marcar archivo como procesado
                processed_files.add(file_path)
                logging.info(f"Archivo {file_name} procesado exitosamente.")
                return True
            else:
                logging.error(f"No se pudo procesar {file_name}.")
                return False
                
    except Exception as e:
        logging.error(f"Error al procesar archivo {file_metadata.name}: {str(e)}")
        return False

def main():
    """
    Función principal para procesar archivos PDF en Dropbox.
    """
    logging.info("Iniciando servicio de conversión PDF a Excel...")
    
    try:
        # Intentar conectar con Dropbox
        dbx = get_dropbox_client()
        if not dbx:
            logging.error("No se pudo conectar con Dropbox.")
            sys.exit(1)
        
        # Buscar nuevos PDFs
        logging.info(f"Revisando nuevos PDFs en {DROPBOX_FOLDER}...")
        new_files = check_for_new_pdfs(dbx)
        
        # Procesar archivos encontrados
        if new_files:
            logging.info(f"Se encontraron {len(new_files)} nuevos PDFs para procesar.")
            for file_metadata in new_files:
                process_pdf_file(dbx, file_metadata)
        else:
            logging.info("No se encontraron nuevos PDFs.")
    
    except Exception as e:
        logging.error(f"Error en la ejecución: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
