import streamlit as st
from zipfile import ZipFile
import os
import json
import shutil
from PIL import Image
import re
import pandas as pd
import io
import base64
import time

from st_aggrid import AgGrid
# from streamlit import cache_data # Deprecated, use st.cache_data
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
# from google_auth_httplib2 import Request # Not explicitly used, http is built directly
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp # Asegúrate de que esta librería esté instalada

# from googleapiclient.http import HttpRequest
# from googleapiclient.http import build_http
# http = build_http() # This can cause issues if not managed carefully with caching/session state
# http.timeout = 120 

st.set_page_config(layout="wide")

# Define new group names and corresponding folder names
# Ensure these folder names EXACTLY match what's inside your 'data/' directory in the ZIP
EXPECTED_GROUP_FOLDERS = {
    "old": "OLD",
    "young": "YOUNG",
    "middle-age": "MIDDLE-AGE", # Use the exact value from your 'age_group' column
    "person": "PERSON" # Assuming 'person' is an age_group value
}
# If your age_group column has values like 'older', 'younger', 'middle_aged', 'general_person'
# then update EXPECTED_GROUP_FOLDERS keys accordingly.
# The values ("OLD", "YOUNG", etc.) are the FOLDER NAMES in the ZIP.

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df_results = None
    st.session_state.image_folders = {} # To store images from multiple group folders
    st.session_state.group_filter = "Todos"  
    st.session_state.search_term = ""  
    st.session_state.ORIGINAL_FILENAME_COLUMN = "filename" # Columna con los nombres originales (ej. .png)
    st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN = "filename_actual_jpg" # Nombre de la columna que se creará y usará

@st.cache_data()
def count_observations(df, category_column_name, options, is_activity_filter=False):
    # category_column_name is the actual column in df, or 'prompt' for activities
    if is_activity_filter: # Activities are keywords in 'prompt'
        return {option: df['prompt'].str.contains(option, case=False, na=False).sum() for option in options}
    elif category_column_name in df.columns:
        # For list-like columns (e.g., objects), we need to check for containment
        if df[category_column_name].apply(lambda x: isinstance(x, list)).any():
             return {option: df[category_column_name].apply(lambda x: option in x if isinstance(x, list) else False).sum() for option in options}
        return {option: df[df[category_column_name].astype(str) == str(option)].shape[0] for option in options}
    return {option: 0 for option in options}


@st.cache_data()
def get_sorted_options(df, category_key, options):
    # category_key is the key used in st.session_state.categories (e.g., "activities", "gender")
    # This needs to map to the actual DataFrame column name
    
    column_name_for_counting = category_key
    is_activity = False
    if category_key == 'activities': # Special handling for activities in prompt
        column_name_for_counting = 'prompt'
        is_activity = True
    elif category_key == 'assistive_devices':
        column_name_for_counting = 'assistive_devices'
    elif category_key == 'digital_devices':
        column_name_for_counting = 'digital_devices'
    # Add other mappings if category_key doesn't match df column name

    counts = count_observations(df, column_name_for_counting, options, is_activity_filter=is_activity)
    options_with_count = sorted([(option, count) for option, count in counts.items()], key=lambda x: x[1], reverse=True)
    return [f"{option} ({count})" for option, count in options_with_count if count > 0] # Only show options with counts

@st.cache_data(max_entries=1)
def create_downloadable_zip(filtered_df, image_folders_dict):
    zip_buffer = io.BytesIO()

    actual_fn_col_name_in_df = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN # Leerlo una vez fuera del bucle
    original_fn_col_name_in_df = st.session_state.ORIGINAL_FILENAME_COLUMN
    
    try:
        with ZipFile(zip_buffer, 'w') as zip_file:
            for _, row in filtered_df.iterrows():
                # image_name = row.get('filename')  # Use 'filename'

                image_name_for_path_and_zip = row.get(actual_fn_col_name_in_df)
                image_name_original = row.get(original_fn_col_name_in_df)
                age_group = row.get('age_group') # Use 'age_group' to determine folder

                # if image_name is None:
                #     st.warning(f"No se encontró el nombre de la imagen (filename) en la fila: {row.get('ID', 'N/A')}")
                #     continue

                if image_name_for_path_and_zip is None:
                    st.warning(f"No se encontró '{actual_fn_col_name_in_df}' en la fila: ID {row.get('ID', 'N/A')}")
                    continue
                
                if age_group is None:
                    st.warning(f"No se encontró el 'age_group' en la fila: {row.get('ID', 'N/A')}")
                    continue

                # Find the corresponding folder name in ZIP (e.g., "OLD", "YOUNG")
                # based on the age_group value (e.g., "old", "young")
                folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(str(age_group).lower())

                if not folder_name_in_zip:
                    st.warning(f"Grupo de edad '{age_group}' no mapea a una carpeta conocida en EXPECTED_GROUP_FOLDERS. Fila ID: {row.get('ID', 'N/A')}")
                    continue
                
                # Get images for this specific age_group
                current_group_images = image_folders_dict.get(folder_name_in_zip)
                if not current_group_images:
                    st.warning(f"No se encontraron imágenes cargadas para el grupo de carpetas: {folder_name_in_zip}")
                    continue

                image_path = current_group_images.get(image_name_for_path_and_zip)
                
                if image_path and os.path.exists(image_path):
                    #zip_file.write(image_path, os.path.join(folder_name_in_zip, image_name))
                    zip_file.write(image_path, os.path.join(folder_name_in_zip, image_name_for_path_and_zip))

                else:
                    #st.warning(f"No se encontró la imagen '{image_name}' en la carpeta '{folder_name_in_zip}' o la ruta no existe.")
                    st.warning(f"No se encontró la imagen '{image_name_for_path_and_zip}' (original: {image_name_original}) ...")
                    
    except Exception as e:
        st.error(f"Error al crear el archivo ZIP: {str(e)}")
    finally:
        zip_buffer.seek(0)
    return zip_buffer
    
@st.cache_resource
def get_drive_service():
    try:
        encoded_sa = os.getenv('GOOGLE_SERVICE_ACCOUNT')
        if not encoded_sa:
            # Intenta cargar desde los secretos de Streamlit si no está en el entorno
            if 'GOOGLE_SERVICE_ACCOUNT_B64' in st.secrets:
                encoded_sa = st.secrets['GOOGLE_SERVICE_ACCOUNT_B64']
            else:
                raise ValueError("La variable de entorno GOOGLE_SERVICE_ACCOUNT o el secreto GOOGLE_SERVICE_ACCOUNT_B64 no están configurados")

        sa_json = base64.b64decode(encoded_sa).decode('utf-8')
        sa_dict = json.loads(sa_json)

        credentials = service_account.Credentials.from_service_account_info(
            sa_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

        # Crear un cliente HTTP autorizado a partir de las credenciales
        authed_http = AuthorizedHttp(credentials)
        # Establecer el timeout en el cliente HTTP autorizado
        # httplib2 (usado por google-auth-httplib2) espera el timeout en el constructor
        # o directamente en el atributo del objeto http.
        # Para google-auth-httplib2, el timeout se pasa al construir el objeto httplib2.Http() subyacente
        # o se puede intentar configurar directamente si la propiedad existe.
        # La forma más común es que AuthorizedHttp maneje la creación de httplib2.Http
        # y si httplib2.Http tiene un timeout por defecto, ese se usará.
        # Para establecer un timeout específico, usualmente se pasa al construir el httplib2.Http
        # que AuthorizedHttp podría encapsular.
        # Sin embargo, httplib2.Http sí tiene un atributo timeout.
        
        # Re-creamos el objeto http subyacente con el timeout si es necesario,
        # o confiamos en que `AuthorizedHttp` lo haga, o lo configuramos si es posible.
        # La documentación de google-auth-httplib2 no es explícita sobre cómo pasar el timeout
        # directamente a AuthorizedHttp.
        # Una forma es acceder al objeto http subyacente si AuthorizedHttp lo expone,
        # o construirlo con httplib2 y luego pasarlo a AuthorizedHttp.
        # Por ahora, intentaremos la forma más directa:
        if hasattr(authed_http, 'timeout'):
             authed_http.timeout = 120
        else:
            # Si AuthorizedHttp no tiene un atributo 'timeout' directo,
            # esto indica que el timeout debe configurarse en el objeto httplib2.Http
            # que AuthorizedHttp usa internamente. Esto es más complejo de acceder directamente.
            # Para la mayoría de los casos, el timeout por defecto de httplib2 podría ser suficiente,
            # o la librería maneja reintentos.
            # Si los timeouts persisten como problema, se necesitaría una inicialización más explícita
            # del objeto httplib2.Http con el timeout y luego envolverlo con google_auth_httplib2.
            # Por simplicidad y basado en que tu código original tenía http.timeout = 120,
            # asumimos que el objeto que usa AuthorizedHttp (probablemente httplib2.Http)
            # podría tener este atributo accesible o que la librería lo configura.
            # Una alternativa más robusta si lo anterior no funciona:
            import httplib2
            http_client = httplib2.Http(timeout=120)
            authed_http = AuthorizedHttp(credentials, http=http_client)


        # Construir el servicio usando el cliente HTTP autorizado y configurado
        service = build('drive', 'v3', http=authed_http)
        return service
    except Exception as e:
        # Captura el error específico si es de tipo AttributeError por 'timeout'
        if isinstance(e, AttributeError) and "'AuthorizedHttp' object has no attribute 'timeout'" in str(e):
             st.error(f"Error configurando timeout en AuthorizedHttp: {str(e)}. "
                      "Esto podría requerir un ajuste en cómo se instancia httplib2.Http "
                      "con el timeout antes de pasarlo a AuthorizedHttp.")
        st.error(f"Error al obtener el servicio de Google Drive: {str(e)}")
        return None

# def get_drive_service():
#     try:
#         encoded_sa = os.getenv('GOOGLE_SERVICE_ACCOUNT')
#         if not encoded_sa:
#             # Try to load from Streamlit secrets if not in env
#             if 'GOOGLE_SERVICE_ACCOUNT_B64' in st.secrets:
#                 encoded_sa = st.secrets['GOOGLE_SERVICE_ACCOUNT_B64']
#             else:
#                 raise ValueError("La variable de entorno GOOGLE_SERVICE_ACCOUNT o el secreto GOOGLE_SERVICE_ACCOUNT_B64 no están configurados")

#         sa_json = base64.b64decode(encoded_sa).decode('utf-8')
#         sa_dict = json.loads(sa_json)

#         credentials = service_account.Credentials.from_service_account_info(
#             sa_dict,
#             scopes=['https://www.googleapis.com/auth/drive.readonly']
#         )
#         # Use a fresh http object for the service to avoid timeout issues with a global one
#         service_http = build_http()
#         service_http.timeout = 120
#         service = build('drive', 'v3', credentials=credentials, http=service_http)
#         return service
#     except Exception as e:
#         st.error(f"Error al obtener el servicio de Google Drive: {str(e)}")
#         return None

def list_files_in_folder(service, folder_id, retries=3):
    for attempt in range(retries):
        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents",
                fields="files(id, name)"
            ).execute()
            return results.get('files', [])
        except HttpError as error:
            st.error(f"Error al listar archivos (intento {attempt+1}): {error}")
            if attempt < retries - 1:
                time.sleep(5)  
            else:
                raise

def download_file_from_google_drive(service, file_id, dest_path, retries=3):
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(dest_path, 'wb')
            # Pass the service's http object to MediaIoBaseDownload
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024*5) # 5MB chunk
            
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=2) # num_retries for chunk
                # st.write(f'Download {int(status.progress() * 100)}%')
            
            fh.close()
            # st.success(f"Archivo descargado correctamente") # Moved to main flow
            return
        except Exception as e:
            st.error(f"Error al descargar el archivo (intento {attempt+1}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(5)  
            else:
                raise

def extract_zip(zip_path, extract_to):
    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        st.write("Archivos extraídos en:", extract_to)
        st.write("Contenido de la carpeta extraída:", os.listdir(extract_to))
        # Check for 'data' subdirectory
        if 'data' in os.listdir(extract_to):
            st.write("Contenido de 'data/':", os.listdir(os.path.join(extract_to, 'data')))
        else:
            st.warning("No se encontró la subcarpeta 'data/' en el ZIP extraído.")

    except Exception as e:
        st.error(f"Error al extraer el archivo ZIP: {str(e)}")

@st.cache_data()
def extract_folder_id(url):
    match = re.search(r'folders/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    return None

def show_image_details(image_data):
    for key, value in image_data.items():
        st.write(f"**{key}:** {value}")

@st.cache_data(persist="disk")
def read_images_from_folder(folder_path):
    images = {}
    if not os.path.exists(folder_path):
        st.warning(f"La carpeta de imágenes no existe: {folder_path}")
        return images
    filenames = sorted(os.listdir(folder_path), key=natural_sort_key)
    for filename in filenames:
        if filename.lower().endswith((".jpg", ".jpeg")):
            image_path = os.path.join(folder_path, filename)
            images[filename] = image_path
    return images

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

# Removed: @st.cache_data(persist="disk")
# def read_dataframe_from_zip(zip_path):
#     # This logic is now part of the main loading sequence
#     pass

def toggle_fullscreen(image_name):
    if 'fullscreen_image' in st.session_state and st.session_state.fullscreen_image == image_name:
        st.session_state.fullscreen_image = None
    else:
        st.session_state.fullscreen_image = image_name

def get_default(category_key):
    # Used to repopulate multiselects after a filter reset or rerun
    session_key = f"multiselect_{category_key}"
    if session_key in st.session_state:
        return st.session_state[session_key]
    return []


@st.cache_data(persist="disk")
def get_unique_list_items(df_results, column_name):
    if column_name in df_results.columns:
        # Handle potential lists of strings (like in 'objects') or simple strings
        all_items = []
        for item_list in df_results[column_name].dropna():
            if isinstance(item_list, list):
                all_items.extend(item_list)
            elif isinstance(item_list, str):
                try:
                    # Attempt to evaluate if it's a string representation of a list
                    evaluated_item = eval(item_list)
                    if isinstance(evaluated_item, list):
                        all_items.extend(evaluated_item)
                    else:
                        all_items.append(str(item_list)) # Treat as a single string item
                except (SyntaxError, NameError):
                    all_items.append(str(item_list)) # Treat as a single string item
            else: # numbers, etc.
                all_items.append(str(item_list))

        # Convert all to string to ensure hashability and uniqueness
        unique_items_str = set(map(str, all_items))
        return sorted(list(unique_items_str))
    return []


@st.cache_data()
def get_unique_objects_with_counts(df, column_name):
    """
    Processes a column that might contain lists of strings (e.g., objects)
    or string representations of lists.
    Returns a dictionary of unique items and their counts, sorted by count.
    """
    item_counts = {}
    if column_name not in df.columns:
        return {}
        
    for entry in df[column_name].dropna():
        items_to_process = []
        if isinstance(entry, list):
            items_to_process = entry
        elif isinstance(entry, str):
            try:
                evaluated_entry = eval(entry) # Handles "['item1', 'item2']"
                if isinstance(evaluated_entry, list):
                    items_to_process = evaluated_entry
                # else:  # It's a plain string, not a list string. Handled by direct add later if needed
                #    items_to_process = [entry] 
            except (SyntaxError, NameError): # Not a parsable list string, treat as single item
                items_to_process = [entry] # Or decide how to handle plain strings
        
        for item in items_to_process:
            item_str = str(item) # Ensure consistency
            item_counts[item_str] = item_counts.get(item_str, 0) + 1
            
    return dict(sorted(item_counts.items(), key=lambda item: item[1], reverse=True))


st.markdown("<h1 style='text-align: center; color: white;'>AGEAI: Imágenes y Metadatos. v30 (Nuevo DF)</h1>", unsafe_allow_html=True)

# Updated instructions
st.markdown(f"""
<details>
<summary>📋 Instrucciones para el archivo ZIP</summary>

<h3>📁 Estructura de Carpetas</h3>
El archivo ZIP debe contener una carpeta `data/` en su raíz. Dentro de `data/`, deben estar:
<ul>
    <li><code>data/</code>
        <ul>
            <li>Una carpeta para cada grupo de edad/tipo de imagen. Los nombres de estas carpetas <b>deben coincidir exactamente</b> con los valores definidos en <code>EXPECTED_GROUP_FOLDERS</code> (ej: <code>{', '.join(EXPECTED_GROUP_FOLDERS.values())}</code>).
                <ul>
                    <li><code>OLD/</code>
                        <ul><li>[imágenes .jpg o .jpeg]</li></ul>
                    </li>
                    <li><code>YOUNG/</code>
                        <ul><li>[imágenes .jpg o .jpeg]</li></ul>
                    </li>
                    <li><code>MIDDLE-AGE/</code>
                        <ul><li>[imágenes .jpg o .jpeg]</li></ul>
                    </li>
                    <li><code>PERSON/</code>
                        <ul><li>[imágenes .jpg o .jpeg]</li></ul>
                    </li>
                    <li><i>(y así sucesivamente si hay más grupos)</i></li>
                </ul>
            </li>
            <li>Un archivo CSV (ej: <code>df_results.csv</code>, <code>data.csv</code>). El nombre debe empezar con "df_" o ser un CSV genérico. La aplicación buscará el primer CSV que encuentre en la carpeta `data/`.</li>
        </ul>
    </li>
</ul>

<h3>📄 Archivo CSV</h3>
<ul>
    <li>Debe estar ubicado directamente dentro de la carpeta <code>data/</code>.</li>
    <li>Columnas esperadas mínimas: <code>filename</code> (nombre del archivo de imagen, ej: "image1.jpg"), <code>prompt</code>, <code>age_group</code> (ej: "old", "young", "middle-age", "person" - estos valores deben coincidir con las claves de <code>EXPECTED_GROUP_FOLDERS</code>), <code>ID</code>.</li>
    <li>Otras columnas como <code>person_count</code>, <code>location</code>, <code>objects</code>, <code>assistive_devices</code>, <code>digital_devices</code>, <code>gender</code>, <code>age</code> (rango), <code>race</code>, <code>emotion</code>, <code>personality</code>, <code>position</code> serán usadas para filtros si están presentes.</li>
</ul>

<h3>🖼️ Imágenes</h3>
<ul>
    <li>Formato .jpg o .jpeg.</li>
    <li>Los nombres de archivo deben coincidir con los de la columna <code>filename</code> del CSV.</li>
    <li>Organizadas en las carpetas de grupo correspondientes (<code>OLD/</code>, <code>YOUNG/</code>, etc.) dentro de <code>data/</code>.</li>
</ul>

</details>
""", unsafe_allow_html=True)
st.markdown(" ")


# Initialize categories dictionary structure
if 'categories' not in st.session_state:
    st.session_state.categories = {
        "gender": [], "race": [], "activities": [], "emotion": [],
        "personality": [], "position": [], "person_count": [], "location": [],
        # These are special and will be handled by get_unique_objects_with_counts
        # "objects": [], "assistive_devices": [], "digital_devices": []
    }

if not st.session_state.data_loaded:
    service = get_drive_service()
    if service is None:
        st.error("No se pudo establecer la conexión con Google Drive.")
        st.stop()
    else:
        success_message = st.empty()
        success_message.success("Conexión a Google Drive establecida correctamente.")
        time.sleep(2)
        success_message.empty()

    folder_url = st.text_input("Ingrese el enlace de la carpeta de Google Drive:", value="")
    folder_id = extract_folder_id(folder_url)

    if not folder_id:
        if folder_url: # Only show warning if user typed something
            st.warning("Por favor, ingrese un enlace de carpeta de Google Drive válido.")
        st.stop()

    try:
        files = list_files_in_folder(service, folder_id)
    except HttpError as e:
        st.error(f"Error crítico al listar archivos de Google Drive: {e}")
        st.stop()
        
    if not files:
        st.error("No se encontraron archivos en la carpeta de Google Drive.")
        st.stop()

    file_options = {item['name']: item['id'] for item in files if item['name'].endswith('.zip')}
    if not file_options:
        st.error("No se encontraron archivos .zip en la carpeta de Google Drive.")
        st.stop()
        
    selected_file_name = st.selectbox("Selecciona el archivo ZIP:", list(file_options.keys()))

    if selected_file_name and st.button("Confirmar selección y Cargar Datos"):
        with st.spinner("Descargando y procesando archivo ZIP... Esto puede tardar unos minutos."):
            file_id = file_options[selected_file_name]
            temp_zip_path = "temp_data.zip" # Use a more descriptive name
            
            try:
                download_file_from_google_drive(service, file_id, temp_zip_path)
                st.success(f"Archivo '{selected_file_name}' descargado.")
            except Exception as e:
                st.error(f"Fallo al descargar el archivo ZIP: {e}")
                if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                st.stop()

            temp_extract_path = "extracted_data_content" # More descriptive
            if os.path.exists(temp_extract_path): # Clean up previous extraction
                shutil.rmtree(temp_extract_path, ignore_errors=True)
            
            try:
                extract_zip(temp_zip_path, temp_extract_path)
            except Exception as e:
                st.error(f"Fallo al extraer el archivo ZIP: {e}")
                if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                st.stop()
            
            if os.path.exists(temp_extract_path):
                data_folder_path = os.path.join(temp_extract_path, 'data')
                if not os.path.exists(data_folder_path):
                    st.error(f"La carpeta 'data/' no se encontró dentro del ZIP extraído en '{temp_extract_path}'. Verifique la estructura del ZIP.")
                    if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                    if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                    st.stop()

                st.session_state.image_folders = {}
                loaded_any_images = False
                for age_group_key, folder_name_in_zip in EXPECTED_GROUP_FOLDERS.items():
                    current_img_folder_path = os.path.join(data_folder_path, folder_name_in_zip)
                    if os.path.exists(current_img_folder_path):
                        images = read_images_from_folder(current_img_folder_path)
                        st.session_state.image_folders[folder_name_in_zip] = images
                        st.write(f"Cargadas {len(images)} imágenes de la carpeta '{folder_name_in_zip}'.")
                        if images: loaded_any_images = True
                    else:
                        st.warning(f"Carpeta de imágenes '{folder_name_in_zip}' no encontrada en '{data_folder_path}'.")
                
                if not loaded_any_images and not st.session_state.image_folders: # Check if any images at all were loaded
                    st.error("No se cargaron imágenes de ninguna carpeta de grupo. Verifique la estructura del ZIP y los nombres de las carpetas.")
                    # Clean up and stop
                    if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                    if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                    st.stop()


                # # Load DataFrame
                # csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv') and (f.startswith('df_') or True)] # More flexible CSV naming
                # if not csv_files:
                #     st.error(f"No se encontró ningún archivo CSV en '{data_folder_path}'.")
                #     # Clean up and stop
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                #     st.stop()

                # csv_file_path = os.path.join(data_folder_path, csv_files[0])
                # st.write(f"Intentando leer CSV desde: {csv_file_path}")

                # # +++++ INICIO DE LAS COMPROBACIONES DEL ARCHIVO CSV ANTES DEL TRY +++++
                # st.write(f"Ruta completa al archivo CSV a leer: {csv_file_path}")
                # if not os.path.exists(csv_file_path):
                #     st.error(f"¡El archivo CSV NO EXISTE en la ruta especificada!: {csv_file_path}")
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path) # Limpieza
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True) # Limpieza
                #     st.stop()
                
                # file_size = os.path.getsize(csv_file_path)
                # st.write(f"Tamaño del archivo CSV: {file_size} bytes")
                # if file_size == 0:
                #     st.error(f"¡El archivo CSV está VACÍO (0 bytes)!: {csv_file_path}")
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path) # Limpieza
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True) # Limpieza
                #     st.stop()
                # # +++++ FIN DE LAS COMPROBACIONES DEL ARCHIVO CSV ANTES DEL TRY +++++
                                
                
                # try:
                #     #st.session_state.df_results = pd.read_csv(csv_file_path)
                #     df_temp = pd.read_csv(csv_file_path) # Cargar en una variable temporal primero

                #  # +++++ INICIO DE LA LÓGICA DE DEPURACIÓN PARA df_temp is None +++++
                #     if df_temp is None:
                #         st.error(f"pd.read_csv devolvió None para el archivo {csv_file_path}. El archivo podría ser inválido, tener un formato inesperado o estar vacío de contenido interpretable por Pandas.")
                        
                #         # Intento con otra codificación
                #         st.write("Intentando leer CSV con encoding 'latin1' como prueba...")
                #         try:
                #             df_temp_latin1 = pd.read_csv(csv_file_path, encoding='latin1')
                #             if df_temp_latin1 is not None and not df_temp_latin1.empty:
                #                 st.info("Leer con 'latin1' pareció funcionar y produjo un DataFrame no vacío. El CSV podría no ser UTF-8.")
                #                 df_temp = df_temp_latin1 # Usar esta versión si tuvo éxito
                #             elif df_temp_latin1 is None:
                #                 st.warning("Leer con 'latin1' también devolvió None.")
                #             else: # DataFrame vacío
                #                 st.warning(f"Leer con 'latin1' devolvió un DataFrame vacío (Columnas: {df_temp_latin1.columns.tolist()}, Filas: {len(df_temp_latin1)}).")
                #         except Exception as e_latin1:
                #             st.warning(f"Error al intentar leer con 'latin1': {type(e_latin1).__name__} - {e_latin1}")
                
                #         # Si df_temp sigue siendo None después de los intentos, mostrar primeras líneas y detener
                #         if df_temp is None:
                #             st.warning("El DataFrame sigue siendo None después de intentar con 'latin1'.")
                #             try:
                #                 with open(csv_file_path, 'r', errors='ignore') as f_inspect: # Usar errors='ignore' para evitar errores de decode al solo inspeccionar
                #                     st.text("Primeras 5 líneas del archivo CSV para inspección (pueden no mostrarse bien si la codificación es incorrecta):")
                #                     for i in range(5):
                #                         line = f_inspect.readline()
                #                         if not line:
                #                             break
                #                         st.text(f"Línea {i+1}: {line.strip()}")
                #             except Exception as e_inspect:
                #                 st.warning(f"No se pudieron leer las primeras líneas del CSV para inspección: {e_inspect}")
                            
                #             if os.path.exists(temp_zip_path): os.remove(temp_zip_path) # Limpieza
                #             if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True) # Limpieza
                #             st.stop() # Detener ejecución si no se puede cargar el DataFrame
                            
                #     # +++++ FIN DE LA LÓGICA DE DEPURACIÓN PARA df_temp is None +++++
                    
                #     original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN
                #     actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN
                    
                #     st.write(f"DataFrame cargado desde '{csv_files[0]}'. Columnas: {st.session_state.df_results.columns.tolist()}")
                #     st.write(f"Usando '{original_fn_col}' como columna de nombre de archivo original.")
                #     st.write(f"Se creará/usará '{actual_fn_col}' para los nombres de archivo de imagen reales.")
                    
                #     # if 'filename' in st.session_state.df_results.columns:
                #     #     # Crear la nueva columna reemplazando .png por .jpg
                #     #     # Asegúrate de que solo reemplaza al final del string si es necesario
                #     #     st.session_state.df_results['filename_jpg'] = st.session_state.df_results['filename'].apply(
                #     #         lambda x: x.rpartition('.')[0] + '.jpg' if isinstance(x, str) and x.lower().endswith('.png') else x
                #     #     )
                #     #     # Si algunos ya son .jpg o tienen otras extensiones, se mantendrán igual con la lógica anterior
                #     #     # o puedes ser más específico:
                #     #     # st.session_state.df_results['filename_jpg'] = st.session_state.df_results['filename'].str.replace(r'\.png$', '.jpg', regex=True, case=False)
                #     #     st.write("Columna 'filename_jpg' creada a partir de 'filename'.")
                #     # else:
                #     #     st.error("La columna 'filename' es necesaria para crear 'filename_jpg'.")
                #     #     # Detener o manejar el error adecuadamente
                    
                #     if original_fn_col in df_temp.columns:
                #         # Crear la nueva columna con el nombre dinámico
                #         df_temp[actual_fn_col] = df_temp[original_fn_col].apply(
                #             lambda x: x.rpartition('.')[0] + '.jpg' if isinstance(x, str) and x.lower().endswith('.png') else x
                #         )
                #         st.write(f"Columna '{actual_fn_col}' creada/actualizada.")
                #     elif actual_fn_col in df_temp.columns:
                #         # Si la columna ya existe (quizás pre-procesada), simplemente la usamos.
                #         st.write(f"Usando columna existente '{actual_fn_col}' para nombres de archivo de imagen.")
                #     else:
                #         st.error(f"No se encontró la columna '{original_fn_col}' para procesar, ni la columna '{actual_fn_col}' preexistente.")
                #         st.stop()
                        
                #     st.session_state.df_results = df_temp  # Actualizar el DataFrame en session_state   


                # except pd.errors.EmptyDataError: # Capturar específicamente si Pandas dice que el archivo está vacío
                #     st.error(f"Error al leer el archivo CSV '{csv_files[0]}': Pandas reporta EmptyDataError (El archivo puede estar vacío o no contener datos después del encabezado).")
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                #     st.stop()
                # except Exception as e: # Captura otras excepciones de pd.read_csv o del procesamiento
                #     st.error(f"Error durante la lectura o procesamiento inicial del archivo CSV '{csv_files[0]}': {type(e).__name__} - {e}")
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                #     st.stop()
                    
                # # except Exception as e:
                # #     st.error(f"Error al leer el archivo CSV '{csv_files[0]}': {e}")
                # #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                # #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                # #     st.stop()
                # if st.session_state.df_results is None: # Doble comprobación por si acaso, aunque no debería llegar aquí
                #     st.error("Error crítico: df_results es None después del bloque try-except de carga. Deteniendo.")
                #     if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                #     if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                #     st.stop()

#####################################
                # Load DataFrame
                csv_files = [f for f in os.listdir(data_folder_path) if f.endswith('.csv') and (f.startswith('df_') or True)]
                if not csv_files:
                    st.error(f"No se encontró ningún archivo CSV en '{data_folder_path}'.")
                    if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                    if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                    st.stop()
                
                csv_file_path = os.path.join(data_folder_path, csv_files[0])
                st.write(f"Intentando leer CSV desde: {csv_file_path}")
                
                df_temp = None # Inicializar a None
                read_error = None
                
                try:
                    df_temp = pd.read_csv(csv_file_path)
                except Exception as e:
                    read_error = e
                    st.error(f"Excepción DIRECTA al llamar a pd.read_csv: {type(e).__name__} - {e}")
                
                st.write(f"Tipo de df_temp después de pd.read_csv: {type(df_temp)}")
                
                if df_temp is None:
                    st.error(f"df_temp ES None después de pd.read_csv. Error de lectura previo (si hubo): {read_error}")
                    # Aquí puedes añadir el intento con 'latin1' y la inspección de las primeras líneas si es necesario
                    try:
                        st.write("Intentando leer CSV con encoding 'latin1' como prueba...")
                        df_temp_latin1 = pd.read_csv(csv_file_path, encoding='latin1')
                        if df_temp_latin1 is not None:
                            st.info("Leer con 'latin1' devolvió un objeto. Tipo: {type(df_temp_latin1)}")
                            if not df_temp_latin1.empty:
                                st.info("DataFrame con latin1 no está vacío.")
                                df_temp = df_temp_latin1 # Intentar usar este
                            else:
                                 st.warning(f"DataFrame con latin1 está VACÍO. Columnas: {df_temp_latin1.columns.tolist()}")
                        else:
                            st.warning("Leer con 'latin1' también devolvió None.")
                    except Exception as e_latin1:
                        st.warning(f"Error al intentar leer con 'latin1': {type(e_latin1).__name__} - {e_latin1}")
                
                    if df_temp is None: # Si sigue siendo None
                        st.warning("El DataFrame sigue siendo None. Inspeccionando primeras líneas del archivo...")
                        try:
                            with open(csv_file_path, 'r', errors='ignore') as f_inspect:
                                st.text("Primeras 5 líneas del archivo CSV:")
                                for i in range(5):
                                    line = f_inspect.readline()
                                    if not line: break
                                    st.text(f"L{i+1}: {line.strip()}")
                        except Exception as e_inspect:
                            st.warning(f"No se pudieron leer las primeras líneas para inspección: {e_inspect}")
                        
                        if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                        if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                        st.stop() # Detener si no se puede cargar
                
                # Si llegamos aquí, df_temp NO debería ser None
                st.write(f"DataFrame procesado (antes de más operaciones). Columnas: {df_temp.columns.tolist()}") # ESTA LÍNEA AHORA DEBERÍA FUNCIONAR
                
                # --- AHORA REINTRODUCIMOS EL RESTO DE LA LÓGICA DE FORMA SEGURA ---
                try:
                    original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN
                    actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN
                    
                    st.write(f"Usando '{original_fn_col}' como columna de nombre de archivo original.")
                    st.write(f"Se creará/usará '{actual_fn_col}' para los nombres de archivo de imagen reales.")
                    
                    if original_fn_col in df_temp.columns:
                        df_temp[actual_fn_col] = df_temp[original_fn_col].apply(
                            lambda x: x.rpartition('.')[0] + '.jpg' if isinstance(x, str) and x.lower().endswith('.png') else x
                        )
                        st.write(f"Columna '{actual_fn_col}' creada/actualizada.")
                    elif actual_fn_col in df_temp.columns:
                        st.write(f"Usando columna existente '{actual_fn_col}' para nombres de archivo de imagen.")
                    else:
                        st.error(f"No se encontró la columna '{original_fn_col}' para procesar, ni la columna '{actual_fn_col}' preexistente.")
                        st.stop()
                        
                    st.session_state.df_results = df_temp
                
                except Exception as e_processing:
                    st.error(f"Error DURANTE EL PROCESAMIENTO del DataFrame (después de la lectura): {type(e_processing).__name__} - {e_processing}")
                    if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                    if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                    st.stop()
                
                
                # El resto de tu código para comprobar columnas requeridas, dropna, etc.
                if st.session_state.df_results is None:
                    st.error("Error crítico: df_results es None después del bloque try-except de carga. Deteniendo.")
                    if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                    if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                    st.stop()
                
                if st.session_state.df_results is not None:
                    # required_columns = ['filename', 'prompt', 'age_group', 'ID']
            
                    required_columns = [st.session_state.ORIGINAL_FILENAME_COLUMN, 
                    st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN, # Asegurarse que esta exista después del paso anterior
                    'prompt', 'age_group', 'ID']
                    # Quitar duplicados si original_fn_col y actual_fn_col son iguales (poco probable aquí)
                    required_columns = list(dict.fromkeys(required_columns)) 
                    
                    missing_columns = [col for col in required_columns if col not in st.session_state.df_results.columns]
                    if missing_columns:
                        st.error(f"Las siguientes columnas obligatorias no se encontraron en el DataFrame: {', '.join(missing_columns)}")
                        if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                        if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path, ignore_errors=True)
                        st.stop()

                    # Asegurarse de no eliminar filas si la columna ACTUAL_IMAGE_FILENAME_COLUMN
                    # pudiera tener NaNs debido a que la ORIGINAL_FILENAME_COLUMN tenía NaNs.
                    # Es mejor hacer dropna solo en las columnas que siempre deben tener valor.
                    cols_for_dropna = [st.session_state.ORIGINAL_FILENAME_COLUMN, 'prompt', 'age_group', 'ID']
                    if st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN not in cols_for_dropna: # Evitar duplicados
                        if st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN in st.session_state.df_results.columns:
                             cols_for_dropna.append(st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN)
                    
                    st.session_state.df_results = st.session_state.df_results.dropna(subset=required_columns)
                    
                    # Standardize age_group to lower for matching with EXPECTED_GROUP_FOLDERS keys
                    if 'age_group' in st.session_state.df_results.columns:
                         st.session_state.df_results['age_group'] = st.session_state.df_results['age_group'].astype(str).str.lower()

                    # Define categories to populate dynamically for filters
                    # These should be the *keys* you use for st.session_state.categories
                    # and will map to DataFrame column names.
                    category_keys_to_populate = {
                        "gender": "gender", "race": "race", "emotion": "emotion",
                        "personality": "personality", "position": "position",
                        "person_count": "person_count", "location": "location",
                        # "activities" is special (searches prompt), no direct column needed unless you have one
                    }
                    
                    for cat_key, df_col_name in category_keys_to_populate.items():
                        if df_col_name in st.session_state.df_results.columns:
                            st.session_state.categories[cat_key] = get_unique_list_items(st.session_state.df_results, df_col_name)
                            if cat_key == 'personality' and st.session_state.categories[cat_key]: # Example: lowercase personality
                                st.session_state.categories[cat_key] = [p.lower() for p in st.session_state.categories[cat_key]]
                                st.session_state.df_results[df_col_name] = st.session_state.df_results[df_col_name].astype(str).str.lower()
                        else:
                            st.warning(f"Columna '{df_col_name}' para la categoría '{cat_key}' no encontrada en el DataFrame. El filtro no estará disponible.")
                            st.session_state.categories[cat_key] = []
                    
                    # For activities, if you have a predefined list of keywords you want to offer:
                    # st.session_state.categories['activities'] = ["keyword1", "keyword2", ...] 
                    # Otherwise, it remains empty, and users use the general search or a dedicated prompt search.
                    # For now, let's keep it empty to fulfill "sin tener que especificar las actividades"
                    st.session_state.categories['activities'] = [] # No predefined activity options initially
                    # If you want to populate from a specific "activity_keywords" column, you could do:
                    # if 'activity_keywords' in st.session_state.df_results.columns:
                    #    st.session_state.categories['activities'] = get_unique_list_items(st.session_state.df_results, 'activity_keywords')


                    st.session_state.data_loaded = True
                    st.success("Datos cargados y procesados correctamente. La aplicación se actualizará.")
                    
            # Clean up temporary files
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
            if os.path.exists(temp_extract_path):
                shutil.rmtree(temp_extract_path, ignore_errors=True)
            
            if st.session_state.data_loaded:
                st.rerun()
            else:
                st.error("La carga de datos falló. Revise los mensajes anteriores.")
else: # Data is loaded, show dashboard
    df_results = st.session_state.df_results
    image_folders_dict = st.session_state.image_folders

    st.sidebar.header("Filtrar imágenes")

    # Group filter using age_group values (keys of EXPECTED_GROUP_FOLDERS)
    group_options = ["Todos"] + list(EXPECTED_GROUP_FOLDERS.keys())
    
    # Find current index for group_filter for selectbox
    try:
        current_group_filter_index = group_options.index(st.session_state.group_filter)
    except ValueError:
        current_group_filter_index = 0 # Default to "Todos" if not found
        st.session_state.group_filter = "Todos"

    group_filter = st.sidebar.selectbox(
        "Seleccionar Grupo", 
        group_options, 
        index=current_group_filter_index,
        format_func=lambda x: x.replace("-", " ").title() # Prettier display
    )
    st.session_state.group_filter = group_filter

    filtered_df = df_results.copy()

    if group_filter != "Todos":
        # Filter by the age_group value (e.g., "old", "young")
        # Ensure age_group column in df is lowercase if keys in EXPECTED_GROUP_FOLDERS are lowercase
        filtered_df = df_results[df_results['age_group'].astype(str).str.lower() == group_filter.lower()]

    # Dynamic category filters
    categories_for_sidebar = st.session_state.categories.copy() # Operate on a copy

    # Age Range Filter (if 'age' column exists)
    if 'age' in df_results.columns:
        age_ranges = sorted(df_results['age'].astype(str).unique().tolist())
        # Get default remembers previous selection for "age_range" key
        selected_age_ranges_display = st.sidebar.multiselect(
            "Seleccionar Age Range",
            get_sorted_options(df_results, 'age', age_ranges), 
            default=get_default("age_range"), # Use a consistent key
            key="multiselect_age_range"
        )
        selected_age_ranges = [age.split(" (")[0] for age in selected_age_ranges_display]
        if selected_age_ranges:
            filtered_df = filtered_df[filtered_df['age'].astype(str).isin(selected_age_ranges)]
        #st.session_state.multiselect_age_range = selected_age_ranges_display # Save for repopulation

    if st.sidebar.button("Resetear Filtros"):
        st.session_state.group_filter = "Todos"
        st.session_state.search_term = ""
        # Clear all multiselect session states
        for key in list(st.session_state.keys()): # Iterate over a copy of keys
            if key.startswith('multiselect_'):
                st.session_state[key] = []
        st.rerun()

    for category_key, options in categories_for_sidebar.items():
        if not options and category_key != 'activities': # Skip empty option lists, unless it's activities (handled differently)
            continue

        filter_title = f"Seleccionar {category_key.replace('_', ' ').title()}"
        
        # For activities, allow selection from a (currently empty) predefined list,
        # or rely on general prompt search.
        if category_key == "activities":
             # If you had a list of common activities, get_sorted_options would populate them with counts
            selected_display = st.sidebar.multiselect(
                filter_title,
                get_sorted_options(df_results, 'activities', options), # 'options' is st.session_state.categories['activities']
                default=get_default(category_key),
                key=f"multiselect_{category_key}"
            )
            selected_options_values = [opt.split(" (")[0] for opt in selected_display]
            if selected_options_values:
                 # Apply OR logic: row matches if ANY selected activity keyword is in prompt
                pattern = '|'.join(map(re.escape, selected_options_values)) # Create a regex OR pattern
                filtered_df = filtered_df[filtered_df['prompt'].str.contains(pattern, case=False, na=False)]
            #st.session_state[f"multiselect_{category_key}"] = selected_display # Save for repopulation
            continue # Move to next category

        # For other categories (gender, race, etc.)
        df_column_name = category_key # Assuming category_key matches df column name for these
        
        selected_display = st.sidebar.multiselect(
            filter_title,
            get_sorted_options(df_results, df_column_name, options), # Use df_column_name for counting
            default=get_default(category_key),
            key=f"multiselect_{category_key}"
        )
        selected_options_values = [opt.split(" (")[0] for opt in selected_display]

        if selected_options_values:
            # Ensure comparison is robust (e.g. string vs string)
            # For list-like columns, this needs adjustment (see objects filter)
            if df_results[df_column_name].apply(lambda x: isinstance(x, list)).any():
                 # Handle columns that are lists of items
                filtered_df = filtered_df[filtered_df[df_column_name].apply(lambda L: isinstance(L, list) and any(item in selected_options_values for item in L))]
            else:
                # Handle columns with single string/numeric values
                filtered_df = filtered_df[filtered_df[df_column_name].astype(str).isin(selected_options_values)]
        #st.session_state[f"multiselect_{category_key}"] = selected_display # Save for repopulation


    # Object filters (using the new column names)
    object_columns_map = {
        "objects": "Objetos",
        "assistive_devices": "Dispositivos de Asistencia",
        "digital_devices": "Dispositivos Digitales"
    }

    for col_name, display_name in object_columns_map.items():
        if col_name in df_results.columns:
            unique_items_with_counts = get_unique_objects_with_counts(df_results, col_name)
            
            # Get default remembers previous selection for this object type
            selected_items_display = st.sidebar.multiselect(
                f"Seleccionar {display_name}",
                [f"{obj} ({count})" for obj, count in unique_items_with_counts.items()],
                default=get_default(col_name), # Use a consistent key
                key=f"multiselect_{col_name}" # Unique key for this multiselect
            )
            selected_items_values = [item.split(" (")[0] for item in selected_items_display]

            if selected_items_values:
                # Filter rows where the column (which might be a string representation of a list or an actual list)
                # contains ANY of the selected items.
                def check_item_presence(entry, items_to_find):
                    if pd.isna(entry):
                        return False
                    current_list = []
                    if isinstance(entry, list):
                        current_list = entry
                    elif isinstance(entry, str):
                        try:
                            evaluated = eval(entry)
                            if isinstance(evaluated, list):
                                current_list = evaluated
                        except: # Not a list string, treat as single item or ignore
                            pass # Or: current_list = [entry] if plain strings should be checked
                    
                    # Convert all items in current_list to string for comparison
                    current_list_str = [str(i) for i in current_list]
                    return any(item_to_find in current_list_str for item_to_find in items_to_find)

                filtered_df = filtered_df[filtered_df[col_name].apply(lambda x: check_item_presence(x, selected_items_values))]
            #st.session_state[f"multiselect_{col_name}"] = selected_items_display # Save for repopulation
        else:
            st.sidebar.caption(f"Columna '{col_name}' no encontrada para filtro de {display_name}.")


    st.sidebar.header("Buscador General")
    search_columns_options = ['Todas las Columnas'] + df_results.columns.tolist()
    
    # Determine default for selected_column_search
    default_search_column = 'Todas las Columnas'
    if 'selected_column_search' in st.session_state and st.session_state.selected_column_search in search_columns_options:
        default_search_column = st.session_state.selected_column_search
    
    selected_column_search = st.sidebar.selectbox(
        "Buscar en Variable", 
        search_columns_options, 
        index=search_columns_options.index(default_search_column)
    )
    st.session_state.selected_column_search = selected_column_search # Save selection

    search_term = st.sidebar.text_input(f"Término de Búsqueda", value=st.session_state.get("search_term", ""))
    st.session_state.search_term = search_term

    if search_term:
        if selected_column_search == 'Todas las Columnas':
            # Search across all columns
            # Create a boolean mask, True if any column contains the search term
            mask = filtered_df.apply(lambda row: row.astype(str).str.contains(search_term, case=False, na=False).any(), axis=1)
            filtered_df = filtered_df[mask]
        else:
            # Search in a specific column
            filtered_df = filtered_df[filtered_df[selected_column_search].astype(str).str.contains(search_term, case=False, na=False)]

    # --- Display Area ---
    st.markdown("---")
    st.subheader(f"Resultados Filtrados: {len(filtered_df)} imágenes")

    # Display applied filters
    applied_filters_summary = []
    if group_filter != "Todos":
        applied_filters_summary.append(f"Grupo: {group_filter.replace('-', ' ').title()}")
    
    # Age range
    if 'multiselect_age_range' in st.session_state and st.session_state.multiselect_age_range:
        applied_filters_summary.append(f"Age Range: {', '.join([s.split(' (')[0] for s in st.session_state.multiselect_age_range])}")

    # General categories
    for cat_key in st.session_state.categories.keys():
        session_ms_key = f"multiselect_{cat_key}"
        if session_ms_key in st.session_state and st.session_state[session_ms_key]:
            selected_vals = [s.split(' (')[0] for s in st.session_state[session_ms_key]]
            applied_filters_summary.append(f"{cat_key.replace('_', ' ').title()}: {', '.join(selected_vals)}")
    
    # Object categories
    for col_name_obj in object_columns_map.keys():
        session_ms_key = f"multiselect_{col_name_obj}"
        if session_ms_key in st.session_state and st.session_state[session_ms_key]:
            selected_vals = [s.split(' (')[0] for s in st.session_state[session_ms_key]]
            applied_filters_summary.append(f"{object_columns_map[col_name_obj]}: {', '.join(selected_vals)}")

    if search_term:
        applied_filters_summary.append(f"Búsqueda '{search_term}' en '{selected_column_search}'")

    if applied_filters_summary:
        with st.expander("Filtros Aplicados", expanded=False):
            for filter_info in applied_filters_summary:
                st.markdown(f"- {filter_info}")
    else:
        st.info("No se han aplicado filtros activos.")


    gb = AgGrid(
        filtered_df,
        height=400,
        width='100%',
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True, # Set to True if you trust the data source
        enable_enterprise_modules=False # Set to True if you have a license
    )

    csv = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Descargar Tabla Filtrada (CSV)",
        data=csv,
        file_name="filtered_data.csv",
        mime="text/csv",
    )

    st.markdown("---")
    st.subheader("Visualización de Imágenes")

    if 'fullscreen_image' not in st.session_state: # Initialize if not present
        st.session_state.fullscreen_image = None

    # Display images
    if st.session_state.fullscreen_image is None:
        if not filtered_df.empty:
            images_per_row = st.slider("Imágenes por fila", min_value=2, max_value=8, value=4, key="images_per_row_slider")
            for i in range(0, len(filtered_df), images_per_row):
                row_data = filtered_df.iloc[i:i+images_per_row]
                cols = st.columns(len(row_data)) # Create as many columns as there are images in this row
                for col_index, (idx, row) in enumerate(row_data.iterrows()):
                    # image_name = row.get('filename')
                    # age_group_val = row.get('age_group') # e.g., "old", "young"
                    
                    # if isinstance(image_name, str) and isinstance(age_group_val, str):
                    #     folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(age_group_val.lower())
                    #     image_path = None
                    #     if folder_name_in_zip and folder_name_in_zip in image_folders_dict:
                    #         image_path = image_folders_dict[folder_name_in_zip].get(image_name)

                    #     if image_path and os.path.exists(image_path):
                    #         try:
                    #             cols[col_index].image(image_path, caption=f"{image_name}\nID: {row.get('ID', 'N/A')}", use_column_width=True)
                    #             if cols[col_index].button(f"Detalles", key=f"btn_detail_{image_name}_{idx}"): # Unique key
                    #                 toggle_fullscreen(image_name)
                    #                 st.rerun()
                    #         except Exception as e:
                    #             cols[col_index].error(f"Error al cargar {image_name}: {e}")
                    #     else:
                    #         cols[col_index].warning(f"Imagen no encontrada: {image_name} en grupo {age_group_val}")

                    # +++++ INICIO DEL BLOQUE NUEVO CON LOS CAMBIOS +++++
                    image_name_actual_for_path = row.get(st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN)
                    image_name_original_for_df = row.get(st.session_state.ORIGINAL_FILENAME_COLUMN)
                    age_group_val = row.get('age_group')

                   # --- Debugging ---
                    cols[col_index].write(f"DF Original: {image_name_original_for_df}") # Descomentar para depurar
                    cols[col_index].write(f"DF Actual (.jpg): {image_name_actual_for_path}") # Descomentar para depurar
                    cols[col_index].write(f"Age Group: {age_group_val}") # Descomentar para depurar
                    # --- Fin Debugging ---
                    
                    if isinstance(image_name_actual_for_path, str) and \
                       isinstance(image_name_original_for_df, str) and \
                       isinstance(age_group_val, str):
                        
                        folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(age_group_val.lower())
                        image_path = None

                        # --- Debugging ---
                        if folder_name_in_zip:
                           cols[col_index].write(f"Folder in ZIP: {folder_name_in_zip}") # Descomentar
                           if folder_name_in_zip in image_folders_dict:
                               cols[col_index].write(f"Keys in image_folders_dict['{folder_name_in_zip}'][0:5]: {list(image_folders_dict[folder_name_in_zip].keys())[0:5]}") # Mostrar algunas claves
                           else:
                               cols[col_index].warning(f"Folder '{folder_name_in_zip}' NO ENCONTRADO en image_folders_dict. Claves disponibles: {list(image_folders_dict.keys())}")
                        else:
                           cols[col_index].warning(f"No se pudo mapear age_group '{age_group_val}' a folder_name_in_zip.")
                        # --- Fin Debugging ---
                           
                        if folder_name_in_zip and folder_name_in_zip in image_folders_dict:
                            # --- Debugging: Búsqueda exacta ---
                            target_key = image_name_actual_for_path
                            if target_key in image_folders_dict[folder_name_in_zip]:
                                cols[col_index].success(f"'{target_key}' ENCONTRADO en image_folders_dict['{folder_name_in_zip}']!")
                            else:
                                cols[col_index].error(f"'{target_key}' NO ENCONTRADO en image_folders_dict['{folder_name_in_zip}']!")
                            # --- Fin Debugging ---
                            image_path = image_folders_dict[folder_name_in_zip].get(image_name_actual_for_path)

                        # --- Debugging ---
                        cols[col_index].write(f"Image Path Generado: {image_path}") # Descomentar
                        # --- Fin Debugging ---
                           
                        if image_path and os.path.exists(image_path):
                            try:
                                cols[col_index].image(image_path, caption=f"{image_name_original_for_df}\nID: {row.get('ID', 'N/A')}", use_column_width=True)
                                # Usar una clave única para el botón, idealmente combinando el nombre del archivo y su índice de fila (idx)
                                button_key = f"btn_detail_{image_name_original_for_df}_{idx}" 
                                if cols[col_index].button(f"Detalles", key=button_key):
                                    toggle_fullscreen(image_name_original_for_df) # Pasamos el original para buscar en el DF y para el estado
                                    st.rerun()
                            except Exception as e:
                                cols[col_index].error(f"Error al cargar {image_name_actual_for_path}: {e}")
                        else:
                            #cols[col_index].warning(f"Imagen no encontrada: {image_name_actual_for_path} (original: {image_name_original_for_df}) en grupo {age_group_val}. Ruta esperada: {image_path if image_path else 'No generada'}")
                            # Mensaje de warning más detallado
                            warning_msg = f"Imagen no encontrada: '{image_name_actual_for_path}'"
                            warning_msg += f"\n(Original DF: '{image_name_original_for_df}')"
                            warning_msg += f"\nGrupo: '{age_group_val}' (Carpeta: '{folder_name_in_zip if folder_name_in_zip else 'No mapeada'}')"
                            if image_path:
                                warning_msg += f"\nRuta generada: '{image_path}' (Pero os.path.exists es Falso)"
                            else:
                                warning_msg += "\nRuta no generada (clave no encontrada en diccionario de imágenes)"
                            cols[col_index].warning(warning_msg)
                            
                    elif not isinstance(image_name_actual_for_path, str):
                        cols[col_index].caption(f"Falta '{st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN}' para ID: {row.get('ID', 'N/A')}")
                    elif not isinstance(image_name_original_for_df, str):
                         cols[col_index].caption(f"Falta '{st.session_state.ORIGINAL_FILENAME_COLUMN}' para ID: {row.get('ID', 'N/A')}")
                    # +++++ FIN DEL BLOQUE NUEVO +++++
                    
                st.markdown("<hr style='margin-top: 10px; margin-bottom: 10px;'>", unsafe_allow_html=True)
        else:
            st.info("No hay imágenes que coincidan con los filtros aplicados.")
    else: # Fullscreen mode
        col1, col2 = st.columns([3, 2]) # Image on left, details on right
        
        fullscreen_image_name = st.session_state.fullscreen_image
        
        fullscreen_image_name_original = st.session_state.fullscreen_image # Este es el original del DF
        # Find the row corresponding to the fullscreen image to get its age_group
        #fullscreen_row_df = filtered_df[filtered_df['filename'] == fullscreen_image_name]
        fullscreen_row_df = filtered_df[filtered_df[st.session_state.ORIGINAL_FILENAME_COLUMN] == fullscreen_image_name_original]

        if not fullscreen_row_df.empty:
            fullscreen_row = fullscreen_row_df.iloc[0]
            age_group_val = fullscreen_row.get('age_group')
            folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(str(age_group_val).lower())
            fullscreen_image_path = None

            if folder_name_in_zip and folder_name_in_zip in image_folders_dict:
                 fullscreen_image_path = image_folders_dict[folder_name_in_zip].get(fullscreen_image_name)

            with col1:
                if fullscreen_image_path and os.path.exists(fullscreen_image_path):
                    st.image(fullscreen_image_path, caption=f"{fullscreen_image_name} (ID: {fullscreen_row.get('ID', 'N/A')})", use_column_width=True)
                else:
                    st.error("No se pudo encontrar la imagen para mostrar en pantalla completa.")
            
            with col2:
                st.subheader("Detalles de la Imagen")
                show_image_details(fullscreen_row.to_dict())
        else:
            # This case should ideally not happen if toggle_fullscreen was called on an image from filtered_df
            st.error("No se encontraron detalles para esta imagen en el conjunto filtrado actual.")

        if st.button("Cerrar Vista Detallada", key="close_fullscreen_btn"):
            st.session_state.fullscreen_image = None
            st.rerun()
        st.markdown("<hr style='margin-top: 20px; margin-bottom: 20px;'>", unsafe_allow_html=True)

    # Botón para descargar imágenes filtradas como ZIP
    if not filtered_df.empty:
        zip_buffer = create_downloadable_zip(filtered_df, image_folders_dict)
        if zip_buffer and zip_buffer.getbuffer().nbytes > 0:
            st.download_button(
                label="Descargar Imágenes Filtradas (ZIP)",
                data=zip_buffer,
                file_name="filtered_images.zip",
                mime="application/zip"
            )
        # else: # Avoid showing error if zip is empty due to no valid images
            # st.error("No se pudo crear el archivo ZIP o está vacío.")
    elif st.session_state.data_loaded: # Only show if data was loaded but filters result in empty
        st.info("No hay imágenes filtradas para descargar.")
