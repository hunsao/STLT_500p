import streamlit as st
from zipfile import ZipFile
import os
import json
import shutil
from PIL import Image # No se usa explícitamente, pero st.image puede depender de ella
import re
import pandas as pd
import io
import base64
import time

from st_aggrid import AgGrid

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
# from google_auth_httplib2 import Request # Not explicitly used, http is built directly
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp # Asegúrate de que esta librería esté instalada

st.set_page_config(layout="wide")

EXPECTED_GROUP_FOLDERS = {
    "older": "OLD",
    "young": "YOUNG",
    "middle-aged": "MIDDLE-AGE",
    "person": "PERSON"
}

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df_results = None
    st.session_state.image_folders = {}
    st.session_state.group_filter = "Todos"
    st.session_state.search_term = ""
    st.session_state.ORIGINAL_FILENAME_COLUMN = "filename"
    st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN = "filename_actual_jpg"
    st.session_state.current_page = 1 # Para paginación de imágenes
    st.session_state.images_per_page_display = 50 # Para paginación de imágenes

# --- Funciones Cacheadas ---
@st.cache_data()
def count_observations(df, category_column_name, options, is_activity_filter=False):
    if df is None or df.empty:
        return {option: 0 for option in options}
    if is_activity_filter:
        return {option: df['prompt'].str.contains(option, case=False, na=False).sum() for option in options}
    elif category_column_name in df.columns:
        if df[category_column_name].apply(lambda x: isinstance(x, list)).any():
             return {option: df[category_column_name].apply(lambda x: option in x if isinstance(x, list) else False).sum() for option in options}
        return {option: df[df[category_column_name].astype(str) == str(option)].shape[0] for option in options}
    return {option: 0 for option in options}

@st.cache_data()
def get_sorted_options(_df_results, category_key, options): # _df_results para indicar que su contenido importa para la caché
    # Usar st.session_state.df_results directamente aquí puede ser problemático si cambia
    # Es mejor pasar el df como argumento para que la caché funcione correctamente
    # Sin embargo, las opciones de filtro se basan en el df completo, no el filtrado.
    df_full = st.session_state.get('df_results_for_filters_options', pd.DataFrame()) # Usar una copia para opciones
    if df_full.empty:
        return []

    column_name_for_counting = category_key
    is_activity = False
    if category_key == 'activities':
        column_name_for_counting = 'prompt'
        is_activity = True
    elif category_key in ['assistive_devices', 'digital_devices']: # Ya son nombres de columna
        pass
    # Añadir más mapeos si es necesario

    counts = count_observations(df_full, column_name_for_counting, options, is_activity_filter=is_activity)
    options_with_count = sorted([(option, count) for option, count in counts.items()], key=lambda x: x[1], reverse=True)
    return [f"{option} ({count})" for option, count in options_with_count if count > 0]

@st.cache_data(max_entries=1) # Solo necesitamos una copia del zip en memoria a la vez
def create_downloadable_zip(_filtered_df, _image_folders_dict): # Cachear la creación del ZIP
    zip_buffer = io.BytesIO()
    actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN
    original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN

    with ZipFile(zip_buffer, 'w') as zip_file:
        for _, row in _filtered_df.iterrows():
            image_name_for_path = row.get(actual_fn_col)
            # image_name_original_for_zip = row.get(original_fn_col) # Usar el actual para el nombre en el zip también
            age_group = row.get('age_group')

            if image_name_for_path is None or age_group is None:
                # st.warning(f"Datos incompletos para ZIP: ID {row.get('ID', 'N/A')}") # Evitar st.write en funciones cacheadas
                print(f"Advertencia (ZIP): Datos incompletos para ID {row.get('ID', 'N/A')}")
                continue

            folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(str(age_group).lower())
            if not folder_name_in_zip:
                print(f"Advertencia (ZIP): Grupo de edad '{age_group}' no mapeado para ID {row.get('ID', 'N/A')}")
                continue

            current_group_images = _image_folders_dict.get(folder_name_in_zip)
            if not current_group_images:
                print(f"Advertencia (ZIP): No hay imágenes para grupo {folder_name_in_zip}")
                continue

            image_path_on_disk = current_group_images.get(image_name_for_path)
            if image_path_on_disk and os.path.exists(image_path_on_disk):
                zip_file.write(image_path_on_disk, os.path.join(folder_name_in_zip, image_name_for_path))
            else:
                print(f"Advertencia (ZIP): Imagen no encontrada en disco '{image_name_for_path}' en '{folder_name_in_zip}'")
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

        if hasattr(authed_http, 'timeout'):
             authed_http.timeout = 120
        else:
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

def list_files_in_folder(service, folder_id, retries=3): # No cachear, puede cambiar
    for attempt in range(retries):
        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents", fields="files(id, name)"
            ).execute()
            return results.get('files', [])
        except HttpError as error:
            st.error(f"Error al listar archivos (intento {attempt+1}): {error}")
            if attempt < retries - 1: time.sleep(5)
            else: raise

def download_file_from_google_drive(service, file_id, dest_path, retries=3): # No cachear, es una acción
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            with io.FileIO(dest_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024*5)
                done = False
                while not done:
                    status, done = downloader.next_chunk(num_retries=2)
            return
        except Exception as e:
            st.error(f"Error al descargar archivo (intento {attempt+1}): {str(e)}")
            if attempt < retries - 1: time.sleep(5)
            else: raise

def extract_zip(zip_path, extract_to_relative):
    abs_extract_to = os.path.abspath(extract_to_relative)
    if os.path.exists(abs_extract_to):
        shutil.rmtree(abs_extract_to, ignore_errors=True)
    try:
        os.makedirs(abs_extract_to, exist_ok=True)
    except Exception as e_mkdir:
        st.error(f"No se pudo crear dir de extracción '{abs_extract_to}': {e_mkdir}")
        raise

    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            # Optional: Check zip structure if needed before extraction
            # namelist = zip_ref.namelist()
            # if not any(name.startswith('data/') for name in namelist if '/' in name):
            #      st.warning("Advertencia: ZIP no parece tener carpeta 'data/' en la raíz.")
            zip_ref.extractall(path=abs_extract_to)
        # st.success(f"Archivos extraídos en: {abs_extract_to}") # Menos verbose
    except Exception as e:
        st.error(f"Error al extraer ZIP a '{abs_extract_to}': {str(e)}")
        raise

@st.cache_data() # Función simple, cachear es opcional pero inofensivo
def extract_folder_id(url):
    match = re.search(r'folders/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

@st.cache_data() # Cachear la lectura de imágenes de una carpeta
def read_images_from_folder_cached(abs_folder_path):
    images = {}
    if not (os.path.exists(abs_folder_path) and os.path.isdir(abs_folder_path)):
        # st.warning(f"Carpeta de imágenes no existe: {abs_folder_path}") # Evitar st.write
        print(f"Advertencia (read_images): Carpeta no existe {abs_folder_path}")
        return images
    try:
        filenames_on_disk = sorted(os.listdir(abs_folder_path), key=natural_sort_key)
    except Exception as e:
        # st.error(f"Error listando dir {abs_folder_path}: {e}") # Evitar st.write
        print(f"Error (read_images): Listando dir {abs_folder_path}: {e}")
        return images

    for filename_on_disk in filenames_on_disk:
        if filename_on_disk.lower().endswith((".jpg", ".jpeg")):
            images[filename_on_disk] = os.path.join(abs_folder_path, filename_on_disk)
    return images

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

def toggle_fullscreen(image_name_original_df):
    st.session_state.fullscreen_image = None if st.session_state.get('fullscreen_image') == image_name_original_df else image_name_original_df

def get_default(category_key):
    return st.session_state.get(f"multiselect_{category_key}", [])

@st.cache_data()
def get_unique_list_items(_df, column_name): # Pasar df para que la caché dependa de él
    if column_name in _df.columns:
        all_items = []
        for item_list in _df[column_name].dropna():
            items_to_add = []
            if isinstance(item_list, list): items_to_add = item_list
            elif isinstance(item_list, str):
                try:
                    evaluated_item = eval(item_list)
                    if isinstance(evaluated_item, list): items_to_add = evaluated_item
                    else: items_to_add = [str(item_list)]
                except: items_to_add = [str(item_list)]
            else: items_to_add = [str(item_list)]
            all_items.extend(map(str, items_to_add))
        return sorted(list(set(all_items)))
    return []

@st.cache_data()
def get_unique_objects_with_counts(_df, column_name): # Pasar df para que la caché dependa de él
    item_counts = {}
    if column_name not in _df.columns: return {}
    for entry in _df[column_name].dropna():
        items_to_process = []
        if isinstance(entry, list): items_to_process = entry
        elif isinstance(entry, str):
            try:
                evaluated_entry = eval(entry)
                if isinstance(evaluated_entry, list): items_to_process = evaluated_entry
                # else: items_to_process = [entry] # Si se quieren contar strings no-lista
            except: items_to_process = [entry] # Si se quieren contar strings no-lista
        
        for item in items_to_process:
            item_str = str(item)
            item_counts[item_str] = item_counts.get(item_str, 0) + 1
    return dict(sorted(item_counts.items(), key=lambda item: item[1], reverse=True))

# --- Fin Funciones Cacheadas ---

st.markdown("<h1 style='text-align: center; color: white;'>AGEAI: Imágenes y Metadatos. v30 (Optimizado)</h1>", unsafe_allow_html=True)

# Instrucciones (sin cambios, se puede colapsar por defecto)
with st.expander("📋 Instrucciones para el archivo ZIP", expanded=not st.session_state.data_loaded):
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

if 'categories' not in st.session_state:
    st.session_state.categories = {
        "gender": [], 
        "race": [], 
        "activities": ['sleeping',
         'napping',
         'falling sleep',
         'waking up',
         'relaxing',
         'being sick in bed',
         'in a medical bed',
         'sick',
         'recovering',
         'lying in bed',
         'eating',
         'getting drunk',
         'using narcotics',
         'dinning',
         'having a snack',
         'taking a shower',
         'washing hand',
         'in evening chores',
         'washing face',
         'shaving',
         'with a facial cleansing mask',
         'visiting the doctor',
         'receiving personal care',
         'pacient',
         'at the hairdresser',
         'taking medicines',
         "in the doctor's waiting-room",
         'monitoring blood pressure',
         'having a massage',
         'with their couple',
         'programmer',
         'executive manager',
         'lawyer',
         'scientific',
         'politician',
         'cashier',
         'driver',
         'school teacher',
         'housekeeper',
         'janitor',
         'taking a lunch break',
         'after work',
         'during a work pause',
         'resting after lunch break',
         'in a job fair',
         'looking for a job',
         'at the work place',
         'in a job interview',
         'in a performance evaluation',
         'taking a course',
         'at school',
         'at university',
         'taking exams',
         'taking oral exams',
         'doing homework',
         'preparing for a test',
         'preparing a presentation',
         'studying',
         'completing assignments',
         'doing an internship',
         'on a job training',
         'as a trainee',
         'doing work research',
         'doing an unpaid internship',
         'taking a break from studying',
         'taking a break at the university',
         'in the university canteen',
         'in the cafeteria',
         'leaving university',
         'on fitness classes',
         'on a science fair',
         'on book club',
         'on charity activities',
         'on fund raising activities',
         'attending a webinar',
         'receiving grades',
         'in a study group',
         'teaching peers',
         'researching',
         'on driving lesson',
         'on language courses',
         'on artistic courses',
         'on typing courses',
         'on self-taught activities',
         'doing housework',
         'doing home chores',
         'doing family routines',
         'on domestic duties',
         'doing home rituals',
         'cooking',
         'preparing coffee',
         'heating up some meals',
         'baking',
         'preparing snacks',
         'washing dishes',
         'drying dishes',
         'storing dishes',
         'clearing the table',
         'arranging dishes in the cabinet',
         'arranging food cabinets',
         'storing food in the refrigerator',
         'preserving food',
         'arranging the grocery shopping',
         'serving food',
         'doing house cleaning',
         'dusting',
         'vacuuming',
         'cleaning the kitchen',
         'cleaning the bathroom',
         'cleaning the garden',
         'mowing the lawn',
         'pruning a tree',
         'painting the fence',
         'raking the leaves',
         'heating the dwelling',
         'turns on the heating',
         'lights the fireplace',
         'keeping warm at home',
         'sitting in front of fireplace',
         'arranging household goods',
         'arranging tools',
         'arranging threads',
         'arranging frames',
         'setting the table',
         'recycling',
         'sorts waste',
         'recycling glasses',
         'taking out the trash',
         'recycling paper',
         'closing blinds',
         'opening courtains',
         'opens a door',
         'closes a window',
         'locking the door',
         'doing laundry',
         'whasing by hand',
         'soaking clothes',
         'folding sheets',
         'sorting of laundry',
         'ironing sheets',
         'puts laundry in drawer',
         'folding cloths',
         'darning socks',
         'making a hem',
         'cleaning shoes',
         'gardening',
         'picking flowers',
         'fertilizing the garden',
         'collecting rose hips',
         'ploughing',
         'bee-keeping',
         'feeding the chicken',
         'horse grooming',
         'feeding rabbits',
         'tending hens',
         'caring for pets',
         'petting a dog',
         'peeting a cat',
         'feeding a cat',
         'feeding a dog',
         'walking the dog',
         'in the forest with the dog',
         'training the dog',
         'adding insulation to walls',
         'installing electricity',
         'puts up drain-pipes',
         'restoring a kitchen',
         'painting walls',
         'repairing the dwelling',
         'installing light fittings',
         'opening a blocked-up sink',
         'tearing down wardrobes',
         'tiling the wall',
         'fixing and maintaining tools',
         'changing electric bulbs',
         'repairing a lamp',
         'loading batteries',
         'cleaning fans',
         'maintaining the vehicle',
         'chaging tyres on the car',
         'repairing the motorcycle',
         'cleaning the bycicle',
         "changing car's oil",
         'bought a present',
         'bought plants for the garden',
         'bought snack food from a kiosk',
         'fuelling a motor vehicle',
         'inspecting a car at a car showroom',
         'looked at an apartment for sale',
         'looked at clothes',
         'purchasing medicines',
         'purchasing tickets for the cinema',
         'tried on clothes in a shop',
         'was at a food store',
         'was at estate agents',
         'was at the market',
         'at car inspection centre',
         'doing the check-in to hotel',
         'picking up a package from the post office',
         'had oil change and car greased in a garage',
         'withdrawing money from cash machine',
         'waiting in the line for paying',
         'using bank services',
         'ordering a pizza',
         'planning a journey',
         'planning a party',
         'plalnning painting the walls',
         'babysitting',
         'supervising children in the playground',
         'changing nappies',
         "combed child's hair",
         'holding a baby',
         'checking homework',
         'playing games with the children',
         'read a story to the children',
         'attending school celebration',
         'attending a school concert',
         'providing physical care of an adult household member',
         "combed an adult's hair",
         'feeding an adult',
         'preparing the medicine for an adult',
         'talking with an adult with  alzheimer',
         'teaching how to use a computer',
         'teaching how to use a smartphone',
         'playing games with a household member',
         'entertaining family members',
         'doing administrative computer work',
         'in a council meeting',
         'volunteering',
         'coaching sports',
         'donating blood',
         'helping to construct',
         'helping to repair',
         'making a toy for a kid',
         'helping with milking',
         'helping tending a cattle',
         'helping to clean the office',
         'helping calves',
         'helping in the farm',
         'doing unpaid childminding',
         'minding children',
         'visiting somebody at the hospital',
         'giving mental support to a friend',
         'visiting a friend a home',
         'lendng money',
         'helping a neighbour',
         'helping a relative',
         'attending meetings',
         'political party meeting',
         'scout camp',
         'attended mass',
         'listening a recording of a religious ceremony',
         'reading the bible',
         'participating in baptism ceremony',
         'participating in ceremonies of baptism, confirmation, first communion',
         'paying respects at graves',
         'tending flowers on a grave',
         'cleaning the gravestone',
         'participating in community events',
         'voting',
         'witness in court',
         'in a family meeting',
         'arguing with a sister',
         'talking with a brother',
         'saying goodbye to the family',
         'teasing a brother',
         'hosting visitors',
         'had a visitor',
         'visited my friend',
         'having guests',
         'in a party',
         'in a wedding',
         'in an anniversarie',
         'in a work party',
         'having conversations',
         'having a video call',
         'calling the landlord',
         'listening to messages',
         'doing a phone call',
         'sending messages',
         'checking the email',
         'writing cards',
         'reading messages',
         'spending time on social media',
         'on facebook',
         'on linkedin',
         'on tiktok',
         'on whatsapp',
         'in a social gathering',
         'having conversations with neighbours',
         'having conversations in a cafeteria',
         'outdoors with friends',
         'at a pub with a friend',
         'in a movie night',
         'watching movies in cinema',
         'in a movie club',
         'in the line for  the cinema',
         'attending a concert',
         'watching a play',
         'watching a dance show',
         'watching street show',
         'viewing art collections',
         'visiting town museum',
         'visiting art gallery',
         'visiting  a cultural site',
         'visit a castle',
         'in a library',
         'borrowing a book from the library',
         'searching for a book in a library',
         'participating in sports events',
         'attending a boxing match',
         'attending a car race',
         'attending a horse racing',
         'attending a football match',
         'going to botanical gardens',
         'walking around zoological garden',
         'admiring plants in a botanical garden',
         'visiting a natural reserve',
         'taking a break',
         'admiring a flower',
         'being bored',
         'cooling off',
         'did not do anything special',
         'gathering strength',
         'just letting the time pass',
         'looking out through the window',
         'just listening to birds',
         'killing time',
         'laying in bed after lunch',
         'lazing around',
         'lounging',
         'lying in sun',
         'philosophising',
         'at the beach',
         'taking it easy',
         'trying to get to know myself',
         'waiting for the children to come',
         'waiting for guests to arrive',
         'walking in the house',
         'watching through the window',
         'going for walks',
         'walking',
         'hiking',
         'strolling in town',
         'taking a nature walk',
         'running for exercise',
         'jogging',
         'doing exercise',
         'running',
         'riding a bike',
         'skiing',
         'skating'], 
        "emotion": [],
        "personality": ["Conscientiousness", "Openness", "Extraversion", 
                "Agreeableness", "Neuroticism"], 
        "position": [], 
        "person_count": [], 
        "location": [],
    }
    st.session_state.df_results_for_filters_options = pd.DataFrame() # Para que get_sorted_options tenga de dónde leer

# --- BLOQUE DE CARGA DE DATOS ---
if not st.session_state.data_loaded:
    service = get_drive_service()
    if service is None:
        st.error("No se pudo conectar con Google Drive.")
        st.stop()

    # ... (lógica de selección de archivo de Drive sin cambios) ...
    folder_url = st.text_input("Ingrese el enlace de la carpeta de Google Drive:", value=st.session_state.get("gdrive_folder_url", ""))
    if folder_url: st.session_state.gdrive_folder_url = folder_url
    folder_id = extract_folder_id(folder_url)

    if not folder_id:
        if folder_url: st.warning("Por favor, ingrese un enlace de carpeta de Google Drive válido.")
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
        
    selected_file_name = st.selectbox("Selecciona el archivo ZIP:", list(file_options.keys()), 
                                      index=st.session_state.get("selected_zip_index", 0))
    if selected_file_name: 
        st.session_state.selected_zip_index = list(file_options.keys()).index(selected_file_name)


    if selected_file_name and st.button("Confirmar selección y Cargar Datos"):
        with st.spinner("Descargando y procesando archivo ZIP..."):
            file_id = file_options[selected_file_name]
            temp_zip_path = "temp_data.zip"
            temp_extract_path = "extracted_data_content" # Relativa al script
            abs_temp_extract_path = os.path.abspath(temp_extract_path)
            st.session_state.abs_temp_extract_path = abs_temp_extract_path # Guardar para referencia

            try:
                download_file_from_google_drive(service, file_id, temp_zip_path)
                extract_zip(temp_zip_path, temp_extract_path)
            except Exception as e:
                st.error(f"Fallo en descarga o extracción: {e}")
                # --- INICIO LIMPIEZA Y STOP ---
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip:
                        st.warning(f"No se pudo eliminar temp_zip_path: {e_clean_zip}")
                abs_temp_extract_path = st.session_state.get("abs_temp_extract_path") # Recuperar si ya se guardó
                if abs_temp_extract_path and os.path.exists(abs_temp_extract_path):
                    try:
                        shutil.rmtree(abs_temp_extract_path, ignore_errors=True) # Usar la absoluta aquí
                    except Exception as e_clean_extract:
                        st.warning(f"No se pudo eliminar abs_temp_extract_path: {e_clean_extract}")
                st.stop()

            abs_data_folder_path = os.path.join(abs_temp_extract_path, 'data')
            if not os.path.exists(abs_data_folder_path):
                st.error(f"Carpeta 'data/' no encontrada en '{abs_temp_extract_path}'. Verifique estructura del ZIP.")
                # --- INICIO LIMPIEZA Y STOP ---
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip:
                        st.warning(f"No se pudo eliminar temp_zip_path: {e_clean_zip}")
                # abs_temp_extract_path ya está definida y es la carpeta base extraída
                if os.path.exists(abs_temp_extract_path):
                    try:
                        shutil.rmtree(abs_temp_extract_path, ignore_errors=True)
                    except Exception as e_clean_extract:
                        st.warning(f"No se pudo eliminar abs_temp_extract_path: {e_clean_extract}")
                # --- FIN LIMPIEZA Y STOP ---
                st.stop()

            # Cargar imágenes
            st.session_state.image_folders = {}
            loaded_any_images = False
            for age_group_key, folder_name_in_zip in EXPECTED_GROUP_FOLDERS.items():
                abs_current_img_folder_path = os.path.join(abs_data_folder_path, folder_name_in_zip)
                # Usar la función cacheada para leer imágenes
                images = read_images_from_folder_cached(abs_current_img_folder_path)
                if images:
                    st.session_state.image_folders[folder_name_in_zip] = images
                    loaded_any_images = True
            
            if not loaded_any_images:
                st.error("No se cargaron imágenes. Verifique estructura del ZIP y nombres de carpetas.")
                # --- INICIO LIMPIEZA Y STOP ---
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip:
                        st.warning(f"No se pudo eliminar temp_zip_path: {e_clean_zip}")
                if os.path.exists(abs_temp_extract_path):
                    try:
                        shutil.rmtree(abs_temp_extract_path, ignore_errors=True)
                    except Exception as e_clean_extract:
                        st.warning(f"No se pudo eliminar abs_temp_extract_path: {e_clean_extract}")
                # --- FIN LIMPIEZA Y STOP ---
                st.stop()

            # Cargar y PROCESAR DataFrame
            csv_files = [f for f in os.listdir(abs_data_folder_path) if f.endswith('.csv')]
            if not csv_files:
                st.error(f"No se encontró CSV en '{abs_data_folder_path}'.")
                # --- INICIO LIMPIEZA Y STOP ---
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip:
                        st.warning(f"No se pudo eliminar temp_zip_path: {e_clean_zip}")
                if os.path.exists(abs_temp_extract_path):
                    try:
                        shutil.rmtree(abs_temp_extract_path, ignore_errors=True)
                    except Exception as e_clean_extract:
                        st.warning(f"No se pudo eliminar abs_temp_extract_path: {e_clean_extract}")
                # --- FIN LIMPIEZA Y STOP ---
                st.stop()

            csv_file_path = os.path.join(abs_data_folder_path, csv_files[0])
            try:
                df = pd.read_csv(csv_file_path)

                # --- PROCESAMIENTO DEL DF (HACERLO AQUÍ UNA VEZ) ---
                original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN
                actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN

                if original_fn_col in df.columns:
                    df[actual_fn_col] = df[original_fn_col].apply(
                        lambda x: x.rpartition('.')[0] + '.jpg' if isinstance(x, str) and x.lower().endswith('.png') else x
                    )
                elif actual_fn_col not in df.columns: # Si no existe ni el original para crearla, ni ella misma
                    st.error(f"Columnas de nombre de archivo '{original_fn_col}' o '{actual_fn_col}' no encontradas.")
                    raise ValueError("Faltan columnas de nombre de archivo")

                required_cols = [actual_fn_col, 'prompt', 'age_group', 'ID'] # 'filename' original ya no es crítico si actual existe
                if original_fn_col in df.columns and original_fn_col not in required_cols:
                    required_cols.append(original_fn_col) # Si existe, mantenerla requerida para info

                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    st.error(f"Columnas obligatorias faltantes: {', '.join(missing_cols)}")
                    raise ValueError("Faltan columnas obligatorias")
                
                df.dropna(subset=[col for col in required_cols if col in df.columns], inplace=True) # Dropna solo de las que existen
                
                if 'age_group' in df.columns:
                    df['age_group'] = df['age_group'].astype(str).str.lower()
                
                st.session_state.df_results = df # Guardar el DF PROCESADO
                st.session_state.df_results_for_filters_options = df.copy() # Copia para opciones de filtro

                # Poblar categorías para filtros (basado en el DF completo y procesado)
                category_keys_to_populate = {
                    "gender": "gender", 
                    "race": "race", 
                    "emotion": "emotion",
                    #"personality": "personality", 
                    "position": "position",
                    "person_count": "person_count", 
                    "location": "location",
                }
                for cat_key, df_col_name in category_keys_to_populate.items():
                    if df_col_name in df.columns:
                        # Usar el df completo para obtener todas las opciones únicas
                        st.session_state.categories[cat_key] = get_unique_list_items(df, df_col_name)
                        #if cat_key == 'personality' and st.session_state.categories[cat_key]:
                        #    st.session_state.categories[cat_key] = [p.lower() for p in st.session_state.categories[cat_key]]
                            # La columna del DF ya se modificará si es necesario en el filtrado, o se asume que ya está lower.
                    else:
                        st.session_state.categories[cat_key] = []
                
                st.session_state.categories['activities'] = [] # Sin opciones predefinidas por ahora

                st.session_state.data_loaded = True
                st.success("Datos cargados y procesados.")
                if os.path.exists(temp_zip_path): # Limpiar ZIP descargado si todo fue bien
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip_success:
                        st.warning(f"No se pudo eliminar temp_zip_path (tras éxito): {e_clean_zip_success}")
                st.rerun()

            except Exception as e_df:
                st.error(f"Error procesando DataFrame o cargando datos: {e_df}")
                # --- INICIO LIMPIEZA Y STOP ---
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e_clean_zip:
                        st.warning(f"No se pudo eliminar temp_zip_path: {e_clean_zip}")
                # abs_temp_extract_path debería estar definida si la extracción ocurrió
                if 'abs_temp_extract_path' in st.session_state and os.path.exists(st.session_state.abs_temp_extract_path):
                    try:
                        shutil.rmtree(st.session_state.abs_temp_extract_path, ignore_errors=True)
                    except Exception as e_clean_extract:
                        st.warning(f"No se pudo eliminar abs_temp_extract_path: {e_clean_extract}")
                elif 'abs_temp_extract_path' not in st.session_state: # Si falló antes de guardarlo
                    local_abs_extract_path = os.path.abspath("extracted_data_content") # Reconstruir localmente
                    if os.path.exists(local_abs_extract_path):
                        try:
                            shutil.rmtree(local_abs_extract_path, ignore_errors=True)
                        except Exception as e_clean_extract_local:
                            st.warning(f"No se pudo eliminar (local) abs_temp_extract_path: {e_clean_extract_local}")
                # --- FIN LIMPIEZA Y STOP ---
                st.stop()


# --- FIN BLOQUE DE CARGA DE DATOS ---

else: # --- INICIO BLOQUE DASHBOARD (DATOS CARGADOS) ---
    df_results = st.session_state.df_results # Este es el DF ya procesado
    image_folders_dict = st.session_state.image_folders

    # Definir nombres de columna para usar en este bloque
    actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN
    original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN
    
    st.sidebar.header("Filtrar imágenes")

    # Group filter
    group_options = ["Todos"] + list(EXPECTED_GROUP_FOLDERS.keys())
    try: current_group_filter_index = group_options.index(st.session_state.group_filter)
    except ValueError: current_group_filter_index = 0
    group_filter = st.sidebar.selectbox("Seleccionar Grupo", group_options, index=current_group_filter_index,
                                        format_func=lambda x: x.replace("-", " ").title())
    if st.session_state.group_filter != group_filter: # Reset page if filter changes
        st.session_state.group_filter = group_filter
        st.session_state.current_page = 1 
        # st.rerun() # Selectbox ya causa rerun

    filtered_df = df_results.copy()
    if group_filter != "Todos":
        filtered_df = filtered_df[filtered_df['age_group'].astype(str).str.lower() == group_filter.lower()]

    # Age Range Filter
    if 'age' in df_results.columns:
        age_ranges = sorted(df_results['age'].astype(str).unique().tolist())
        selected_age_ranges_display = st.sidebar.multiselect(
            "Seleccionar Age Range",
            get_sorted_options(st.session_state.df_results_for_filters_options, 'age', age_ranges),
            default=get_default("age_range"), key="multiselect_age_range"
        )
        if st.session_state.get("multiselect_age_range", []) != selected_age_ranges_display: # Reset page
             st.session_state.current_page = 1
        # st.session_state.multiselect_age_range = selected_age_ranges_display # Ya lo hace el widget
        selected_age_ranges = [age.split(" (")[0] for age in selected_age_ranges_display]
        if selected_age_ranges:
            filtered_df = filtered_df[filtered_df['age'].astype(str).isin(selected_age_ranges)]


    if st.sidebar.button("Resetear Filtros"):
        st.session_state.group_filter = "Todos"
        st.session_state.search_term = ""
        st.session_state.current_page = 1
        for key in list(st.session_state.keys()):
            if key.startswith('multiselect_'): st.session_state[key] = []
        st.rerun()

    # Dynamic category filters
    for category_key, options in st.session_state.categories.items():
        if not options and category_key != 'activities': continue
        filter_title = f"Seleccionar {category_key.replace('_', ' ').title()}"
        
        current_selection = get_default(category_key)
        selected_display = st.sidebar.multiselect(
            filter_title,
            get_sorted_options(st.session_state.df_results_for_filters_options, category_key, options),
            default=current_selection, key=f"multiselect_{category_key}"
        )
        if current_selection != selected_display: # Reset page
            st.session_state.current_page = 1

        selected_values = [opt.split(" (")[0] for opt in selected_display]
        if selected_values:
            if category_key == "activities":
                pattern = '|'.join(map(re.escape, selected_values))
                filtered_df = filtered_df[filtered_df['prompt'].str.contains(pattern, case=False, na=False)]
            else: # Otros filtros categóricos
                df_column_name = category_key # Asumiendo mapeo directo
                if df_results[df_column_name].apply(lambda x: isinstance(x, list)).any():
                    filtered_df = filtered_df[filtered_df[df_column_name].apply(
                        lambda L: isinstance(L, list) and any(item in selected_values for item in L))]
                else: # Asegurar que la comparación se haga con la columna pre-procesada (ej. lowercased personality)
                    # Si 'personality' se hizo lowercase en el df_results original, la comparación ya es correcta
                    if category_key == 'personality': # Ejemplo: si la columna original no se modificó
                         filtered_df = filtered_df[filtered_df[df_column_name].astype(str).str.lower().isin([v.lower() for v in selected_values])]
                    else:
                         filtered_df = filtered_df[filtered_df[df_column_name].astype(str).isin(selected_values)]


    # Object filters
    object_columns_map = {"objects": "Objetos", "assistive_devices": "Dispositivos de Asistencia", "digital_devices": "Dispositivos Digitales"}
    for col_name, display_name in object_columns_map.items():
        if col_name in df_results.columns:
            unique_items_with_counts = get_unique_objects_with_counts(st.session_state.df_results_for_filters_options, col_name)
            
            current_selection = get_default(col_name)
            selected_items_display = st.sidebar.multiselect(
                f"Seleccionar {display_name}",
                [f"{obj} ({count})" for obj, count in unique_items_with_counts.items()],
                default=current_selection, key=f"multiselect_{col_name}"
            )
            if current_selection != selected_items_display: # Reset page
                st.session_state.current_page = 1

            selected_items_values = [item.split(" (")[0] for item in selected_items_display]
            if selected_items_values:
                def check_item_presence(entry, items_to_find):
                    if pd.isna(entry): return False
                    current_list = []
                    if isinstance(entry, list): current_list = entry
                    elif isinstance(entry, str):
                        try:
                            evaluated = eval(entry)
                            if isinstance(evaluated, list): current_list = evaluated
                        except: pass
                    return any(str(item_to_find) in map(str, current_list) for item_to_find in items_to_find)
                filtered_df = filtered_df[filtered_df[col_name].apply(lambda x: check_item_presence(x, selected_items_values))]

    # Buscador General
    st.sidebar.header("Buscador General")
    search_columns_options = ['Todas las Columnas'] + df_results.columns.tolist()
    default_search_column = st.session_state.get('selected_column_search', 'Todas las Columnas')
    if default_search_column not in search_columns_options: default_search_column = 'Todas las Columnas'
    
    selected_column_search = st.sidebar.selectbox("Buscar en Variable", search_columns_options, 
                                                  index=search_columns_options.index(default_search_column))
    
    search_term_input = st.sidebar.text_input(f"Término de Búsqueda", value=st.session_state.get("search_term", ""))

    if st.session_state.search_term != search_term_input or st.session_state.get('selected_column_search') != selected_column_search:
        st.session_state.search_term = search_term_input
        st.session_state.selected_column_search = selected_column_search
        st.session_state.current_page = 1 # Reset page

    if st.session_state.search_term:
        term = st.session_state.search_term
        if selected_column_search == 'Todas las Columnas':
            mask = filtered_df.apply(lambda row: row.astype(str).str.contains(term, case=False, na=False).any(), axis=1)
            filtered_df = filtered_df[mask]
        else:
            filtered_df = filtered_df[filtered_df[selected_column_search].astype(str).str.contains(term, case=False, na=False)]
    
    st.session_state.filtered_df_count = len(filtered_df) # Guardar para paginación

    # --- Display Area ---
    st.markdown("---")
    st.subheader(f"Resultados Filtrados: {st.session_state.filtered_df_count} imágenes")

    # ... (Display applied filters summary - sin cambios, pero podría quitarse si es muy largo) ...
    
    if not filtered_df.empty:
        gb = AgGrid(filtered_df, height=300, fit_columns_on_grid_load=True, allow_unsafe_jscode=True, enable_enterprise_modules=False)
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button("Descargar Tabla Filtrada (CSV)", csv, "filtered_data.csv", "text/csv")
    else:
        st.info("La tabla está vacía con los filtros actuales.")


    st.markdown("---")
    st.subheader("Visualización de Imágenes")

    if 'fullscreen_image' not in st.session_state: st.session_state.fullscreen_image = None

    if st.session_state.fullscreen_image is None:
        if not filtered_df.empty:
            # --- PAGINACIÓN ---
            total_items = st.session_state.filtered_df_count
            items_per_page = st.session_state.images_per_page_display
            
            total_pages = (total_items + items_per_page - 1) // items_per_page
            if total_pages == 0: total_pages = 1 # Evitar división por cero si no hay items

            page_col1, page_col2 = st.columns([0.7, 0.3])
            with page_col1:
                 images_per_row = st.slider("Imágenes por fila", min_value=1, max_value=10, value=st.session_state.get("images_per_row_slider_val", 4), key="images_per_row_slider")
                 st.session_state.images_per_row_slider_val = images_per_row
            with page_col2:
                 st.session_state.current_page = st.number_input(f"Página (1-{total_pages})", 
                                                                  min_value=1, max_value=total_pages, 
                                                                  value=st.session_state.current_page, step=1,
                                                                  key="image_page_selector")

            start_idx = (st.session_state.current_page - 1) * items_per_page
            end_idx = start_idx + items_per_page
            paginated_df_view = filtered_df.iloc[start_idx:end_idx]
            # --- FIN PAGINACIÓN ---

            actual_fn_col = st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN
            original_fn_col = st.session_state.ORIGINAL_FILENAME_COLUMN

            for i in range(0, len(paginated_df_view), images_per_row):
                row_data_for_display = paginated_df_view.iloc[i:i+images_per_row]
                cols = st.columns(images_per_row) # Usar images_per_row que es fijo por página
                for col_idx, (df_idx, row) in enumerate(row_data_for_display.iterrows()):
                    image_name_actual = row.get(actual_fn_col)
                    image_name_original_df = row.get(original_fn_col, image_name_actual) # Fallback
                    age_group_val = row.get('age_group')

                    if image_name_actual and age_group_val:
                        folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(str(age_group_val).lower())
                        image_path_on_disk = None
                        if folder_name_in_zip and folder_name_in_zip in image_folders_dict:
                            image_path_on_disk = image_folders_dict[folder_name_in_zip].get(image_name_actual)

                        if image_path_on_disk and os.path.exists(image_path_on_disk): # os.path.exists es rápido para local
                            try:
                                cols[col_idx].image(image_path_on_disk, caption=f"{image_name_original_df}\nID: {row.get('ID', 'N/A')}", use_column_width=True)
                                if cols[col_idx].button(f"Detalles", key=f"btn_detail_{df_idx}"): # Usar df_idx para unicidad
                                    toggle_fullscreen(image_name_original_df) # Se usa el original para buscar en DF
                                    st.rerun()
                            except Exception as e:
                                cols[col_idx].error(f"Error al cargar {image_name_actual}: {e}")
                        else:
                            cols[col_idx].warning(f"Img no hallada: {image_name_actual} (orig: {image_name_original_df})")
                    else:
                        cols[col_idx].caption(f"Faltan datos para imagen ID: {row.get('ID', 'N/A')}")
                st.markdown("<hr style='margin-top: 5px; margin-bottom: 5px;'>", unsafe_allow_html=True)
        else:
            st.info("No hay imágenes que coincidan con los filtros aplicados.")
    else: # Fullscreen mode
        col1, col2 = st.columns([3, 2])
        fullscreen_image_name_original_df = st.session_state.fullscreen_image # Este es el original del DF

        # Buscar en el DataFrame filtrado actual, o en el completo si es necesario
        # Es mejor buscar en el filtered_df porque es el contexto del usuario
        fullscreen_row_s = filtered_df[filtered_df[original_fn_col] == fullscreen_image_name_original_df]
        if fullscreen_row_s.empty: # Si no está en el filtrado, buscar en el completo
             fullscreen_row_s = df_results[df_results[original_fn_col] == fullscreen_image_name_original_df]

        if not fullscreen_row_s.empty:
            fullscreen_row = fullscreen_row_s.iloc[0]
            age_group_val = fullscreen_row.get('age_group')
            image_name_actual_for_fullscreen = fullscreen_row.get(actual_fn_col)
            
            folder_name_in_zip = EXPECTED_GROUP_FOLDERS.get(str(age_group_val).lower())
            fullscreen_image_path_on_disk = None

            if folder_name_in_zip and folder_name_in_zip in image_folders_dict and image_name_actual_for_fullscreen:
                 fullscreen_image_path_on_disk = image_folders_dict[folder_name_in_zip].get(image_name_actual_for_fullscreen)

            with col1:
                if fullscreen_image_path_on_disk and os.path.exists(fullscreen_image_path_on_disk):
                    st.image(fullscreen_image_path_on_disk, caption=f"{fullscreen_image_name_original_df} (ID: {fullscreen_row.get('ID', 'N/A')})", use_column_width=True)
                else:
                    st.error("No se pudo encontrar la imagen para pantalla completa.")
            with col2:
                st.subheader("Detalles de la Imagen")
                # Convertir toda la fila a string para evitar errores con tipos no serializables en show_image_details
                details_dict = {k: str(v) for k, v in fullscreen_row.to_dict().items()}
                # show_image_details(details_dict) # Tu función original
                for key, value in details_dict.items(): # Implementación directa
                    st.write(f"**{key}:** {value}")
        else:
            st.error("No se encontraron detalles para esta imagen.")

        if st.button("Cerrar Vista Detallada", key="close_fullscreen_btn"):
            st.session_state.fullscreen_image = None
            st.rerun()
        st.markdown("<hr style='margin-top: 10px; margin-bottom: 10px;'>", unsafe_allow_html=True)

    # Descarga de Imágenes ZIP
    if not filtered_df.empty:
        df_for_zip = filtered_df.drop_duplicates(subset=[st.session_state.ACTUAL_IMAGE_FILENAME_COLUMN, 'age_group'], keep='first')
        # La creación del ZIP se cachea, por lo que filtered_df y image_folders_dict deben ser argumentos
        # para que la caché se invalide si cambian.
        zip_buffer = create_downloadable_zip(filtered_df, image_folders_dict)
        if zip_buffer.getbuffer().nbytes > 0:
            st.download_button("Descargar Imágenes Filtradas (ZIP)", zip_buffer, "filtered_images.zip", "application/zip")
    elif st.session_state.data_loaded:
        st.info("No hay imágenes filtradas para descargar.")
