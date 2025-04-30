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
from streamlit import cache_data
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_httplib2 import Request
from googleapiclient.errors import HttpError

from googleapiclient.http import HttpRequest
from googleapiclient.http import build_http

http = build_http()
http.timeout = 120 

st.set_page_config(layout="wide")

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df_results = None
    st.session_state.images1 = None
    st.session_state.images2 = None
    st.session_state.group_filter = "Todos"  
    st.session_state.search_term = ""  

@st.cache_data()
def count_observations(df, category, options):
    if category in ['activities']:
        return {option: df['prompt'].str.contains(option, case=False, na=False).sum() for option in options}
    elif category == 'prompt':
        return {option: df[category].str.contains(option, case=False, na=False).sum() for option in options}
    else:
        return {option: df[df[category] == option].shape[0] for option in options}

@st.cache_data()
def get_sorted_options(df, category, options):
    if category in ['activities']:
        column = 'prompt'
    else:
        column = category
    
    counts = count_observations(df, category, options)
    options_with_count = sorted([(option, count) for option, count in counts.items()], key=lambda x: x[1], reverse=True)
    return [f"{option} ({count})" for option, count in options_with_count]

@st.cache_data(max_entries=1)
def create_downloadable_zip(filtered_df, images1, images2):
    zip_buffer = io.BytesIO()
    try:
        with ZipFile(zip_buffer, 'w') as zip_file:
            for _, row in filtered_df.iterrows():
                image_name = row.get('filename_jpg')  
                group_id = row.get('ID')
                
                if image_name is None:
                    st.error("No se encontró el nombre de la imagen en la fila.")
                    continue
                
                if group_id is None:
                    st.error("No se encontró el ID del grupo en la fila.")
                    continue
                
                if isinstance(image_name, str) and isinstance(group_id, str):
                    if group_id.startswith('a_'):
                        image_path = images1.get(image_name, None)
                        folder_name = 'NEUTRAL'
                    elif group_id.startswith('o_'):
                        image_path = images2.get(image_name, None)
                        folder_name = 'OLDER'
                    else:
                        st.warning(f"ID del grupo no coincide con ningún grupo conocido: {group_id}")
                        continue
                    
                    if image_path:
                        zip_file.write(image_path, os.path.join(folder_name, image_name))
                    else:
                        st.warning(f"No se encontró la imagen en el diccionario de imágenes: {image_name}")
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
            raise ValueError("La variable de entorno GOOGLE_SERVICE_ACCOUNT no está configurada")

        sa_json = base64.b64decode(encoded_sa).decode('utf-8')

        sa_dict = json.loads(sa_json)

        credentials = service_account.Credentials.from_service_account_info(
            sa_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

        service = build('drive', 'v3', credentials=credentials)
        return service
    except Exception as e:
        st.error(f"Error al obtener el servicio de Google Drive: {str(e)}")
        return None

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
            
class RequestWithTimeout(Request):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = 120  

def download_file_from_google_drive(service, file_id, dest_path, retries=3):
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(dest_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                #st.write(f'Download {int(status.progress() * 100)}%')
            
            fh.close()
            st.success(f"Archivo descargado correctamente")
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
        st.write(os.listdir(extract_to))
    except Exception as e:
        st.error(f"Error al extraer el archivo ZIP: {str(e)}")

@st.cache_data()
def extract_folder_id(url):
    """Extract the folder ID from a Google Drive URL."""
    match = re.search(r'folders/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    return None

############################################################################

def show_image_details(image_data):
    for key, value in image_data.items():
        st.write(f"**{key}:** {value}")

@st.cache_data(persist="disk")
def read_images_from_folder(folder_path):
    images = {}
    filenames = sorted(os.listdir(folder_path), key=natural_sort_key)
    for filename in filenames:
        if filename.lower().endswith((".jpg", ".jpeg")):
            image_path = os.path.join(folder_path, filename)
            images[filename] = image_path
    return images

# Natural sort function
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

@st.cache_data(persist="disk")
def read_dataframe_from_zip(zip_path):
    with ZipFile(zip_path, 'r') as zip_ref:
        if 'df_results.csv' in zip_ref.namelist():
            with zip_ref.open('df_results.csv') as csv_file:
                return pd.read_csv(io.BytesIO(csv_file.read()))
    return None

def toggle_fullscreen(image_name):
    if st.session_state.fullscreen_image == image_name:
        st.session_state.fullscreen_image = None
    else:
        st.session_state.fullscreen_image = image_name

def get_default(category):
    if category in st.session_state:
        return st.session_state[category]
    return []

if 'fullscreen_image' not in st.session_state:
    st.session_state.fullscreen_image = None

@st.cache_data(persist="disk")
def get_unique_list_items(df_results, category):
    if category in df_results.columns:
        all_items = df_results[category].dropna().tolist()
        
        # Convertir diccionarios a tuplas de valores para hacerlos hashables
        unique_items = set()
        for item in all_items:
            if isinstance(item, dict):
                item = tuple(sorted(item.items()))  # Convertir dict a tupla de pares clave-valor
            unique_items.add(item)
        
        return sorted(unique_items)
    return []

@st.cache_data()
def get_unique_objects(df, column_name):
    unique_objects = {}  # Usar un diccionario para almacenar el conteo
    for objects_list in df[column_name].dropna():
        if isinstance(objects_list, list):
            for item in objects_list:
                if item in unique_objects:
                    unique_objects[item] += 1
                else:
                    unique_objects[item] = 1
        elif isinstance(objects_list, str):
            try:
                objects = eval(objects_list)
                if isinstance(objects, list):
                    for item in objects:
                        if item in unique_objects:
                            unique_objects[item] += 1
                        else:
                            unique_objects[item] = 1
            except:
                pass

    sorted_objects = dict(sorted(unique_objects.items(), key=lambda item: item[1], reverse=True))
    return sorted_objects  

#############################################################################################################################
st.markdown("<h1 style='text-align: center; color: white;'>AGEAI: Imágenes y Metadatos. v31 3.1.25</h1>", unsafe_allow_html=True)

st.markdown("""
<details>
<summary>📋 Instrucciones para el archivo ZIP</summary>

<h3>📁 Estructura de Carpetas</h3>
Para que la aplicación funcione correctamente, el archivo ZIP debe contener:

<ul>
<li>archivo.zip
  <ul>
    <li>data/
      <ul>
        <li>NEUTRAL/
          <ul>
            <li>[imágenes .jpg o .jpeg]</li>
          </ul>
        </li>
        <li>OLDER/
          <ul>
            <li>[imágenes .jpg o .jpeg]</li>
          </ul>
        </li>
        <li>df_x.csv</li>
      </ul>
    </li>
  </ul>
</li>
</ul>

<h3>📁🖼️ Imágenes</h3>
<ul>
<li>Deben estar en formato .jpg o .jpeg</li>
<li>Los nombres de archivo deben coincidir con los listados en la columna <code>filename_jpg</code> del CSV</li>
<li>Deben estar organizadas en las carpetas correspondientes (NEUTRAL u OLDER)</li>
</ul>

<h3>📁⚠️ Notas Importantes</h3>
<ul>
<li>El CSV no debe tener valores nulos en las columnas obligatorias</li>
<li>La estructura de carpetas debe respetarse exactamente como se indica</li>
</ul>

</details>
""", unsafe_allow_html=True)
st.markdown(" ")

# Inicializar categorías fuera del bloque if/else
if 'categories' not in st.session_state:
    st.session_state.categories = {
        #"shot": ["full shot", "close-up shot", "medium shot"],
        "gender": ["male", "female", "not identified"],
        "race": ["asian", "white", "black", "hispanic", "other"],
        "activities" : [
            "sleeping",
            "being sick in bed",
            "eating",
            "grooming",
            "receiving personal care",
            "taking a bath",
            "at work",
            "taking a lunch break",
            "in a job fair",
            "taking a course",
            "doing homework",
            "doing an internship",
            "taking a break from studying",
            "attending extracurricular classes",
            "attending a webinar",
            "in a study group",
            "handling home tasks",
            "preparing food",
            "washing dishes",
            "storing food",
            "doing house cleaning",
            "cleaning the garden",
            "heating their home",
            "arranging household goods",
            "recycling",
            "doing home maintenance",
            "doing laundry",
            "ironing",
            "gardening",
            "caring for pets",
            "walking the dog",
            "constructing or renovating the house",
            "repairing the dwelling",
            "fixing and maintaining tools",
            "maintaining the vehicle",
            "shopping",
            "managing banking accounts",
            "planning shopping",
            "managing the household",
            "providing physical care and supervision of a child",
            "educating the child",
            "reading, playing, and talking with the child",
            "providing physical care of an adult household member",
            "offering childcare services",
            "providing support to an adult",
            "volunteering",
            "attending meetings",
            "engaging in religious activities",
            "Paying respects at graves",
            "participating in community events",
            "in a family meeting",
            "hosting guests at home",
            "in a party",
            "engaging in a discussion",
            "sending and receiving messages",
            "spending time on social media",
            "in a social gathering",
            "in a movie night",
            "attending theatre or live concerts",
            "viewing art collections",
            "in a library",
            "participating in sports events",
            "in a botanical garden",
            "taking a break",
            "going for a walk",
            "running for exercise",
            "riding a bike",
            "engaging in team sports",
            "engaging in fitness routines",
            "doing swimming and other water activities",
            "meditating",
            "engaging in productive exercise",
            "participating in sports-related activities",
            "engaging in visual arts",
            "amassing collectibles",
            "making handicraft products",
            "using computers",
            "searching for information online",
            "handling video game consoles",
            "engaging in smartphone games",
            "reading news",
            "reading books",
            "watching movies or videos",
            "listening to music or talk shows",
            "updating the time diary",
            "in the room where they sleep",
            "in the living room",
            "traveling",
            "traveling for work",
            "going to study locations",
            "going to shops and services",
            "traveling for family care",
            "moving to a new location"],
        "emotions_short": ["neutral", "positive", "negative"],
        #"personality_short": ["openness", "conscientiousness", "extraversion", "agreeableness", "neuroticism"],
        "personality_short": [item.lower() for item in ["Openness", "Conscientiousness", "Extraversion", "Agreeableness", "Neuroticism"]],
        "position_short": [],
        "person_count": ["1", "2", "3", "+3"],
        "location": ["indoors", "outdoors", "not identified"]
    }
    
if not st.session_state.data_loaded:
    service = get_drive_service()
    if service is None:
        st.error("No se pudo establecer la conexión con Google Drive.")
        st.stop()
    else:
        success_message = st.empty()
        success_message.success("Conexión a Google Drive establecida correctamente.")
        time.sleep(3)
        success_message.empty()

    folder_url = st.text_input(
        "Ingrese el enlace de la carpeta de Google Drive:",
        value=""
    )

    folder_id = extract_folder_id(folder_url)

    if not folder_id:
        st.warning("Por favor, ingrese un enlace de carpeta de Google Drive válido.")
        st.stop()

    files = list_files_in_folder(service, folder_id)
    #st.write(f"Número de archivos encontrados: {len(files)}")

    if not files:
        st.error("No se encontraron archivos en la carpeta de Google Drive.")
        st.stop()

    file_options = {item['name']: item['id'] for item in files if item['name'].endswith('.zip')}
    selected_file_name = st.selectbox("Selecciona el archivo ZIP:", list(file_options.keys()))

    if selected_file_name and st.button("Confirmar selección"):
        # Descargar y extraer el ZIP
        file_id = file_options[selected_file_name]
        temp_zip_path = "temp.zip"
        download_file_from_google_drive(service, file_id, temp_zip_path)
        temp_extract_path = "extracted_folders"
        extract_zip(temp_zip_path, temp_extract_path)
        
        # Cargar datos en la sesión
        if os.path.exists(temp_extract_path):

            st.write("Contenido de la carpeta extraída:", os.listdir(temp_extract_path))

            data_folder = os.path.join(temp_extract_path, 'data')
            folder1 = os.path.join(data_folder, 'NEUTRAL')
            folder2 = os.path.join(data_folder, 'OLDER')
            
            if os.path.exists(folder1) and os.path.exists(folder2):
                st.session_state.images1 = read_images_from_folder(folder1)
                st.session_state.images2 = read_images_from_folder(folder2)
                st.session_state.df_results = read_dataframe_from_zip(temp_zip_path)
                
                # Buscar y cargar cualquier archivo CSV que comience con "df_"
                csv_files = [f for f in os.listdir(data_folder) if f.startswith('df_') and f.endswith('.csv')]
                if csv_files:
                    csv_file_path = os.path.join(data_folder, csv_files[0])
                    st.session_state.df_results = pd.read_csv(csv_file_path)

                    if st.session_state.df_results is not None:
                        st.write("Columnas del DataFrame:", st.session_state.df_results.columns.tolist()) # Imprimir columnas
                        
                        required_columns = ['ID', 'filename', 'prompt']
                        missing_columns = [col for col in required_columns if col not in st.session_state.df_results.columns]
                        if missing_columns:
                            st.error(f"Las siguientes columnas no se encontraron en el DataFrame: {', '.join(missing_columns)}")
                            st.stop()
                        
                        st.session_state.df_results = st.session_state.df_results.rename(columns={'filename': 'filename_jpg'})
                        st.session_state.df_results = st.session_state.df_results.dropna(subset=['ID', 'filename_jpg', 'prompt'])
                        
                        new_categories = ["shot", "gender", "race", "emotions_short", "personality_short", "position_short", "person_count", "location",
                                          "objects", "objects_assist_devices", "objects_digi_devices"] 

                        # required_columns = ['ID', 'prompt']
                        
                        # # Verificar que al menos una de las columnas 'filename' o 'filename_jpg' esté presente
                        # if 'filename' not in st.session_state.df_results.columns and 'filename_jpg' not in st.session_state.df_results.columns:
                        #     st.error("El DataFrame debe tener una columna llamada 'filename' o 'filename_jpg'.")
                        #     st.stop()
    
                        # # Verificar las columnas obligatorias
                        # missing_columns = [col for col in required_columns if col not in st.session_state.df_results.columns]
                        # if missing_columns:
                        #     st.error(f"Las siguientes columnas no se encontraron en el DataFrame: {', '.join(missing_columns)}")
                        #     st.stop()
                        
                        # # Renombrar 'filename' a 'filename_jpg' si 'filename_jpg' no existe
                        # if 'filename_jpg' not in st.session_state.df_results.columns and 'filename' in st.session_state.df_results.columns:
                        #     st.session_state.df_results = st.session_state.df_results.rename(columns={'filename': 'filename_jpg'})
                        
                        # st.session_state.df_results = st.session_state.df_results.dropna(subset=['ID', 'filename_jpg', 'prompt'])
                        
                        # new_categories = ["shot", "gender", "race", "emotions_short", "personality_short", "position_short", "person_count", "location",
                        #                   "objects", "objects_assist_devices", "objects_digi_devices"] 
                        
                        #Convertir a minuscula personality_short
                        if 'personality_short' in st.session_state.df_results.columns:
                            st.session_state.df_results['personality_short'] = st.session_state.df_results['personality_short'].str.lower()
                        
                        for category in new_categories:
                            st.session_state.categories[category] = get_unique_list_items(st.session_state.df_results, category)
                        
                        st.session_state.data_loaded = True
                        st.success("Datos cargados correctamente. La página se actualizará automáticamente.")
                        st.rerun()
                    else:
                        st.error("No se pudo cargar el DataFrame.")
            
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
            if os.path.exists(temp_extract_path):
                shutil.rmtree(temp_extract_path, ignore_errors=True)

# Parte 2: dashboard
else:
    df_results = st.session_state.df_results
    images1 = st.session_state.images1
    images2 = st.session_state.images2

    st.sidebar.header("Filtrar imágenes")

    group_filter = st.sidebar.selectbox("Seleccionar Grupo", ["Todos", "NEUTRAL", "OLDER"], index=["Todos", "NEUTRAL", "OLDER"].index(st.session_state.group_filter))
    st.session_state.group_filter = group_filter

    filtered_df = df_results.copy()

    if group_filter == "NEUTRAL":
        filtered_df = df_results[df_results['age_group'] == 'neutral']
    elif group_filter == "OLDER":
        filtered_df = df_results[df_results['age_group'] == 'older']

    categories = st.session_state.categories

    if 'reset_filters' not in st.session_state:
        st.session_state.reset_filters = False

    # Filtro de Age Range 
    age_ranges = sorted(df_results['age_range'].unique().tolist())
    selected_age_ranges = st.sidebar.multiselect(
        "Seleccionar Age Range",
        get_sorted_options(df_results, 'age_range', age_ranges),
        default=get_default("age_ranges"),
        key="multiselect_age_ranges"
    )
    selected_age_ranges = [age.split(" (")[0] for age in selected_age_ranges]
    if selected_age_ranges:
        filtered_df = filtered_df[filtered_df['age_range'].isin(selected_age_ranges)]

    if st.sidebar.button("Resetear Filtros"):
        st.session_state.reset_filters = True
        st.session_state.group_filter = "Todos"
        st.session_state.search_term = ""
        for key in st.session_state.keys():
            if key.startswith('multiselect_'):
                st.session_state[key] = []
        st.rerun()

    # for category, options in categories.items():
    #     selected = st.sidebar.multiselect(
    #         f"Seleccionar {category.replace('_', ' ').title()}",
    #         get_sorted_options(df_results, category, options),
    #         default=get_default(category),
    #         key=f"multiselect_{category}"
    #     )

        ## if selected_options:
        ##     if category in ["activities"]:
        ##         filtered_df = filtered_df[filtered_df['prompt'].apply(lambda x: any(item.lower() in x.lower() for item in selected_options))]
        ##     else:
        ##         filtered_df = filtered_df[filtered_df[category].isin(selected_options)]
    
        # if selected_options:
        #      # Convertir tanto la columna del DataFrame como las opciones seleccionadas a tipo str
        #     filtered_df = filtered_df[filtered_df[category].astype(str).isin(selected_options)]
        #     if category in ["activities"]:
        #         filtered_df = filtered_df[filtered_df['prompt'].apply(lambda x: any(item.lower() in x.lower() for item in selected_options))]
    
    for category, options in categories.items():
        if category == "activities":
            continue  # Skip activities, handled separately
        selected = st.sidebar.multiselect(
            f"Seleccionar {category.replace('_', ' ').title()}",
            get_sorted_options(df_results, category, options),
            default=get_default(category),
            key=f"multiselect_{category}"
        )

        selected_options = [option.split(" (")[0] for option in selected]

        if selected_options:
            # Convertir tanto la columna del DataFrame como las opciones seleccionadas a tipo str
            filtered_df = filtered_df[filtered_df[category].astype(str).isin(selected_options)]

    # Separately handle the 'activities' filter
    selected_activities = st.sidebar.multiselect(
        f"Seleccionar Activities",
        get_sorted_options(df_results, 'activities', categories['activities']),
        default=get_default("activities"),
        key=f"multiselect_activities"
    )
    selected_activities_options = [option.split(" (")[0] for option in selected_activities]
    if selected_activities_options:
        filtered_df = filtered_df[filtered_df['prompt'].apply(lambda x: any(item.lower() in x.lower() for item in selected_activities_options))]
        # Filtro de Objetos
    unique_objects = get_unique_objects(df_results, "objects")
    unique_assist_devices = get_unique_objects(df_results, "objects_assist_devices")
    unique_digi_devices = get_unique_objects(df_results, "objects_digi_devices")
    
    # Crear selectores para cada categoría de objetos
    selected_objects = st.sidebar.multiselect(
        "Seleccionar Objetos (SIN LISTA)",
        [f"{obj} ({count})" for obj, count in unique_objects.items()],
        key="multiselect_objects_list"
    )
    
    selected_assist_devices = st.sidebar.multiselect(
        "Seleccionar Objetos Assist Devices (SIN LISTA)",
        [f"{obj} ({count})" for obj, count in unique_assist_devices.items()],
        key="multiselect_assist_devices_list"
    )
    
    selected_digi_devices = st.sidebar.multiselect(
        "Seleccionar Objetos Digi Devices (SIN LISTA)",
        [f"{obj} ({count})" for obj, count in unique_digi_devices.items()],
        key="multiselect_digi_devices_list"
    )
    
    if selected_objects:
        filtered_df_objects = filtered_df.copy() 
        for obj_with_count in selected_objects:
            obj = obj_with_count.split(" (")[0]
            filtered_df_objects['objects'] = filtered_df_objects['objects'].astype(str)
            filtered_df_objects = filtered_df_objects[filtered_df_objects['objects'].str.contains(obj)]
            filtered_df_objects['objects'] = filtered_df_objects['objects'].apply(eval)
        filtered_df = filtered_df_objects 
    
    if selected_assist_devices:
        filtered_df_assist = filtered_df.copy()  
        for obj_with_count in selected_assist_devices:
            obj = obj_with_count.split(" (")[0]
            filtered_df_assist['objects_assist_devices'] = filtered_df_assist['objects_assist_devices'].astype(str)
            filtered_df_assist = filtered_df_assist[filtered_df_assist['objects_assist_devices'].str.contains(obj)]
            filtered_df_assist['objects_assist_devices'] = filtered_df_assist['objects_assist_devices'].apply(eval)
        filtered_df = filtered_df_assist  
    
    if selected_digi_devices:
        filtered_df_digi = filtered_df.copy()  
        for obj_with_count in selected_digi_devices:
            obj = obj_with_count.split(" (")[0]
            filtered_df_digi['objects_digi_devices'] = filtered_df_digi['objects_digi_devices'].astype(str)
            filtered_df_digi = filtered_df_digi[filtered_df_digi['objects_digi_devices'].str.contains(obj)]
            filtered_df_digi['objects_digi_devices'] = filtered_df_digi['objects_digi_devices'].apply(eval)
        filtered_df = filtered_df_digi  

######################################################
    st.sidebar.header("Buscador de Variables")
    search_columns = df_results.columns.tolist()
    selected_column = st.sidebar.selectbox("Seleccionar Variable para Buscar", search_columns)
    
    # Aplicar búsqueda
    search_term = st.sidebar.text_input(f"Buscar en {selected_column}", value=st.session_state.search_term)
    st.session_state.search_term = search_term  # Actualizar el valor en la sesión

    if search_term:
        filtered_df = filtered_df[filtered_df[selected_column].astype(str).str.contains(search_term, case=False, na=False)]

    AgGrid(filtered_df, height=600, width='100%', fit_columns_on_grid_load=False, enable_enterprise_modules=False)

    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="Descargar DataFrame Filtrado",
        data=csv,
        file_name="filtered_dataframe.csv",
        mime="text/csv",
    )

    st.divider()
    st.write(f"Número de imágenes filtradas: {len(filtered_df)}")

    applied_filters = []
    if group_filter != "Todos":
        applied_filters.append(f"Grupo: {group_filter}")
    for category in categories:
        selected = st.session_state.get(f"multiselect_{category}", [])
        if selected:
            applied_filters.append(f"{category.replace('_', ' ').title()}: {', '.join([s.split(' (')[0] for s in selected])}")
    if selected_age_ranges:
        applied_filters.append(f"Age Range: {', '.join(selected_age_ranges)}")
    if search_term:
        applied_filters.append(f"Búsqueda: '{search_term}' en '{selected_column}'")

    if applied_filters:
        st.write("Filtros aplicados:")
        for filter_info in applied_filters:
            st.write(f"- {filter_info}")
    else:
        st.write("No se han aplicado filtros.")

    # Mostrar imágenes
    if st.session_state.fullscreen_image is None:
        for i in range(0, len(filtered_df), 4):
            row_data = filtered_df.iloc[i:i+4]
            cols = st.columns(len(row_data))
            for col_index, (_, row) in enumerate(row_data.iterrows()):
                image_name = row['filename_jpg']
                if isinstance(image_name, str):
                    image_path = None
                    if image_name in images1:
                        image_path = images1[image_name]
                    elif image_name in images2:
                        image_path = images2[image_name]

                    if image_path:
                        cols[col_index].image(image_path, caption=image_name, use_column_width=True)
                        if cols[col_index].button(f"Ver imagen completa", key=f"btn_{image_name}_{row.name}"):
                            toggle_fullscreen(image_name)
                            st.rerun()
            st.markdown("<hr style='margin-top: 20px; margin-bottom: 20px;'>", unsafe_allow_html=True)
    else:
        col1, col2 = st.columns([3, 2])
        with col1:
            fullscreen_image_path = None
            if st.session_state.fullscreen_image in images1:
                fullscreen_image_path = images1[st.session_state.fullscreen_image]
            elif st.session_state.fullscreen_image in images2:
                fullscreen_image_path = images2[st.session_state.fullscreen_image]

            if fullscreen_image_path:
                st.image(fullscreen_image_path, caption=st.session_state.fullscreen_image, use_column_width=True)
            else:
                st.error("No se pudo encontrar la imagen para mostrar en pantalla completa.")
                
        with col2:
            st.subheader("Detalles de la imagen")
            fullscreen_row = filtered_df[filtered_df['filename_jpg'] == st.session_state.fullscreen_image]
            if not fullscreen_row.empty:
                show_image_details(fullscreen_row.iloc[0].to_dict())
            else:
                st.warning("No se encontraron detalles para esta imagen.")
        
        if st.button("Cerrar imagen completa", key="close_fullscreen"):
            st.session_state.fullscreen_image = None
            st.rerun()
        st.markdown("<hr style='margin-top: 20px; margin-bottom: 20px;'>", unsafe_allow_html=True)

    # Botón para descargar imágenes filtradas como ZIP
    if len(filtered_df) > 0:
        zip_buffer = create_downloadable_zip(filtered_df, images1, images2)
        if zip_buffer and zip_buffer.getbuffer().nbytes > 0:
            st.download_button(
                label="Descargar imágenes filtradas como ZIP",
                data=zip_buffer,
                file_name="filtered_images.zip",
                mime="application/zip"
            )
        else:
            st.error("No se pudo crear el archivo ZIP.")
    else:
        st.error("No se encontraron imágenes que cumplan con los filtros aplicados.")

# Limpiar archivos temporales
if 'temp_zip_path' in locals() and temp_zip_path is not None and os.path.exists(temp_zip_path):
    try:
        os.remove(temp_zip_path)
    except Exception as e:
        st.warning(f"Could not remove temporary zip file: {e}")

if 'temp_extract_path' in locals() and temp_extract_path is not None and os.path.exists(temp_extract_path):
    try:
        shutil.rmtree(temp_extract_path, ignore_errors=True)
    except Exception as e:
        st.warning(f"Could not remove temporary extracted folder: {e}")
