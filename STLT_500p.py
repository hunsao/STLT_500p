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
# Removed cache_data decorator for get_drive_service as it's often better not to cache resources like service objects directly
# from streamlit import cache_data # Removed this import as cache_data is used specifically below
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
# from google_auth_httplib2 import Request # Seems unused, commented out
from googleapiclient.errors import HttpError

# from googleapiclient.http import HttpRequest # Seems unused, commented out
# from googleapiclient.http import build_http # Seems unused, commented out

# http = build_http() # Seems unused, commented out
# http.timeout = 120 # Seems unused, commented out

st.set_page_config(layout="wide")

# --- Session State Initialization ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df_results = None
    st.session_state.all_images = None # Changed from images1/images2
    st.session_state.group_filter = "Todos"
    st.session_state.search_term = ""
    st.session_state.fullscreen_image = None
    st.session_state.reset_filters = False
    # Initialize categories if not already done
    if 'categories' not in st.session_state:
         st.session_state.categories = {
            "gender": ["male", "female", "not identified"],
            "race": ["asian", "white", "black", "hispanic", "other"],
            "activities" : [ # Keep your extensive list
                "sleeping","being sick in bed","eating","grooming","receiving personal care","taking a bath","at work","taking a lunch break","in a job fair","taking a course","doing homework","doing an internship","taking a break from studying","attending extracurricular classes","attending a webinar","in a study group","handling home tasks","preparing food","washing dishes","storing food","doing house cleaning","cleaning the garden","heating their home","arranging household goods","recycling","doing home maintenance","doing laundry","ironing","gardening","caring for pets","walking the dog","constructing or renovating the house","repairing the dwelling","fixing and maintaining tools","maintaining the vehicle","shopping","managing banking accounts","planning shopping","managing the household","providing physical care and supervision of a child","educating the child","reading, playing, and talking with the child","providing physical care of an adult household member","offering childcare services","providing support to an adult","volunteering","attending meetings","engaging in religious activities","Paying respects at graves","participating in community events","in a family meeting","hosting guests at home","in a party","engaging in a discussion","sending and receiving messages","spending time on social media","in a social gathering","in a movie night","attending theatre or live concerts","viewing art collections","in a library","participating in sports events","in a botanical garden","taking a break","going for a walk","running for exercise","riding a bike","engaging in team sports","engaging in fitness routines","doing swimming and other water activities","meditating","engaging in productive exercise","participating in sports-related activities","engaging in visual arts","amassing collectibles","making handicraft products","using computers","searching for information online","handling video game consoles","engaging in smartphone games","reading news","reading books","watching movies or videos","listening to music or talk shows","updating the time diary","in the room where they sleep","in the living room","traveling","traveling for work","going to study locations","going to shops and services","traveling for family care","moving to a new location"
            ],
            "emotions_short": ["neutral", "positive", "negative"],
            "personality_short": [item.lower() for item in ["Openness", "Conscientiousness", "Extraversion", "Agreeableness", "Neuroticism"]],
            "position_short": [], # Will be populated dynamically if found
            "person_count": ["1", "2", "3", "+3"],
            "location": ["indoors", "outdoors", "not identified"],
            "shot": [], # Will be populated dynamically if found
             # Add potentially dynamic categories here too
            "objects": [],
            "objects_assist_devices": [],
            "objects_digi_devices": [],
        }

# --- Caching Functions ---
@st.cache_data()
def count_observations(df, category, options):
    if df is None or category not in df.columns and category not in ['activities', 'prompt']:
         return {option: 0 for option in options}

    if category in ['activities']:
        # Count occurrences within the 'prompt' string for activities
        return {option: df['prompt'].astype(str).str.contains(option, case=False, na=False).sum() for option in options}
    elif category == 'prompt':
         # Generic prompt search (if needed, though usually handled by text search)
        return {option: df[category].astype(str).str.contains(option, case=False, na=False).sum() for option in options}
    elif category in ["objects", "objects_assist_devices", "objects_digi_devices"]:
         # Handle list-like columns stored as strings
         counts = {option: 0 for option in options}
         for option in options:
             # Safely check for the string representation of the option within the column
             # This handles cases like "['item1', 'item2']" containing "'item1'"
             counts[option] = df[category].astype(str).str.contains(f"'{option}'", regex=False, na=False).sum()
         return counts
    else:
         # Standard categorical counting
        return {option: df[df[category].astype(str) == str(option)].shape[0] for option in options}

@st.cache_data()
def get_sorted_options(df, category, options):
    if df is None:
        return [f"{option} (0)" for option in options]

    # Ensure options are strings for consistent processing
    str_options = [str(opt) for opt in options]

    counts = count_observations(df, category, str_options)
    # Handle potential missing counts if an option wasn't found
    options_with_count = sorted([(option, counts.get(option, 0)) for option in str_options], key=lambda x: x[1], reverse=True)
    return [f"{option} ({count})" for option, count in options_with_count]

@st.cache_data(max_entries=1)
def create_downloadable_zip(_filtered_df, _all_images):
    zip_buffer = io.BytesIO()
    try:
        with ZipFile(zip_buffer, 'w') as zip_file:
            # Ensure _all_images is not None
            if _all_images is None:
                 st.error("Image dictionary is not available.")
                 return zip_buffer # Return empty buffer

            for _, row in _filtered_df.iterrows():
                image_name = row.get('filename_jpg')
                age_group = row.get('age_group') # Get the age group from the DataFrame

                if image_name is None:
                    st.warning(f"Row missing 'filename_jpg': {row.get('ID', 'N/A')}")
                    continue
                if age_group is None:
                    st.warning(f"Row missing 'age_group' for image {image_name}")
                    continue

                if isinstance(image_name, str) and isinstance(age_group, str):
                    image_path = _all_images.get(image_name) # Look up in the combined dictionary

                    if image_path and os.path.exists(image_path):
                        # Use the age_group from the DataFrame as the folder name in the ZIP
                        folder_name_in_zip = age_group
                        zip_file.write(image_path, os.path.join(folder_name_in_zip, image_name))
                    else:
                        st.warning(f"Image file not found or path invalid for: {image_name} (Expected at: {image_path})")
                else:
                    st.warning(f"Invalid type for image_name or age_group for image: {image_name}")

    except Exception as e:
        st.error(f"Error creating ZIP file: {str(e)}")
        # Consider logging the full traceback here for debugging
    finally:
        zip_buffer.seek(0)
    return zip_buffer

# --- Google Drive Functions ---
@st.cache_resource # Cache the service object
def get_drive_service():
    try:
        encoded_sa = os.getenv('GOOGLE_SERVICE_ACCOUNT')
        if not encoded_sa:
            # Try loading from Streamlit secrets if env var is not set
            if 'google_service_account_encoded' in st.secrets:
                 encoded_sa = st.secrets['google_service_account_encoded']
            else:
                raise ValueError("Google Service Account credentials not found in environment variables (GOOGLE_SERVICE_ACCOUNT) or Streamlit secrets (google_service_account_encoded)")

        sa_json = base64.b64decode(encoded_sa).decode('utf-8')
        sa_dict = json.loads(sa_json)

        credentials = service_account.Credentials.from_service_account_info(
            sa_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

        service = build('drive', 'v3', credentials=credentials)
        return service
    except Exception as e:
        st.error(f"Error initializing Google Drive service: {str(e)}")
        return None

def list_files_in_folder(service, folder_id, retries=3):
    for attempt in range(retries):
        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false", # Exclude trashed files
                fields="files(id, name)"
            ).execute()
            return results.get('files', [])
        except HttpError as error:
            st.error(f"Error listing files (Attempt {attempt+1}/{retries}): {error}")
            if error.resp.status in [403, 500, 503] and attempt < retries - 1:
                time.sleep(2 ** attempt) # Exponential backoff
            else:
                raise # Reraise the error if it's not retryable or retries exhausted
        except Exception as e: # Catch other potential errors
            st.error(f"An unexpected error occurred while listing files: {e}")
            raise

# Removed custom RequestWithTimeout as it seemed unused
# class RequestWithTimeout(Request):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.timeout = 120

def download_file_from_google_drive(service, file_id, dest_path, retries=3):
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            # Use 'wb' for binary write mode
            with io.FileIO(dest_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024*5) # 5MB chunk size
                progress_bar = st.progress(0)
                status_text = st.empty()
                done = False
                while not done:
                    try:
                        status, done = downloader.next_chunk(num_retries=2) # Add retries within next_chunk
                        if status:
                            progress = int(status.progress() * 100)
                            progress_bar.progress(progress)
                            status_text.text(f"Downloading... {progress}%")
                    except HttpError as e:
                         # Handle chunk download errors specifically
                         st.warning(f"Error during chunk download (Attempt {attempt+1}): {e}. Retrying...")
                         time.sleep(2) # Wait before retrying the chunk
                         # Continue the inner loop to retry the chunk
                    except Exception as inner_e:
                        st.error(f"Unexpected error during chunk download: {inner_e}")
                        raise inner_e # Reraise unexpected errors
            progress_bar.empty() # Remove progress bar on completion
            status_text.success(f"File downloaded successfully to {dest_path}")
            return True # Indicate success
        except HttpError as error:
            st.error(f"Error downloading file (Attempt {attempt+1}/{retries}): {error}")
            if error.resp.status in [403, 500, 503] and attempt < retries - 1:
                 time.sleep(2 ** attempt) # Exponential backoff
            else:
                 st.error(f"Failed to download file after {retries} attempts.")
                 return False # Indicate failure
        except Exception as e:
            st.error(f"An unexpected error occurred during download (Attempt {attempt+1}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                st.error(f"Failed to download file due to unexpected error: {e}")
                return False # Indicate failure
    return False # Indicate failure if all retries fail

def extract_zip(zip_path, extract_to):
    # Ensure the extraction directory exists and is empty
    if os.path.exists(extract_to):
        shutil.rmtree(extract_to)
    os.makedirs(extract_to, exist_ok=True)

    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        st.write(f"Extracted contents in '{extract_to}':")
        # List only top-level items for brevity
        st.write([item for item in os.listdir(extract_to)])
        return True
    except FileNotFoundError:
        st.error(f"Error extracting ZIP: File not found at {zip_path}")
        return False
    except Exception as e:
        st.error(f"Error extracting ZIP file '{zip_path}': {str(e)}")
        return False

@st.cache_data()
def extract_folder_id(url):
    """Extract the folder ID from a Google Drive URL."""
    # Handle different URL formats
    patterns = [
        r'folders/([a-zA-Z0-9-_]+)',
        r'id=([a-zA-Z0-9-_]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

# --- Image and Data Handling Functions ---

def show_image_details(image_data):
    if isinstance(image_data, dict):
        for key, value in image_data.items():
            # Pretty print lists or long strings
            if isinstance(value, list):
                 st.write(f"**{key}:**")
                 st.json(value, expanded=False) # Use st.json for better list display
            elif isinstance(value, str) and len(value) > 100:
                 st.write(f"**{key}:**")
                 st.text_area("", value, height=100, disabled=True, key=f"detail_{key}")
            else:
                st.write(f"**{key}:** {value}")
    else:
        st.write("Invalid image data format.")


# Keep persist="disk" if caching large amounts of image data is beneficial and fits disk limits
@st.cache_data(persist="disk")
def read_images_from_folder(folder_path):
    images = {}
    if not os.path.isdir(folder_path):
        st.warning(f"Image folder not found: {folder_path}")
        return images
    try:
        # Use natural sort key for filenames
        filenames = sorted(os.listdir(folder_path), key=natural_sort_key)
        for filename in filenames:
            # Make check case-insensitive
            if filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp")): # Added more formats
                image_path = os.path.join(folder_path, filename)
                if os.path.isfile(image_path):
                    images[filename] = image_path # Store filename as key, full path as value
    except Exception as e:
        st.error(f"Error reading images from {folder_path}: {e}")
    return images

def natural_sort_key(s):
    # Handles filenames with numbers for sorting (e.g., img1.jpg, img10.jpg)
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s)]

# Removed caching here as it might read outdated CSV if ZIP is re-uploaded with same name
# @st.cache_data(persist="disk")
def find_and_read_csv(extract_path):
    data_folder = os.path.join(extract_path, 'data')
    if not os.path.isdir(data_folder):
        st.error(f"'data' directory not found within extracted files at {extract_path}")
        return None

    csv_files = [f for f in os.listdir(data_folder) if f.startswith('df_') and f.endswith('.csv')]
    if not csv_files:
        st.error(f"No CSV file starting with 'df_' found in the 'data' directory.")
        return None

    if len(csv_files) > 1:
        st.warning(f"Multiple CSV files found ({', '.join(csv_files)}). Using the first one: {csv_files[0]}")

    csv_file_path = os.path.join(data_folder, csv_files[0])
    try:
        df = pd.read_csv(csv_file_path)
        st.success(f"Successfully loaded DataFrame from {csv_files[0]}.")
        return df
    except Exception as e:
        st.error(f"Error reading CSV file {csv_files[0]}: {e}")
        return None


def toggle_fullscreen(image_name):
    if st.session_state.get('fullscreen_image') == image_name:
        st.session_state.fullscreen_image = None
    else:
        st.session_state.fullscreen_image = image_name

# Helper to get default multiselect values safely from session state
def get_default(key):
    return st.session_state.get(key, [])


# Keep caching for potentially expensive unique value extraction
@st.cache_data(persist="disk")
def get_unique_list_items(df_results, category):
    unique_items = set()
    if df_results is not None and category in df_results.columns:
        # Drop NA values before processing
        items_series = df_results[category].dropna()

        for item in items_series:
            # Handle items that might be strings representing lists or actual lists
            processed_item = None
            if isinstance(item, str):
                try:
                    # Try evaluating string as a list/dict
                    evaluated = eval(item)
                    if isinstance(evaluated, list):
                         processed_item = tuple(sorted([str(i) for i in evaluated])) # Convert list elements to string and sort
                    elif isinstance(evaluated, dict):
                         processed_item = tuple(sorted(evaluated.items())) # Convert dict to tuple of sorted items
                    else:
                         processed_item = str(item) # Treat as a simple string if eval doesn't yield list/dict
                except:
                     processed_item = str(item) # Treat as simple string if eval fails
            elif isinstance(item, list):
                 processed_item = tuple(sorted([str(i) for i in item]))
            elif isinstance(item, dict):
                 processed_item = tuple(sorted(item.items()))
            else:
                 processed_item = str(item) # Ensure hashable type

            if processed_item is not None:
                unique_items.add(processed_item)

        # Convert back from tuples if necessary for display/options (optional, depends on need)
        # For now, return the hashable versions found
        # Sort simple strings, leave tuples as they are
        final_list = sorted([item for item in unique_items if isinstance(item, str)]) + \
                     sorted([item for item in unique_items if not isinstance(item, str)])

        # If the original category was intended to be simple strings (like gender, race), convert tuples back if needed
        # This part might need adjustment based on how list-like columns are actually used
        # For now, we assume the goal is just to get the unique *values* regardless of original structure complexity
        # Let's try converting everything back to a simple string representation for the options list
        return sorted([str(item) for item in unique_items])

    return []


# Keep caching for potentially expensive unique value extraction
@st.cache_data()
def get_unique_objects(df, column_name):
    unique_objects = {}
    if df is None or column_name not in df.columns:
        return {}

    for objects_list_str in df[column_name].dropna():
        items_to_add = []
        if isinstance(objects_list_str, str):
            try:
                # Use json.loads for safer evaluation of list-like strings
                objects = json.loads(objects_list_str.replace("'", "\"")) # Replace single quotes for valid JSON
                if isinstance(objects, list):
                    items_to_add = objects
                else:
                     # If it's not a list after parsing, treat the original string as a single item (optional)
                     # items_to_add = [objects_list_str]
                     pass # Or ignore if only lists are expected
            except json.JSONDecodeError:
                 # If JSON parsing fails, treat as a single comma-separated string or single item
                 # This is a fallback, ideally the data is consistently formatted
                 items_to_add = [s.strip() for s in objects_list_str.split(',') if s.strip()]
            except Exception:
                # Fallback for other eval-like errors if json fails
                try:
                    objects = eval(objects_list_str)
                    if isinstance(objects, list):
                         items_to_add = objects
                except:
                    # If all parsing fails, treat the original string as one item
                    items_to_add = [objects_list_str]

        elif isinstance(objects_list_str, list): # Handle actual lists
             items_to_add = objects_list_str

        # Count the items
        for item in items_to_add:
             item_str = str(item).strip() # Ensure string and remove whitespace
             if item_str: # Avoid counting empty strings
                 unique_objects[item_str] = unique_objects.get(item_str, 0) + 1

    # Sort by count descending
    sorted_objects = dict(sorted(unique_objects.items(), key=lambda item: item[1], reverse=True))
    return sorted_objects

# --- Main App Logic ---

st.markdown("<h1 style='text-align: center; color: white;'>AGEAI: Imágenes y Metadatos. v4 (Multi-Group)</h1>", unsafe_allow_html=True)

# Display instructions
with st.expander("📋 Instrucciones / Folder Structure (Click to Expand)"):
    st.markdown("""
    <h3>📁 Required ZIP Structure</h3>
    The application expects a ZIP file containing a `data` folder. Inside `data`, there should be:
    <ol>
        <li>Subfolders named after your age group categories (e.g., `older`, `young`, `middle-aged`, `person`).</li>
        <li>A single CSV file whose name starts with `df_` (e.g., `df_results.csv`).</li>
    </ol>

    Example:
    <pre>
    your_archive.zip
    └── data/
        ├── older/
        │   ├── image1.jpg
        │   └── image2.jpeg
        ├── young/
        │   ├── image3.jpg
        ├── middle-aged/
        │   └── image4.jpg
        ├── person/
        │   └── image5.jpg
        └── df_metadata.csv  <-- Must contain 'age_group', 'filename_jpg', 'ID', 'prompt' columns
    </pre>

    <h3>📄 CSV Requirements</h3>
    <ul>
    <li>Must be inside the `data` folder.</li>
    <li>Filename must start with `df_`.</li>
    <li><b>Crucially, it must contain the following columns:</b>
        <ul>
            <li><code>ID</code>: Unique identifier for the row/image.</li>
            <li><code>filename_jpg</code>: The exact image filename (e.g., `image1.jpg`).</li>
            <li><code>prompt</code>: The text prompt associated with the image.</li>
            <li><code>age_group</code>: The category name corresponding to the subfolder the image is in (e.g., `older`, `young`).</li>
        </ul>
    </li>
    <li>Other metadata columns (gender, race, etc.) are used for filtering if present.</li>
    <li>Ensure no essential columns have null/empty values.</li>
    </ul>

    <h3>🖼️ Images</h3>
    <ul>
    <li>Place images (.jpg, .jpeg, .png, .webp) inside the correct category subfolder within `data`.</li>
    <li>Image filenames must exactly match the entries in the <code>filename_jpg</code> column of the CSV.</li>
    </ul>

    <h3>⚠️ Important Notes</h3>
    <ul>
    <li>Folder names for categories inside `data` **must exactly match** the values used in the `age_group` column of the CSV.</li>
    <li>The app dynamically detects category folders; you don't need to hardcode them here anymore.</li>
    </ul>
    """)
st.markdown(" ")


# --- Part 1: Data Loading ---
if not st.session_state.data_loaded:
    st.header("1. Load Data from Google Drive")
    service = get_drive_service()

    if service is None:
        st.error("Failed to connect to Google Drive. Please check credentials and permissions.")
        st.stop()
    else:
        # Subtle connection success message
        conn_msg = st.success("Connected to Google Drive.")
        time.sleep(2)
        conn_msg.empty()

    folder_url = st.text_input(
        "Enter Google Drive Folder URL:",
        key="gdrive_url_input",
        placeholder="https://drive.google.com/drive/folders/YourFolderID..."
    )

    folder_id = extract_folder_id(folder_url) if folder_url else None

    if folder_id:
        st.info(f"Extracted Folder ID: {folder_id}")
        try:
            with st.spinner("Listing files in Google Drive folder..."):
                files = list_files_in_folder(service, folder_id)

            if not files:
                st.error("No files found in the specified Google Drive folder. Check the URL and folder contents.")
                st.stop()

            # Filter for .zip files only
            file_options = {item['name']: item['id'] for item in files if item['name'].lower().endswith('.zip')}

            if not file_options:
                 st.warning("No ZIP files found in the Google Drive folder.")
                 st.stop()

            selected_file_name = st.selectbox("Select the ZIP file to load:", list(file_options.keys()))

            if selected_file_name and st.button("Load Selected ZIP File"):
                file_id = file_options[selected_file_name]
                temp_zip_path = f"./temp_{selected_file_name}" # Use unique temp name
                temp_extract_path = "./extracted_data"

                # Clean up previous temp files/dirs if they exist
                if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                if os.path.exists(temp_extract_path): shutil.rmtree(temp_extract_path)

                st.info(f"Downloading '{selected_file_name}'...")
                download_success = download_file_from_google_drive(service, file_id, temp_zip_path)

                if download_success:
                    st.info(f"Extracting '{selected_file_name}'...")
                    extract_success = extract_zip(temp_zip_path, temp_extract_path)

                    if extract_success:
                        # Load DataFrame
                        st.session_state.df_results = find_and_read_csv(temp_extract_path)

                        if st.session_state.df_results is not None:
                             # --- Crucial Column Checks ---
                            required_columns = ['ID', 'filename_jpg', 'prompt', 'age_group']
                             # Allow 'filename' as an alias for 'filename_jpg'
                            if 'filename' in st.session_state.df_results.columns and 'filename_jpg' not in st.session_state.df_results.columns:
                                st.session_state.df_results = st.session_state.df_results.rename(columns={'filename': 'filename_jpg'})
                                st.info("Renamed 'filename' column to 'filename_jpg'.")

                            missing_columns = [col for col in required_columns if col not in st.session_state.df_results.columns]
                            if missing_columns:
                                st.error(f"CSV file is missing required columns: {', '.join(missing_columns)}. Please ensure the CSV inside the ZIP is correctly formatted.")
                                st.stop() # Stop execution if essential columns are missing

                            # Drop rows with missing essential values
                            initial_rows = len(st.session_state.df_results)
                            st.session_state.df_results = st.session_state.df_results.dropna(subset=required_columns)
                            dropped_rows = initial_rows - len(st.session_state.df_results)
                            if dropped_rows > 0:
                                st.warning(f"Dropped {dropped_rows} rows due to missing values in essential columns ({', '.join(required_columns)}).")

                            st.write("DataFrame Columns:", st.session_state.df_results.columns.tolist())

                            # Load Images Dynamically
                            data_folder_path = os.path.join(temp_extract_path, 'data')
                            all_loaded_images = {}
                            if os.path.isdir(data_folder_path):
                                st.write("Looking for category subfolders in:", data_folder_path)
                                potential_folders = [d for d in os.listdir(data_folder_path) if os.path.isdir(os.path.join(data_folder_path, d))]
                                st.write("Found potential category folders:", potential_folders)

                                # Read images from each detected subfolder
                                for folder_name in potential_folders:
                                     folder_path = os.path.join(data_folder_path, folder_name)
                                     st.write(f"Reading images from: {folder_path}")
                                     images_in_folder = read_images_from_folder(folder_path)
                                     if images_in_folder:
                                         st.write(f"Found {len(images_in_folder)} images in '{folder_name}'.")
                                         # Check for duplicate filenames across folders
                                         duplicates = set(images_in_folder.keys()) & set(all_loaded_images.keys())
                                         if duplicates:
                                             st.warning(f"Duplicate filenames found across folders: {', '.join(duplicates)}. Using images from the last processed folder ('{folder_name}') for these duplicates.")
                                         all_loaded_images.update(images_in_folder)
                                     else:
                                          st.warning(f"No images found or error reading from '{folder_name}'.")

                                st.session_state.all_images = all_loaded_images
                                st.success(f"Total images loaded from all folders: {len(st.session_state.all_images)}")

                                # Update dynamic categories based on loaded DataFrame
                                st.info("Updating filter options based on loaded data...")
                                dynamic_categories = ["shot", "position_short", "objects", "objects_assist_devices", "objects_digi_devices"] # Add others if needed
                                for category in dynamic_categories:
                                     if category in st.session_state.df_results.columns:
                                         st.session_state.categories[category] = get_unique_list_items(st.session_state.df_results, category)
                                     else:
                                         st.session_state.categories[category] = [] # Ensure it's an empty list if column not found
                                         st.warning(f"Column '{category}' not found in CSV, filter options will be empty.")

                                # Standardize personality_short to lowercase
                                if 'personality_short' in st.session_state.df_results.columns:
                                    st.session_state.df_results['personality_short'] = st.session_state.df_results['personality_short'].astype(str).str.lower()

                                st.session_state.data_loaded = True
                                st.success("Data loaded successfully!")
                                # Clean up temporary files AFTER successful load
                                if os.path.exists(temp_zip_path): os.remove(temp_zip_path)
                                # Keep extracted data for image paths until session ends or new data loaded
                                # If memory/disk is a concern, you might copy images to a more permanent temp location managed by Streamlit
                                st.info("App will now reload with the data.")
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error("'data' directory not found within the extracted ZIP file.")
                        else:
                            st.error("Failed to load DataFrame from the CSV file.")
                    else:
                        st.error("Failed to extract the ZIP file.")
                else:
                    st.error("Failed to download the ZIP file from Google Drive.")

                # Optional: Clean up extracted folder if loading failed mid-way
                # if not st.session_state.data_loaded and os.path.exists(temp_extract_path):
                #     shutil.rmtree(temp_extract_path, ignore_errors=True)


    else:
        if folder_url: # Only show warning if URL is entered but ID extraction failed
            st.warning("Invalid Google Drive URL. Please enter a valid URL pointing to a folder.")
        st.stop()

# --- Part 2: Dashboard (Displayed only after data is loaded) ---
else:
    st.header("2. Explore Images and Metadata")
    df_results = st.session_state.df_results
    all_images = st.session_state.all_images # Use the single image dictionary
    categories = st.session_state.categories

    if df_results is None or all_images is None:
        st.error("Dataframe or images could not be loaded. Please try reloading the data.")
        st.stop()

    # --- Sidebar Filters ---
    st.sidebar.header("Filter Images")

    # Group Filter (Dynamic based on 'age_group' column)
    if 'age_group' in df_results.columns:
        age_group_options = ["Todos"] + sorted(df_results['age_group'].unique().tolist())
        # Ensure the saved filter value is valid, otherwise default to "Todos"
        current_group_filter = st.session_state.get('group_filter', "Todos")
        if current_group_filter not in age_group_options:
            st.session_state.group_filter = "Todos"
        group_filter_index = age_group_options.index(st.session_state.group_filter)

        group_filter = st.sidebar.selectbox(
            "Select Group (from 'age_group' column)",
            age_group_options,
            index=group_filter_index,
            key="group_select"
            )
        st.session_state.group_filter = group_filter # Update session state
    else:
        st.sidebar.warning("'age_group' column not found. Group filter disabled.")
        st.session_state.group_filter = "Todos" # Default if column missing
        group_filter = "Todos"


    # --- Apply Filters ---
    filtered_df = df_results.copy()

    # Apply Group Filter
    if group_filter != "Todos":
        filtered_df = filtered_df[filtered_df['age_group'] == group_filter]

    # Apply other filters...

    # Reset Button
    if st.sidebar.button("Reset All Filters"):
        st.session_state.reset_filters = True
        st.session_state.group_filter = "Todos"
        st.session_state.search_term = ""
        # Clear all multiselect session state keys
        for key in list(st.session_state.keys()):
            if key.startswith('multiselect_'):
                st.session_state[key] = []
        st.rerun() # Rerun to apply the reset

    # Age Range Filter (If column exists)
    if 'age_range' in df_results.columns:
        age_ranges = sorted(df_results['age_range'].astype(str).unique().tolist())
        selected_age_ranges_display = st.sidebar.multiselect(
            "Select Age Range",
            get_sorted_options(df_results, 'age_range', age_ranges), # Use original df for counts
            default=get_default("multiselect_age_ranges"), # Get from session state
            key="multiselect_age_ranges" # Use consistent key
        )
        # Extract actual values from "Option (count)" format
        selected_age_ranges = [re.match(r"^(.*?)\s*\(\d+\)$", age).group(1) for age in selected_age_ranges_display]
        if selected_age_ranges:
            filtered_df = filtered_df[filtered_df['age_range'].isin(selected_age_ranges)]
    else:
        st.sidebar.text("Age Range filter unavailable.")

    # Dynamic Category Filters
    for category, options in categories.items():
        # Skip activities here, handle separately
        # Skip object lists here, handle separately
        if category in ["activities", "objects", "objects_assist_devices", "objects_digi_devices"]:
            continue
        if category not in df_results.columns:
             #st.sidebar.warning(f"Column '{category}' not found, filter skipped.") # Optional warning
             continue # Skip if column doesn't exist in loaded data

        selected_display = st.sidebar.multiselect(
            f"Select {category.replace('_', ' ').title()}",
            get_sorted_options(df_results, category, options), # Get options based on original DF
            default=get_default(f"multiselect_{category}"), # Get from session state
            key=f"multiselect_{category}" # Use consistent key
        )
        # Extract actual option values
        selected_options = [re.match(r"^(.*?)\s*\(\d+\)$", opt).group(1) for opt in selected_display]

        if selected_options:
            # Ensure comparison works even if column has mixed types (integers, strings)
            # Convert column to string for robust comparison
            filtered_df = filtered_df[filtered_df[category].astype(str).isin([str(opt) for opt in selected_options])]


    # Activities Filter (searching within 'prompt')
    if 'activities' in categories and categories['activities']: # Check if activities defined
        selected_activities_display = st.sidebar.multiselect(
            f"Select Activities (searches Prompt)",
            get_sorted_options(df_results, 'activities', categories['activities']), # Use original df
            default=get_default("multiselect_activities"),
            key=f"multiselect_activities"
        )
        selected_activities_options = [re.match(r"^(.*?)\s*\(\d+\)$", opt).group(1) for opt in selected_activities_display]
        if selected_activities_options:
            # Apply filter using case-insensitive search within the 'prompt' column
            pattern = '|'.join([re.escape(act) for act in selected_activities_options]) # Create OR pattern
            filtered_df = filtered_df[filtered_df['prompt'].astype(str).str.contains(pattern, case=False, na=False, regex=True)]

    # Object List Filters
    object_filters = {
        "objects": ("Objetos (Any Match)", "multiselect_objects_list"),
        "objects_assist_devices": ("Assist Devices (Any Match)", "multiselect_assist_devices_list"),
        "objects_digi_devices": ("Digi Devices (Any Match)", "multiselect_digi_devices_list")
    }

    for col_name, (label, key) in object_filters.items():
        if col_name in df_results.columns:
            unique_obj_counts = get_unique_objects(df_results, col_name) # Calculate based on original df
            obj_options = [f"{obj} ({count})" for obj, count in unique_obj_counts.items()]

            selected_obj_display = st.sidebar.multiselect(
                f"Select {label}",
                obj_options,
                default=get_default(key),
                key=key
            )
            selected_obj_options = [re.match(r"^(.*?)\s*\(\d+\)$", opt).group(1) for opt in selected_obj_display]

            if selected_obj_options:
                 # Filter rows where the column (treated as a string) contains *any* of the selected objects
                 # This handles list-like strings: "['obj1', 'obj2']"
                 pattern = '|'.join([re.escape(f"'{obj}'") for obj in selected_obj_options]) # Look for 'obj' including quotes
                 filtered_df = filtered_df[filtered_df[col_name].astype(str).str.contains(pattern, case=False, na=False, regex=True)]
        else:
             st.sidebar.text(f"{label} filter unavailable.")


    # --- Search ---
    st.sidebar.header("Search Specific Variable")
    # Use columns from the original DataFrame for selection
    search_columns = df_results.columns.tolist()
    # Default to 'prompt' if available, otherwise the first column
    default_search_col_index = search_columns.index('prompt') if 'prompt' in search_columns else 0
    selected_column = st.sidebar.selectbox(
        "Select Variable to Search In",
        search_columns,
        index=default_search_col_index,
        key="search_column_select"
        )

    # Apply search term filter
    search_term_input = st.sidebar.text_input(
        f"Search Text in '{selected_column}'",
        value=st.session_state.get("search_term", ""), # Get from session state
        key="search_term_input"
        )
    st.session_state.search_term = search_term_input # Update session state

    if search_term_input:
        # Apply case-insensitive search to the selected column converted to string
        filtered_df = filtered_df[filtered_df[selected_column].astype(str).str.contains(search_term_input, case=False, na=False)]

    # --- Display Filtered DataFrame ---
    st.subheader("Filtered Data Table")
    st.write(f"Showing {len(filtered_df)} out of {len(df_results)} total entries.")
    AgGrid(filtered_df, height=400, width='100%', fit_columns_on_grid_load=True, enable_enterprise_modules=False) # Adjusted height

    # --- Download Filtered CSV ---
    if not filtered_df.empty:
        csv_buffer = io.StringIO()
        filtered_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download Filtered Data as CSV",
            data=csv_buffer.getvalue(),
            file_name="filtered_ageai_data.csv",
            mime="text/csv",
            key="download_csv_button"
        )
    else:
        st.info("No data matches the current filters to download.")


    st.divider()

    # --- Display Applied Filters ---
    st.subheader("Applied Filters")
    applied_filters_list = []
    if group_filter != "Todos":
        applied_filters_list.append(f"Group: {group_filter}")
    if 'age_range' in df_results.columns and st.session_state.get("multiselect_age_ranges"):
         applied_filters_list.append(f"Age Range: {', '.join([re.match(r'^(.*?)\s*\(\d+\)$', age).group(1) for age in st.session_state.multiselect_age_ranges])}")

    # Check other category filters
    for category in categories.keys():
        session_key = f"multiselect_{category}"
        if st.session_state.get(session_key):
             selected_vals = [re.match(r"^(.*?)\s*\(\d+\)$", opt).group(1) for opt in st.session_state[session_key]]
             applied_filters_list.append(f"{category.replace('_', ' ').title()}: {', '.join(selected_vals)}")

     # Check object filters
    for col_name, (label, key) in object_filters.items():
        if st.session_state.get(key):
             selected_vals = [re.match(r"^(.*?)\s*\(\d+\)$", opt).group(1) for opt in st.session_state[key]]
             applied_filters_list.append(f"{label}: {', '.join(selected_vals)}")

    if st.session_state.search_term:
        applied_filters_list.append(f"Search '{st.session_state.search_term}' in '{selected_column}'")

    if applied_filters_list:
        for filter_info in applied_filters_list:
            st.write(f"- {filter_info}")
    else:
        st.write("No filters applied.")

    st.divider()

    # --- Display Images ---
    st.subheader("Filtered Images")
    st.write(f"Displaying {len(filtered_df)} filtered images.")

    if st.session_state.fullscreen_image is None:
        if not filtered_df.empty:
            images_per_row = 4
            # Iterate through the filtered DataFrame for display
            for i in range(0, len(filtered_df), images_per_row):
                row_data = filtered_df.iloc[i:min(i + images_per_row, len(filtered_df))]
                cols = st.columns(images_per_row)
                for col_idx, (_, row) in enumerate(row_data.iterrows()):
                    image_name = row['filename_jpg']
                    # Use the unified image dictionary
                    image_path = all_images.get(image_name)

                    with cols[col_idx]:
                        if image_path and os.path.exists(image_path):
                            try:
                                st.image(image_path, caption=f"{image_name}\n(Group: {row.get('age_group', 'N/A')})", use_column_width=True)
                                if st.button(f"Zoom 🔍", key=f"btn_zoom_{image_name}_{row.name}"):
                                    toggle_fullscreen(image_name)
                                    st.rerun() # Rerun to show fullscreen or go back
                            except Exception as img_e:
                                st.error(f"Error loading {image_name}: {str(img_e)}")
                        else:
                            st.warning(f"Image not found:\n{image_name}")
                st.markdown("---") # Separator between rows
        else:
             st.info("No images match the current filters.")

    # --- Fullscreen Image Display ---
    else:
        fullscreen_image_name = st.session_state.fullscreen_image
        fullscreen_image_path = all_images.get(fullscreen_image_name)

        if fullscreen_image_path and os.path.exists(fullscreen_image_path):
            st.header(f"Viewing: {fullscreen_image_name}")
            col1, col2 = st.columns([3, 2]) # Image on left, details on right
            with col1:
                 st.image(fullscreen_image_path, caption=fullscreen_image_name, use_column_width=True)

            with col2:
                 st.subheader("Image Details")
                 # Find the corresponding row in the *original* or *filtered* DataFrame
                 # Using filtered_df ensures we only show details for images matching current filters
                 fullscreen_row = filtered_df[filtered_df['filename_jpg'] == fullscreen_image_name]
                 if not fullscreen_row.empty:
                     # Convert row to dict for the display function
                     show_image_details(fullscreen_row.iloc[0].to_dict())
                 else:
                      # Fallback to search in original df if somehow not in filtered (shouldn't happen with correct logic)
                      original_row = df_results[df_results['filename_jpg'] == fullscreen_image_name]
                      if not original_row.empty:
                          st.warning("Displaying details from original data (image might not match all current filters).")
                          show_image_details(original_row.iloc[0].to_dict())
                      else:
                          st.warning("Details not found for this image.")

            if st.button("Close Fullscreen", key="close_fullscreen"):
                st.session_state.fullscreen_image = None
                st.rerun() # Rerun to go back to grid view
        else:
             st.error(f"Fullscreen image '{fullscreen_image_name}' not found. Closing.")
             st.session_state.fullscreen_image = None
             time.sleep(2)
             st.rerun()

    # --- Download Filtered Images ZIP ---
    st.divider()
    st.subheader("Download Images")
    if not filtered_df.empty:
        st.info("Preparing ZIP file for download... This may take a moment for many images.")
        # Pass the current filtered DataFrame and the master image dictionary
        zip_buffer = create_downloadable_zip(filtered_df, all_images)

        if zip_buffer.getbuffer().nbytes > 0:
            st.download_button(
                label="Download Filtered Images as ZIP",
                data=zip_buffer,
                file_name="filtered_ageai_images.zip",
                mime="application/zip",
                key="download_zip_button"
            )
        else:
            # Error message already shown in create_downloadable_zip if it failed
            st.warning("Could not create ZIP file, possibly due to missing image files or other errors.")
    else:
        st.info("No images match the current filters to download.")


# Note: The temporary extracted folder (`temp_extract_path`) is intentionally
# NOT deleted at the end of the script run when data IS loaded, because the
# image paths in `st.session_state.all_images` point directly into it.
# Streamlit's execution model means the script reruns, but the file system state persists
# until the app server stops or the cache/temp files are cleared manually or by the platform.
# Deleting it would break image display on subsequent reruns unless images were copied elsewhere.
