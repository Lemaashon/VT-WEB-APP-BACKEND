from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import ifcopenshell
import ifcopenshell.util.element
import pandas as pd
from io import BytesIO
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# EXTRACTION LOGIC (simplified integration)
# -------------------------------
CONVERSION_FACTOR = 0.092903

ALLOWED_FAMILIES = ['Antenna', 'RRU', 'Antenna Air', 'Parabola', 'Platform']

COLUMNS_TO_CHECK = [
    'Height', 'Width', 'Ice_Thickness', 'Weight', 'Ice_Weight',
    'Wind_Area', 'Wind_Area_CP', 'Wind_Area_With_Ice', 'Wind_Area_With_Ice_CP'
]

COLUMNS_TO_METRES = [
    'Wind_Area', 'Wind_Area_CP', 'Wind_Area_With_Ice', 'Wind_Area_With_Ice_CP'
]

def _cache_elements(model):
    """Cache commonly used element types from all models."""
    appliances = []
    building_elements = []


    if not model:
        print("Warning: No models loaded. Element cache will be empty.")
        return


    try:
        # Validate model object before using it
        if not hasattr(model, 'by_type'):
            print(f"Warning: Model at index is not a valid IFC file object (type: {type(model)})")
            return None, None

        appliances.extend(model.by_type("IfcElectricAppliance"))
        building_elements.extend(model.by_type("IfcBuildingElementProxy"))
        
        return appliances, building_elements

    except Exception as e:
        print(f"Warning: Error extracting elements from model {e}")
        return None, None

def extract_info(model, id_start=201):

    filtered_data = []
    id = id_start

    properties = model.by_type("IfcPropertySingleValue")
    for prop in properties:
        if prop.Name == 'Site Code':
            # prop.NominalValue returns the IFCTEXT object.
            site_code = prop.NominalValue.wrappedValue
            break
    loaded_equipments = _cache_elements(model)
    for index, equip in enumerate(loaded_equipments):
        if index == 0:
            sched = "Data Device Schedule Existing"
        else:
            sched = "Generic Model Schedule Existing"
        for item in equip:
            psets = ifcopenshell.util.element.get_psets(item)
            data_sched = psets.get(sched, {})
            allowed_families = ['Antenna', 'RRU', 'Antenna Wifi', 'Parabola', 'Platform']
            family_type = data_sched.get("Type Comments", r"\N")
            if family_type in allowed_families:
                row = {
                    "id": id,
                    "Famiglia": family_type,
                    "Tipo": data_sched.get("Type", r"\N"),
                    "Elevation_Load": data_sched.get("Elevation Load", 0)/1000,
                    "CP": data_sched.get("CP", r"\N"),
                    "Height": data_sched.get("Height", 0),
                    "Width": data_sched.get("Width", 0),
                    "Ice_Thickness": data_sched.get("Ice Thickness", 0),
                    "Weight": data_sched.get("Weight", 0),
                    "Ice_Weight": data_sched.get("Ice Weight", 0),
                    "Wind_Area": data_sched.get("Wind Area", 0),
                    "Wind_Area_CP": data_sched.get("Wind Area CP", 0),
                    "Wind_Area_With_Ice": data_sched.get("Wind Area With  Ice", 0),
                    "Wind_Area_With_Ice_CP": data_sched.get("Wind Area With Ice CP", 0),
                    "Installed_on_Pole": data_sched.get("Installed on Pole", r"\N"),
                    "Orientation": data_sched.get("Orientation", r"\N"),
                    "Status": data_sched.get("Phase Created", r"\N"),
                    "KE": site_code
                }
                id += 1
                filtered_data.append(row)

    return filtered_data, id

def safe_round(val):
    if str(val).strip() == r'\N':
        return val
    try:
        return round(float(val), 2)
    except (ValueError, TypeError):
        return val


def safe_convert_to_sqm(val):
    if str(val).strip() == r'\N':
        return val
    try:
        return round(float(val) * CONVERSION_FACTOR, 2)
    except (ValueError, TypeError):
        return val


def replace_zeros_with_null(df, columns):
    for col in columns:
        df[col] = df[col].replace([0, 0.0, '0', '0.0'], r'\N')
    return df


def apply_safe_function(df, columns, func):
    for col in columns:
        df[col] = df[col].apply(func)
    return df


def process_installed_on_pole(df):
    df['Installed_on_Pole'] = df['Installed_on_Pole'].apply(
        lambda x: 'Yes' if str(x).strip() == 'True' else 'No'
    )
    return df


def process_dataframe(df):
    # 1. Filter
    df = df[df['Famiglia'].isin(ALLOWED_FAMILIES)].copy()

    # 2. Replace zeros
    df = replace_zeros_with_null(df, COLUMNS_TO_CHECK)

    # 3. Round values
    df = apply_safe_function(df, COLUMNS_TO_CHECK, safe_round)

    # 4. Convert to sqm (after rounding logic separation is preserved)
    df = apply_safe_function(df, COLUMNS_TO_METRES, safe_convert_to_sqm)

    # 5. Boolean mapping
    df = process_installed_on_pole(df)

    return df
# -------------------------------
# MAIN ENDPOINT
# -------------------------------

@app.post("/upload-ifc")
async def upload_ifc(files: list[UploadFile] = File(...)):

    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    all_data = []
    id_counter = 200
    for file in files:

        try:
            contents = await file.read()

            # Save temporarily in memory
            temp_buffer = BytesIO(contents)

            model = ifcopenshell.file.from_string(temp_buffer.getvalue().decode("utf-8", errors="ignore"))

            extracted, last_id = extract_info(model, id_start=id_counter)
            all_data.extend(extracted)
            id_counter = last_id+1

        except Exception as e:
            print(f"Skipping file {file.filename}: {e}")

    if not all_data:
        raise HTTPException(status_code=400, detail="No valid data extracted")

    df = process_dataframe(pd.DataFrame(all_data))

    # -------------------------------
    # EXPORT TO EXCEL (IN MEMORY)
    # -------------------------------
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Extracted_Data.xlsx"}
    )