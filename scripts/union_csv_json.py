import pandas as pd
import json
import os

# --- PARTE 1: Configuración de Archivos ---
csv_file_info = [
    ('CAvideos.csv', 'CA'),
    ('DEvideos.csv', 'DE'),
    ('FRvideos.csv', 'FR'),
    ('GBvideos.csv', 'GB'),
    ('INvideos.csv', 'IN'),
    ('JPvideos.csv', 'JP'),
    ('KRvideos.csv', 'KR'),
    ('MXvideos.csv', 'MX'),
    ('RUvideos.csv', 'RU'),
    ('USvideos.csv', 'US')
]

json_file_info = [
    ('CA_category_id.json', 'CA'),
    ('DE_category_id.json', 'DE'),
    ('FR_category_id.json', 'FR'),
    ('GB_category_id.json', 'GB'),
    ('IN_category_id.json', 'IN'),
    ('JP_category_id.json', 'JP'),
    ('KR_category_id.json', 'KR'),
    ('MX_category_id.json', 'MX'),
    ('RU_category_id.json', 'RU'),
    ('US_category_id.json', 'US')
]

all_dfs = []
all_categories_dfs = []

# --- PARTE 2: Carga y Consolidación de CSVs (con columna 'country' en Posición 2) ---
print("Iniciando la carga y combinación de archivos CSV...")
for file_name, country_code in csv_file_info:
    if os.path.exists(file_name):
        try:
            # Manejo de codificación
            try:
                df = pd.read_csv(file_name)
            except UnicodeDecodeError:
                df = pd.read_csv(file_name, encoding='latin1')

            # Agregar y reordenar la columna 'country' a la posición 1 (segunda columna)
            df['country'] = country_code
            cols = list(df.columns)
            cols.remove('country')
            cols.insert(1, 'country') # Inserta en la posición del índice 1
            df = df[cols]
            
            all_dfs.append(df)
            print(f"✅ Cargado: {file_name}")

        except Exception as e:
            print(f"❌ Error al leer {file_name}: {e}")
    else:
        print(f"⚠️ Archivo no encontrado: {file_name}. Saltando...")

if not all_dfs:
    print("\n🚫 No se cargó ningún archivo CSV. Abortando el proceso.")
else:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nCSV combinados listos. Filas totales: {len(combined_df):,}")

    # --- PARTE 3: Carga y Consolidación de JSONs ---
    print("\nIniciando la consolidación de los archivos JSON de categorías...")
    for file_name, _ in json_file_info:
        if os.path.exists(file_name):
            try:
                with open(file_name, 'r', encoding='utf8') as f:
                    data = json.load(f)

                df_cat = pd.json_normalize(data['items'])
                df_cat = df_cat[['id', 'snippet.title']]
                df_cat.columns = ['category_id', 'category_name']
                df_cat['category_id'] = pd.to_numeric(df_cat['category_id'])

                all_categories_dfs.append(df_cat)
                
            except Exception as e:
                print(f"❌ Error al procesar {file_name}: {e}")
        
    if all_categories_dfs:
        # Crea la tabla de mapeo única
        category_map_df = pd.concat(all_categories_dfs).drop_duplicates(subset=['category_id']).reset_index(drop=True)
        print("Tabla de mapeo de categorías consolidada y lista.")

        # --- PARTE 4: Unión (Merge) e Inserción de la Nueva Columna ---
        # Unir el DataFrame de videos con el mapa de categorías
        final_df = combined_df.merge(category_map_df, on='category_id', how='left')

        # === LÓGICA CLAVE: Reordenar para poner 'category_name' al lado de 'category_id' ===
        
        # 1. Obtener la lista de columnas actual
        cols = final_df.columns.tolist()
        
        # 2. Encontrar el índice de 'category_id'
        cat_id_index = cols.index('category_id')
        
        # 3. Mover 'category_name'
        # Primero, la quitamos del final
        cols.remove('category_name') 
        # Luego, la insertamos justo después de 'category_id' (índice: cat_id_index + 1)
        cols.insert(cat_id_index + 1, 'category_name')

        # 4. Aplicar el nuevo orden
        final_df = final_df[cols]
        
        # --- PARTE 5: Guardar el Resultado Final ---
        output_file_name = 'youtube_videos_final.csv'
        final_df.to_csv(output_file_name, index=False)

        print("\n--------------------------------------------------")
        print(f"🎉 Proceso completado con éxito.")
        print(f"El archivo final se guardó como {output_file_name}.")
        print("Verificación del nuevo orden de columnas (category_id y category_name):")
        # Muestra las columnas clave para verificación
        print(final_df[['video_id', 'country', 'category_id', 'category_name', 'title']].head())
        print("--------------------------------------------------")
        
    else:
        print("\n🚫 Error: No se pudo crear la tabla de categorías a partir de los JSON.")