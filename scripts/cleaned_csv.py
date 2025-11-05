import pandas as pd
import os

# Lista de archivos CSV de videos y sus códigos de país
file_info = [
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

# Lista para almacenar todos los DataFrames individuales
all_dfs = []

print("Iniciando la carga y combinación de archivos...")

for file_name, country_code in file_info:
    if os.path.exists(file_name):
        try:
            # Se intenta la lectura, con 'latin1' como alternativa.
            try:
                df = pd.read_csv(file_name)
            except UnicodeDecodeError:
                df = pd.read_csv(file_name, encoding='latin1')

            # 1. Agregar la columna 'country' al final
            df['country'] = country_code

            # 2. Reordenar las columnas para mover 'country' a la posición 1 (segunda columna)
            
            # Obtener la lista actual de nombres de columnas
            cols = list(df.columns)
            
            # Mover 'country' de su posición final a la posición 1
            # Eliminar 'country' de la posición actual
            cols.remove('country')
            
            # Insertar 'country' en la posición de índice 1 (segunda columna)
            cols.insert(1, 'country')
            
            # Aplicar el nuevo orden de columnas al DataFrame
            df = df[cols]

            all_dfs.append(df)
            print(f"✅ Cargado: {file_name} (País: {country_code}, 'country' en pos 2)")

        except Exception as e:
            print(f"❌ Error al leer {file_name}: {e}")
    else:
        print(f"⚠️ Archivo no encontrado: {file_name}. Saltando...")

# Concatenar todos los DataFrames cargados
if all_dfs:
    # Se usa ignore_index=True para que el nuevo DataFrame tenga un índice secuencial
    combined_df = pd.concat(all_dfs, ignore_index=True)

    output_file_name = 'combined_videos.csv'
    combined_df.to_csv(output_file_name, index=False)
    print("\n--------------------------------------------------")
    print(f"🎉 Éxito: Todos los archivos se combinaron en {output_file_name}.")
    print(f"Filas totales en el nuevo archivo: {len(combined_df)}")
    print("--------------------------------------------------")

    # Muestra las primeras filas para confirmar el orden de las columnas
    print("Columnas del DataFrame combinado (las primeras 3):")
    print(combined_df.iloc[:, :3].head())
else:
    print("\n🚫 No se pudo cargar ningún archivo CSV. Asegúrate de que estén en la misma carpeta.")