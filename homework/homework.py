"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    import pandas as pd
    import zipfile
    import os
    import glob
    from io import StringIO

    # Crear directorio de salida si no existe
    output_dir = "files/output"
    os.makedirs(output_dir, exist_ok=True)

    # Encontrar todos los archivos .zip en la carpeta input
    input_dir = "files/input"
    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))

    # Lista para almacenar todos los DataFrames
    all_dataframes = []

    # Procesar cada archivo zip
    for zip_file_path in zip_files:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Obtener lista de archivos CSV dentro del zip
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            
            for csv_file in csv_files:
                # Leer el CSV directamente desde el zip
                with zip_ref.open(csv_file) as file:
                    content = file.read().decode('utf-8')
                    df = pd.read_csv(StringIO(content))
                    all_dataframes.append(df)

    # Concatenar todos los DataFrames
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
    else:
        print("No se encontraron archivos CSV en los zips")
        return

    # Crear client_id si no existe (usando el índice)
    if 'client_id' not in combined_df.columns:
        combined_df.reset_index(inplace=True)
        combined_df['client_id'] = combined_df.index

    # Debug: imprimir columnas disponibles
    print("Columnas disponibles:", combined_df.columns.tolist())

    # ========== PROCESAR CLIENT.CSV ==========
    client_columns = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']
    
    client_df = combined_df.copy()

    # Limpiar columna job
    if 'job' in client_df.columns:
        client_df['job'] = client_df['job'].astype(str).str.replace('.', '', regex=False).str.replace('-', '_', regex=False)

    # Limpiar columna education
    if 'education' in client_df.columns:
        client_df['education'] = client_df['education'].astype(str).str.replace('.', '_', regex=False)
        client_df['education'] = client_df['education'].replace('unknown', pd.NA)

    # Convertir credit_default
    if 'credit_default' in client_df.columns:
        client_df['credit_default'] = (client_df['credit_default'] == 'yes').astype(int)
    elif 'default' in client_df.columns:
        client_df['credit_default'] = (client_df['default'] == 'yes').astype(int)

    # Convertir mortgage (buscar diferentes nombres posibles)
    mortgage_col = None
    for col in ['mortgage', 'mortage', 'housing']:
        if col in client_df.columns:
            mortgage_col = col
            break
    
    if mortgage_col:
        client_df['mortgage'] = (client_df[mortgage_col] == 'yes').astype(int)

    # Seleccionar solo las columnas necesarias para client.csv
    client_final_columns = []
    for col in client_columns:
        if col in client_df.columns:
            client_final_columns.append(col)

    if client_final_columns:
        client_final = client_df[client_final_columns].copy()
        client_final.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # ========== PROCESAR CAMPAIGN.CSV ==========
    campaign_df = combined_df.copy()

    # Mapear columnas específicas para campaign
    if 'campaign' in campaign_df.columns:
        campaign_df['number_contacts'] = campaign_df['campaign']
    elif 'contacts' in campaign_df.columns:
        campaign_df['number_contacts'] = campaign_df['contacts']

    if 'duration' in campaign_df.columns:
        campaign_df['contact_duration'] = campaign_df['duration']

    # Para previous_campaign_contacts - buscar más variaciones
    if 'previous_campaign_contacts' not in campaign_df.columns:
        for col in ['previous_campaign_contacts', 'previous', 'pdays', 'prev_contacts', 'previous_contacts']:
            if col in campaign_df.columns:
                campaign_df['previous_campaign_contacts'] = campaign_df[col]
                break
        else:
            # Si no se encuentra ninguna, crear con valores por defecto
            campaign_df['previous_campaign_contacts'] = 0

    # Convertir previous_outcome
    if 'poutcome' in campaign_df.columns:
        campaign_df['previous_outcome'] = (campaign_df['poutcome'] == 'success').astype(int)
    elif 'previous_outcome' in campaign_df.columns:
        campaign_df['previous_outcome'] = (campaign_df['previous_outcome'] == 'success').astype(int)
    else:
        campaign_df['previous_outcome'] = 0

    # Convertir campaign_outcome
    if 'y' in campaign_df.columns:
        campaign_df['campaign_outcome'] = (campaign_df['y'] == 'yes').astype(int)
    elif 'target' in campaign_df.columns:
        campaign_df['campaign_outcome'] = (campaign_df['target'] == 'yes').astype(int)
    elif 'outcome' in campaign_df.columns:
        campaign_df['campaign_outcome'] = (campaign_df['outcome'] == 'yes').astype(int)
    elif 'campaign_outcome' in campaign_df.columns:
        campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'] == 'yes').astype(int)

    # Crear last_contact_date
    if 'day' in campaign_df.columns and 'month' in campaign_df.columns:
        # Debug: ver algunos valores de day y month
        print("Muestra de valores day:", campaign_df['day'].head())
        print("Muestra de valores month:", campaign_df['month'].head())
        
        # Manejar diferentes formatos de month (nombres vs números)
        month_mapping = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
            'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
            'january': '01', 'february': '02', 'march': '03', 'april': '04', 'may': '05', 'june': '06',
            'july': '07', 'august': '08', 'september': '09', 'october': '10', 'november': '11', 'december': '12'
        }
        
        # Convertir month a string y luego mapear si es necesario
        month_series = campaign_df['month'].astype(str).str.lower()
        
        # Si el month es numérico, mantenerlo, si es texto, mapearlo
        def convert_month(month_val):
            if month_val in month_mapping:
                return month_mapping[month_val]
            else:
                # Asumir que es numérico
                try:
                    return str(int(float(month_val))).zfill(2)
                except:
                    return '01'  # valor por defecto
        
        month_numeric = month_series.apply(convert_month)
        
        # Crear fecha como string con formato YYYY-MM-DD
        campaign_df['last_contact_date'] = (
            '2022-' + 
            month_numeric + '-' + 
            campaign_df['day'].astype(str).str.zfill(2)
        )
        
        # Debug: ver algunas fechas generadas
        print("Muestra de fechas generadas:", campaign_df['last_contact_date'].head())
        print("Fechas únicas (primeras 10):", campaign_df['last_contact_date'].unique()[:10])

    # Seleccionar las 7 columnas requeridas para campaign.csv
    campaign_columns = ['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 
                       'previous_outcome', 'campaign_outcome', 'last_contact_date']

    # Verificar que todas las columnas existen
    missing_columns = []
    for col in campaign_columns:
        if col not in campaign_df.columns:
            missing_columns.append(col)
    
    if missing_columns:
        print(f"Advertencia: Faltan las siguientes columnas en campaign: {missing_columns}")
        # Crear columnas faltantes con valores por defecto
        for col in missing_columns:
            if col == 'previous_campaign_contacts':
                campaign_df[col] = 0
            elif col == 'previous_outcome':
                campaign_df[col] = 0
            elif col == 'campaign_outcome':
                campaign_df[col] = 0
            elif col == 'number_contacts':
                campaign_df[col] = 1
            elif col == 'contact_duration':
                campaign_df[col] = 0
            elif col == 'last_contact_date':
                campaign_df[col] = '2022-01-01'

    campaign_final = campaign_df[campaign_columns].copy()
    campaign_final.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # ========== PROCESAR ECONOMICS.CSV ==========
    economics_df = combined_df.copy()

    # Debug: mostrar columnas disponibles para economics
    print("Columnas disponibles para economics:", [col for col in economics_df.columns if 'price' in col.lower() or 'euribor' in col.lower() or 'cons' in col.lower()])

    # Mapear nombres de columnas para economics
    if 'cons.price.idx' in economics_df.columns:
        economics_df['cons_price_idx'] = economics_df['cons.price.idx']
    elif 'cons_price_idx' in economics_df.columns:
        economics_df['cons_price_idx'] = economics_df['cons_price_idx']

    # Para euribor_three_months - buscar todas las variaciones posibles
    if 'euribor_three_months' not in economics_df.columns:
        for col in ['euribor_three_months', 'euribor3m', 'euribor_3m', 'euribor.3m']:
            if col in economics_df.columns:
                economics_df['euribor_three_months'] = economics_df[col]
                break

    # Seleccionar columnas para economics.csv
    economics_columns = ['client_id', 'cons_price_idx', 'euribor_three_months']
    
    # Verificar que todas las columnas existan
    missing_econ_columns = []
    for col in economics_columns:
        if col not in economics_df.columns:
            missing_econ_columns.append(col)
    
    if missing_econ_columns:
        print(f"Advertencia: Faltan las siguientes columnas en economics: {missing_econ_columns}")
        # Crear columnas faltantes con valores por defecto
        for col in missing_econ_columns:
            if col == 'cons_price_idx':
                economics_df[col] = 0.0
            elif col == 'euribor_three_months':
                economics_df[col] = 0.0

    economics_final = economics_df[economics_columns].copy()
    economics_final.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    print("Procesamiento completado. Archivos generados:")
    print("- client.csv")
    print("- campaign.csv") 
    print("- economics.csv")
    
    # Debug: verificar número de columnas en todos los archivos
    campaign_check = pd.read_csv(os.path.join(output_dir, "campaign.csv"))
    print(f"Campaign.csv tiene {len(campaign_check.columns)} columnas: {campaign_check.columns.tolist()}")
    
    economics_check = pd.read_csv(os.path.join(output_dir, "economics.csv"))
    print(f"Economics.csv tiene {len(economics_check.columns)} columnas: {economics_check.columns.tolist()}")
    
    client_check = pd.read_csv(os.path.join(output_dir, "client.csv"))
    print(f"Client.csv tiene {len(client_check.columns)} columnas: {client_check.columns.tolist()}")

    return