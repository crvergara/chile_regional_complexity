import pandas as pd
import glob
import os
from sqlalchemy import create_engine

# --- CONFIGURACIÓN ---
# ¡OJO! Usa variables de entorno o un archivo .env en el futuro para la password
db_connection = "postgresql://postgres.kmmbfdbysngddtbahqrb:[TU CONTRASEÑA AQUI]@aws-1-sa-east-1.pooler.supabase.com:5432/postgres"

# Construcción de ruta a prueba de fallos
base_dir = os.path.dirname(os.path.abspath(__file__))  # Ruta de este script
ruta_archivos = os.path.join(base_dir, "..", "data", "Exportaciones*202*.txt")

# Nombres de columnas para leer (tal cual vienen en el txt)
indices = [0, 14, 20, 69, 73]
nombres_cols_lectura = ["FECHA", "REGION_ID", "PAIS_DESTINO", "CODIGO_HS", "VALOR_FOB"]

mapa_regiones = {
    "1": "Tarapacá",
    "2": "Antofagasta",
    "3": "Atacama",
    "4": "Coquimbo",
    "5": "Valparaíso",
    "6": "O'Higgins",
    "7": "Maule",
    "8": "Biobío",
    "9": "Araucanía",
    "10": "Los Lagos",
    "11": "Aysén",
    "12": "Magallanes",
    "13": "Metropolitana",
    "14": "Los Ríos",
    "15": "Arica y Parinacota",
    "16": "Ñuble",
    "20": "Indeterminado",
}

engine = create_engine(db_connection)
archivos = glob.glob(ruta_archivos)

print(f"📍 Buscando en: {ruta_archivos}")
print(f"📂 Encontrados {len(archivos)} archivos.")

if not archivos:
    print(
        "⚠️ No se encontraron archivos. Verifica que la carpeta 'data' esté al lado de la carpeta 'app' y que los nombres coincidan."
    )
else:
    print("Iniciando carga masiva...")
    archivos.sort()

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        print(f"Processing: {nombre_archivo}...")

        try:
            df = pd.read_csv(
                archivo,
                sep=";",
                encoding="latin1",
                decimal=",",
                header=None,
                usecols=indices,
                dtype={14: str, 69: str},
            )
            df.columns = nombres_cols_lectura

            # Limpieza de Fecha
            df["FECHA_STR"] = (
                pd.to_numeric(df["FECHA"], errors="coerce")
                .fillna(0)
                .astype(int)
                .astype(str)
                .str.zfill(8)
            )
            df["FECHA_DT"] = pd.to_datetime(
                df["FECHA_STR"], format="%d%m%Y", errors="coerce"
            )

            # Creación de columnas
            df["anio"] = df["FECHA_DT"].dt.year  # Usamos minúsculas directo
            df["mes"] = df["FECHA_DT"].dt.month
            df["nombre_region"] = df["REGION_ID"].map(mapa_regiones).fillna("Otro")
            df["codigo_hs"] = (
                df["CODIGO_HS"].str.replace(".", "", regex=False).str.strip()
            )
            df["pais_destino"] = df["PAIS_DESTINO"]
            df["valor_fob"] = df["VALOR_FOB"]

            # Seleccionar y renombrar para SQL (Todo minúsculas es mejor práctica)
            cols_finales = [
                "anio",
                "mes",
                "nombre_region",
                "pais_destino",
                "codigo_hs",
                "valor_fob",
            ]
            df_clean = df[cols_finales].dropna(subset=["anio"])

            # Carga a SQL
            df_clean.to_sql(
                "exportaciones",
                engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

            print(f"✅ {nombre_archivo} subido exitosamente ({len(df_clean)} filas).")

        except Exception as e:
            print(f"❌ Error en {nombre_archivo}: {e}")

    print("🚀 Proceso finalizado.")
