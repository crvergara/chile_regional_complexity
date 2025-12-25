import os
from sqlalchemy import create_engine, text

# Leemos la URL de la base de datos desde las variables de entorno (Secretos)
# Así no ponemos la contraseña en el código
DATABASE_URL = os.environ.get("SUPABASE_URL")

if not DATABASE_URL:
    raise ValueError("No se encontró la variable SUPABASE_URL")

try:
    # 1. Conectar
    engine = create_engine(DATABASE_URL)

    # 2. Ejecutar una consulta mínima (SELECT 1 es el estándar para pings)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("✅ Ping exitoso a Supabase. La base de datos está despierta.")

except Exception as e:
    print(f"❌ Error al hacer ping: {e}")
    exit(1)
