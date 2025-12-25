import plotly.express as px
import pandas as pd
import numpy as np
import streamlit as st


st.set_page_config(
    layout="wide", page_title="Monitor de exportaciones Chile 2024", page_icon="cl"
)
mapa_meses = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}
nombres_productos = {
    # --- MINERÍA Y QUÍMICOS ---
    "26030000": "Minerales de Cobre y concentrados",
    "74031100": "Cátodos de Cobre refinado",
    "74020010": "Cobre sin refinar (Blister)",  # Código genérico
    "74020012": "Cobre sin refinar (Ánodos)",  # Variación específica
    "74020013": "Cobre sin refinar (Blister/Otros)",
    "26131010": "Concentrados de Molibdeno",
    "26011110": "Minerales de Hierro",
    "71081200": "Oro en bruto",
    "71081219": "Oro semilabrado (Dore)",  # Variación específica
    "28369100": "Carbonato de Litio",  # Código general
    "28369130": "Carbonato de Litio (Grado Batería)",
    "28369190": "Carbonato de Litio (Otros)",
    "28012000": "Yodo",
    "28342100": "Nitratos de Potasio",
    # --- SALMONES Y PESCA ---
    "03021400": "Salmón del Atlántico (Fresco)",
    "03021410": "Salmón del Atlántico (Fresco/Entero)",
    "03031300": "Salmón del Atlántico (Congelado)",
    "03031310": "Salmón del Atlántico (Congelado/Entero)",
    "03031220": "Salmón del Pacífico (Congelado)",
    "03044100": "Filetes de Salmón (Fresco)",
    "03044120": "Filetes de Salmón del Atlántico (Fresco)",
    "03048100": "Filetes de Salmón (Congelado)",
    "03048110": "Filetes de Salmón del Pacífico (Congelado)",
    "03048120": "Filetes de Salmón del Atlántico (Congelado)",
    "03048200": "Filetes de Trucha (Congelado)",
    "03049946": "Carne de Salmón (Picada/Recortes)",
    "03035511": "Jurel (Congelado)",
    "03044100": "Filetes de Salmón",
    "16055300": "Mejillones (Choritos) preparados",
    "23012010": "Harina de Pescado",
    # --- FRUTAL Y FORESTAL ---
    "47032910": "Celulosa (Pino/Eucalipto)",
    "44071110": "Madera de Pino Insigne aserrada",
    "47079000": "Papel y Cartón (Reciclaje)",
    "08092900": "Cerezas Frescas",
    "08092919": "Cerezas Frescas (Otras)",  # Variación específica
    "08061010": "Uvas Frescas",
    # --- COMBUSTIBLES Y OTROS ---
    "27101991": "Aceites combustibles",
    "00259900": "Servicios / Ajustes varios",  # Código especial de Aduanas (suele ser servicios o correcciones)
}


def get_nombre_producto(codigo):
    if codigo in nombres_productos:
        return nombres_productos[codigo]
    return f"Producto {codigo}"


# conexion supabase
conn = st.connection("supabase", type="sql")


@st.cache_data(ttl=3600)
def cargar_data_cloud(anio, region):
    query = """
    SELECT 
        anio as "ANIO",
        mes as "MES",
        nombre_region as "NOMBRE_REGION",
        pais_destino as "PAIS_DESTINO",
        codigo_hs as "CODIGO_HS",
        valor_fob as "VALOR_FOB"
    FROM exportaciones
    WHERE 1=1
    """
    params = {}
    if anio != "Todos":
        query += " AND anio=:anio"
        params["anio"] = int(float(anio))
    if region != "Todas":
        query += " AND nombre_region=:region"
        params["region"] = region
    return conn.query(query, params=params)


# --- SIDEBAR (FILTROS DESDE LA NUBE) ---
st.sidebar.header("🔍 Filtros Cloud")

# 1. SELECTOR DE AÑOS
# Hacemos una mini-consulta SQL solo para obtener los años únicos
df_anios = conn.query("SELECT DISTINCT anio FROM exportaciones ORDER BY anio")

# ¡OJO AQUÍ! Usamos 'df_anios' (el resultado de la query), NO 'df'
# Y la columna se llama "anio" (minúscula) porque así quedó en Postgres
lista_anios = sorted(
    df_anios["anio"].fillna(0).astype(float).astype(int).astype(str).tolist()
)

# Agregamos opción 'Todos' y seleccionamos el último por defecto
lista_anios_opts = ["Todos"] + lista_anios
anio_sel = st.sidebar.selectbox(
    "📅 Año:", lista_anios_opts, index=len(lista_anios_opts) - 1
)


# 2. SELECTOR DE REGIONES
# Hacemos otra mini-consulta solo para los nombres de regiones
df_regiones = conn.query(
    "SELECT DISTINCT nombre_region FROM exportaciones ORDER BY nombre_region"
)

lista_regiones = ["Todas"] + df_regiones["nombre_region"].tolist()
region_sel = st.sidebar.selectbox("📍 Región:", lista_regiones)

try:
    df_filtrado = cargar_data_cloud(anio_sel, region_sel)

    # Pequeño ajuste para que el mes tenga nombre (si lo necesitas para gráficos)
    # Como en la DB guardamos el número de mes, lo mapeamos aquí rápido
    mapa_meses = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }
    if not df_filtrado.empty:
        df_filtrado["NOMBRE_MES"] = df_filtrado["MES"].map(mapa_meses)

except Exception as e:
    st.error(f"Error de conexión: {e}")
    st.stop()

# TÍTULO DINÁMICO
if region_sel == "Todas" and anio_sel == "Todos":
    titulo = "Exportaciones Históricas de Chile"
elif region_sel != "Todas" and anio_sel == "Todos":
    titulo = f"Histórico de {region_sel}"
else:
    titulo = f"Exportaciones {region_sel} - {anio_sel}"


st.title(titulo)


# Metricas
total_exportado = df_filtrado["VALOR_FOB"].sum()
total_mill = total_exportado / 1000000
col1, col2, col3 = st.columns(3)
col1.metric("💰 Total exportado", f"{total_mill:.2f} M$")
col2.metric("📦 Cantidad de Productos", f"{df_filtrado['CODIGO_HS'].nunique():,} Items")
col3.metric("🌎 Destinos", f"{df_filtrado['PAIS_DESTINO'].nunique()} Paises")

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["📊 Visión General", "📈 Detalle", "📋 Datos Crudos"])

# 2. PESTAÑA 1: TU DASHBOARD VISUAL (Aquí va tu código)
with tab1:
    st.header("Panorama General")

    # Creamos las columnas DENTRO de la pestaña
    col_izq, col_der = st.columns([2, 1])

    if not df_filtrado.empty:
        st.subheader("Evolucion de exportaciones 2020 - 2024")
        df_trend = df_filtrado.groupby(["ANIO", "MES"])["VALOR_FOB"].sum().reset_index()
        df_trend["ANIO"] = df_trend["ANIO"].fillna(0).astype(int)
        df_trend["MES"] = df_trend["MES"].fillna(0).astype(int)

        df_trend["FECHA_PLOT"] = pd.to_datetime(
            df_trend["ANIO"].astype(str) + "-" + df_trend["MES"].astype(str) + "-01"
        )
        fig_line = px.line(
            df_trend,
            x="FECHA_PLOT",
            y="VALOR_FOB",
            markers=True,
            labels={"VALOR_FOB": "Valor exportado USD", "FECHA_PLOT": "Fecha"},
            title=f"Tendencia mensual -{region_sel} ",
        )
        fig_line.update_layout(
            hovermode="x unified"
        )  # Al pasar el mouse muestra info detallada
        st.plotly_chart(fig_line, use_container_width=True)

        st.markdown("---")  # Separador visual

    # GRAFICO TOP PRODUCTOS (Tu código indentado)
    with col_izq:
        st.subheader("Top 10 Productos mas exportados")

        # Lógica de datos
        top_productos = df_filtrado.groupby("CODIGO_HS", as_index=False)[
            "VALOR_FOB"
        ].sum()
        top_productos = top_productos.sort_values("VALOR_FOB", ascending=False).head(10)
        top_productos["CODIGO_HS"] = top_productos["CODIGO_HS"].astype(str)
        top_productos["NOMBRE_PROD"] = top_productos["CODIGO_HS"].apply(
            get_nombre_producto
        )

        # Crear gráfico
        fig_prod = px.bar(
            top_productos,
            x="VALOR_FOB",
            y="NOMBRE_PROD",
            orientation="h",
            title="Productos mas exportados",
            labels={"VALOR_FOB": "Valor USD", "CODIGO_HS": "Codigo Producto"},
            text_auto=".2s",
        )
        fig_prod.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_prod, use_container_width=True)

    # GRAFICO TOP DESTINOS (Tu código indentado)
    with col_der:
        st.subheader("Top 5 Destinos")

        # Lógica de datos
        top_paises = df_filtrado.groupby("PAIS_DESTINO", as_index=False)[
            "VALOR_FOB"
        ].sum()
        top_paises = (top_paises.sort_values("VALOR_FOB", ascending=False)).head(5)

        # Crear gráfico
        fig_pie = px.pie(
            top_paises,
            names="PAIS_DESTINO",
            values="VALOR_FOB",
            title="Destinos mas exportados",
            hole=0.4,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# 3. PESTAÑA 2: Oportunidad de expansión (Placeholder)
with tab2:
    st.header("Análisis Detallado")
    st.info("🚧 Gráfico de línea temporal (Enero-Diciembre) en proceso.")

# 4. PESTAÑA 3: Datos Crudos (La tabla)
with tab3:
    st.header("Base de Datos Filtrada")
    # Movemos la tabla aquí para limpiar la vista principal
    st.dataframe(
        df_filtrado.sort_values("VALOR_FOB", ascending=False), use_container_width=True
    )
