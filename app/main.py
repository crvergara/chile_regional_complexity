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


def construir_filtro(anio, region):
    query = " WHERE 1=1"
    params = {}
    if anio != "Todos":
        query += " AND anio = :anio"
        params["anio"] = int(float(anio))
    if region != "Todas":
        query += " AND nombre_region = :region"
        params["region"] = region
    return query, params


@st.cache_data(ttl=3600)
def get_kpis(anio, region):
    filtro, params = construir_filtro(anio, region)
    query = f"""
        SELECT
            SUM(valor_fob) as total_fob,
            COUNT(DISTINCT codigo_hs) as total_productos,
            COUNT(DISTINCT pais_destino) as total_paises
        FROM exportaciones
        {filtro}
    """
    df = conn.query(query, params=params)
    return df.iloc[0]


@st.cache_data(ttl=3600)
def get_evolucion(anio, region):
    filtro, params = construir_filtro(anio, region)
    query = f"""
        SELECT anio,mes, SUM(valor_fob) as valor_fob
        FROM exportaciones
        {filtro}
        GROUP BY anio,mes
        ORDER BY anio,mes
    """
    return conn.query(query, params=params)


@st.cache_data(ttl=3600)
def get_top_productos(anio, region):
    filtro, params = construir_filtro(anio, region)
    query = f"""
        SELECT codigo_hs, SUM(valor_fob) as valor_fob
        FROM exportaciones
        {filtro}
        GROUP BY codigo_hs
        ORDER BY valor_fob DESC
        LIMIT 10
    """
    return conn.query(query, params=params)


@st.cache_data(ttl=3600)
def get_top_paises(anio, region):
    filtro, params = construir_filtro(anio, region)
    query = f"""
        SELECT pais_destino, SUM(valor_fob) as valor_fob
        FROM exportaciones
        {filtro}
        GROUP BY pais_destino
        ORDER BY valor_fob DESC
        LIMIT 10
    """
    return conn.query(query, params=params)


@st.cache_data(ttl=3600)
def get_raw_data(anio, region):
    filtro, params = construir_filtro(anio, region)
    query = f"""
        SELECT anio,mes,nombre_region,pais_destino,codigo_hs,valor_fob
        FROM exportaciones
        {filtro}
        ORDER BY valor_fob DESC
        LIMIT 5000
    """
    return conn.query(query, params=params)


# --- SIDEBAR (FILTROS DESDE LA NUBE) ---
st.sidebar.header("🔍 Filtros")

# Consulta ligera solo para llenar el menú
try:
    df_anios = conn.query("SELECT DISTINCT anio FROM exportaciones ORDER BY anio")
    lista_anios = sorted(
        df_anios["anio"].fillna(0).astype(float).astype(int).astype(str).tolist()
    )
    lista_anios = [x for x in lista_anios if x != "0"]
except:
    lista_anios = []

lista_anios_opts = ["Todos"] + lista_anios
anio_sel = st.sidebar.selectbox(
    "📅 Año:", lista_anios_opts, index=len(lista_anios_opts) - 1
)

try:
    df_regiones = conn.query(
        "SELECT DISTINCT nombre_region FROM exportaciones ORDER BY nombre_region"
    )
    lista_regiones = ["Todas"] + df_regiones["nombre_region"].tolist()
except:
    lista_regiones = ["Todas"]

region_sel = st.sidebar.selectbox("📍 Región:", lista_regiones)

st.title("🚢 Monitor de Exportaciones de Chile")
st.markdown(f"**Filtros Activos:** Año `{anio_sel}` | Región `{region_sel}`")

# 1. Cargar KPIs
kpis = get_kpis(anio_sel, region_sel)
total_millones = kpis["total_fob"] / 1_000_000 if kpis["total_fob"] else 0

col1, col2, col3 = st.columns(3)
col1.metric("💰 Valor Exportado", f"${total_millones:,.0f} M USD")
col2.metric("📦 Productos Únicos", f"{kpis['total_productos']:,.0f} Productos")
col3.metric("🌎 Mercados Destino", f"{kpis['total_paises']:,.0f} Paises")

st.markdown("---")

# 2. Pestañas y Gráficos
tab1, tab2 = st.tabs(["📊 Visión General", "📋 Datos Crudos"])

with tab1:
    # --- Gráfico de Evolución ---
    df_trend = get_evolucion(anio_sel, region_sel)

    if not df_trend.empty:
        # Arreglo de fecha seguro
        df_trend["ANIO"] = df_trend["anio"].astype(int)
        df_trend["MES"] = df_trend["mes"].astype(int)
        df_trend["FECHA_PLOT"] = pd.to_datetime(
            df_trend["ANIO"].astype(str) + "-" + df_trend["MES"].astype(str) + "-01"
        )
        meses_esp = {
            1: "Ene",
            2: "Feb",
            3: "Mar",
            4: "Abr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Ago",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dic",
        }
        df_trend["Periodo"] = (
            df_trend["MES"].map(meses_esp) + "-" + df_trend["ANIO"].astype(str)
        )

        fig_line = px.line(
            df_trend,
            x="FECHA_PLOT",
            y="valor_fob",
            title=f"Tendencia Mensual ({anio_sel})",
            markers=True,
            labels={
                "valor_fob": "Monto exportado USD",
                "FECHA_PLOT": "Fecha",
                "Periodo": "Mes",
            },
            hover_data={"FECHA_PLOT": False, "Periodo": True, "valor_fob": True},
        )
        fig_line.update_layout(hovermode="x unified", xaxis_title=None)

        fig_line.update_xaxes(
            tickformat="%Y-%m",  # Formato limpio: 2024-01
            dtick="M6",  # <--- CLAVE: Muestra una etiqueta solo cada 6 MESES
            tickangle=0,  # Texto recto (o -45 si prefieres inclinado)
        )

        fig_line.update_yaxes(tickformat=".2s")

        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.warning("No hay datos para esta selección.")

    # --- Gráficos de Torta y Barras ---
    col_izq, col_der = st.columns(2)

    with col_izq:
        # 1. Traemos los datos optimizados de la nube
        df_prod = get_top_productos(anio_sel, region_sel)

        if not df_prod.empty:
            # --- CORRECCIÓN CRÍTICA ---
            # 2. Convertimos el código numérico de la DB a string
            df_prod["codigo_hs"] = df_prod["codigo_hs"].astype(str)

            # 3. Aplicamos tu función para obtener el nombre legible
            # (Asegúrate de que la función get_nombre_producto exista arriba)
            df_prod["nombre_prod"] = df_prod["codigo_hs"].apply(get_nombre_producto)

            # 4. Creamos el gráfico usando el NOMBRE, no el código
            fig_bar = px.bar(
                df_prod,
                x="valor_fob",
                y="nombre_prod",  # <-- Usamos la nueva columna con nombres
                orientation="h",
                title="Top 10 Productos Más Exportados",
                labels={"valor_fob": "Valor USD", "nombre_prod": "Producto"},
                text_auto=".2s",  # <-- Agrega el texto con los valores en las barras
            )

            # Ordenamos para que el más grande quede arriba
            fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No hay datos de productos para mostrar.")

    with col_der:
        df_pais = get_top_paises(anio_sel, region_sel)
        if not df_pais.empty:
            fig_pie = px.pie(
                df_pais,
                names="pais_destino",
                values="valor_fob",
                title="Top 10 Destinos",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

with tab2:
    st.info("⚠️ Por rendimiento, se muestran solo las 5.000 transacciones más grandes.")
    df_raw = get_raw_data(anio_sel, region_sel)
    st.dataframe(df_raw, use_container_width=True)


# --- FOOTER ---
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: grey;">
        Desarrollado por <b>Cristóbal Vergara</b> | Ingeniería Civil Industrial UdeC<br>
        <a href="https://www.linkedin.com/in/cristobalvergarajofre/" target="_blank">LinkedIn</a> | 
        <a href="mailto:crvergara2022@udec.cl">Contacto</a>
    </div>
    """,
    unsafe_allow_html=True,
)
