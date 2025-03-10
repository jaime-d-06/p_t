import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import tempfile
import os
import re  
from datetime import datetime 
from pandas.io.excel import ExcelWriter 

# Configuración de PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "nueva_base",
    "user": "jaime",
    "password": "180706xX",
    "port": "5432"
}

# --- Funciones scripts originales (adaptadas) ---
def combinar_archivos(tiquets_file, detalle_file):
    tiquets_df = pd.read_excel(tiquets_file)  # Usar el archivo subido, no uno estático
    detalle_df = pd.read_excel(detalle_file).rename(columns={'Ticket': 'id', 'ID': 'C_Id'})
    
    merged_df = pd.merge(tiquets_df, detalle_df, on='id', how='inner')
    
    resultado_df = merged_df[[
        'C_Id', 'id', 'Servei/Projecte - OPLES', 'Assumpte', 
        'Time Taken', 'Creat per', 'Creat', 'Contingut'
    ]].rename(columns={
        'Creat per': 'Responsable',
        'Creat': 'Fecha_Creacion',
        'Time Taken': 'Tiempo_Minutos',
        'Contingut': 'Detalle_Actividad',
        'Assumpte':'Asunto'
    })
    
    return resultado_df  # ¡Retornar el DataFrame!


def insertar_en_bd(df):                          

    # Configuración de PostgreSQL
    config_db = {
        "host": "localhost",
        "database": "nueva_base",
        "user": "jaime",
        "password": "180706xX",
        "port": "5432",
        "client_encoding": "UTF8"
    }

    def obtener_registros_existentes(cursor):
        """Obtiene C_Id existentes en la base de datos para evitar duplicados"""
        cursor.execute("SELECT C_Id FROM servicios_registro")
        return {row[0] for row in cursor.fetchall()}

    def filtrar_duplicados(df, cursor):
        """Filtra registros usando C_Id como identificador único"""
        existentes = obtener_registros_existentes(cursor)
        
        nuevos_registros = set()
        datos_filtrados = []
        
        for registro in df.itertuples(index=False):
            c_id = registro.C_Id
            if c_id not in existentes and c_id not in nuevos_registros:
                nuevos_registros.add(c_id)
                datos_filtrados.append(registro)
        
        return pd.DataFrame(datos_filtrados, columns=df.columns)

    def limpiar_tiempo(tiempo):
        """Convierte el tiempo a minutos enteros (ya está en minutos en el Excel)"""
        try:
            return int(tiempo)
        except:
            return 0

    def limpiar_fecha(fecha):
        """Normaliza fechas a formato 'MM/YYYY'"""
        try:
            if isinstance(fecha, pd.Timestamp):
                return fecha.strftime("%m/%Y")
            
            if isinstance(fecha, str):
                fecha_part = fecha.split()[0]  # Eliminar hora si existe
                dt = datetime.strptime(fecha_part, "%Y-%m-%d")
                return dt.strftime("%m/%Y")
            
            return None
        except Exception as e:
            print(f"Error procesando fecha: {fecha} → {str(e)}")
            return None

    def limpiar_c_id(c_id):
        """Convierte C_Id a entero"""
        try:
            return int(c_id)
        except:
            return None

    df = df.rename(columns={
        'Servei/Projecte - OPLES': 'Servicios',
        'Tiempo_Minutos': 'Tiempo_Trabajado',
        'Fecha_Creacion': 'Fecha',
        'Detalle_Actividad': 'Descripcion'
    }).dropna(subset=['Servicios', 'id', 'C_Id'])

    # Aplicar transformaciones
    df['Tiempo_Trabajado'] = df['Tiempo_Trabajado'].apply(limpiar_tiempo)
    df['Fecha'] = df['Fecha'].apply(limpiar_fecha)
    df['C_Id'] = df['C_Id'].apply(limpiar_c_id)  # Limpiar C_Id

    # Filtrar datos válidos
    df_validos = df.dropna(subset=['Fecha', 'C_Id']).sort_values(by='Fecha', ascending=False)

    # Inserción en PostgreSQL
    try:
        with psycopg2.connect(**config_db) as conn:
            with conn.cursor() as cursor:
                # Filtrar registros existentes
                df_final = filtrar_duplicados(df_validos, cursor)
                
                # Insertar solo nuevos registros
                if not df_final.empty:
                    cursor.executemany("""
                        INSERT INTO servicios_registro 
                        (C_Id, ID, Servicios, Asunto, Tiempo_Trabajado, Responsable, Fecha, Descripcion)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                    """, df_final.itertuples(index=False, name=None))
                    conn.commit()
                    print(f"\n🎉 Datos insertados: {len(df_final)} registros nuevos")
                    print("📅 Distribución mensual:")
                    print(df_final['Fecha'].value_counts().sort_index(ascending=False))
                else:
                    print("\n✅ No se encontraron registros nuevos para insertar")

    except Exception as e:
        print(f"\n❌ Error en base de datos: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def generar_informe():

    # Configuración de conexión a la base de datos PostgreSQL
    configuracion_bd = {
        "host": "localhost",            
        "database": "nueva_base",       
        "user": "jaime",            
        "password": "180706xX",     
        "port": "5432"              
    }

    def exportar_datos_consolidados():
        """Función principal que ejecuta todo el proceso de extracción, transformación y exportación"""
        try:
            # Creación del motor de SQLAlchemy
            engine = create_engine(
                f"postgresql+psycopg2://{configuracion_bd['user']}:{configuracion_bd['password']}"
                f"@{configuracion_bd['host']}:{configuracion_bd['port']}/{configuracion_bd['database']}",
                connect_args={'options': '-c search_path=public'}
            )
            
            # Consultas para resumen y horas por persona
            consulta_servicios = """
                SELECT 
                    servicios AS servicio,
                    SUM(tiempo_trabajado) AS tiempo_total,
                    fecha AS mes_año
                FROM servicios_registro
                GROUP BY servicios, fecha
                ORDER BY 
                    TO_DATE(fecha, 'MM/YYYY') DESC,
                    SUM(tiempo_trabajado) DESC
            """
            
            consulta_personas = """
                SELECT 
                    responsable AS persona,
                    SUM(tiempo_trabajado) AS tiempo_total,
                    fecha AS mes_año
                FROM servicios_registro
                GROUP BY responsable, fecha
                ORDER BY 
                    TO_DATE(fecha, 'MM/YYYY') DESC,
                    SUM(tiempo_trabajado) DESC
            """
            
            # Consulta modificada para mantener el ID del tiquet
            consulta_detalle = """
                SELECT 
                    servicios AS servicio,
                    ID AS tiquet,
                    asunto AS descripcion,
                    fecha AS mes_año,
                    tiempo_trabajado AS minutos_dedicados
                FROM servicios_registro
                ORDER BY 
                    TO_DATE(fecha, 'MM/YYYY') DESC,
                    servicios,
                    ID,
                    asunto
            """
            # Ejecutar consultas
            df_servicios = pd.read_sql(consulta_servicios, engine)
            df_personas = pd.read_sql(consulta_personas, engine)
            df_detalle = pd.read_sql(consulta_detalle, engine)
            
            # Transformación de datos para hojas resumen
            df_servicios['tiempo_total'] = df_servicios['tiempo_total'].astype(int)
            df_personas['tiempo_total'] = df_personas['tiempo_total'].astype(int)
            
            # Agrupar tiquets por descripción y mes, manteniendo los IDs
            df_detalle['fecha_orden'] = pd.to_datetime(df_detalle['mes_año'], format='%m/%Y')
            df_detalle = df_detalle.groupby(
                ['servicio', 'tiquet', 'descripcion', 'mes_año']
            ).agg(
                minutos_dedicados=('minutos_dedicados', 'sum'),
                fecha_orden=('fecha_orden', 'first')
            ).reset_index()
            
            # Ordenar por fecha descendente y otros criterios
            df_detalle = df_detalle.sort_values(
                ['fecha_orden', 'servicio', 'descripcion'],
                ascending=[False, True, True]
            ).drop('fecha_orden', axis=1)
            
            df_detalle['minutos_dedicados'] = df_detalle['minutos_dedicados'].astype(int)

            # Exportación a Excel
            output_path = 'informe_consolidado.xlsx'  # Definir variable
            with ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Hojas Resumen y Horas por Persona 
                df_servicios.to_excel(
                    writer,
                    index=False,
                    sheet_name='Resumen',
                    columns=['servicio', 'tiempo_total', 'mes_año'],
                    header=['Servicio', 'Tiempo Total (minutos)', 'Mes/Año']
                )
                
                df_personas.to_excel(
                    writer,
                    index=False,
                    sheet_name='Horas por Persona',
                    columns=['persona', 'mes_año', 'tiempo_total'],
                    header=['Persona','Mes/Año', 'Tiempo Total (minutos)']
                )
                
                # Hoja Detalle modificada
                df_detalle.to_excel(
                    writer,
                    index=False,
                    sheet_name='Detalle Tiquets',
                    columns=['servicio', 'tiquet', 'descripcion', 'mes_año', 'minutos_dedicados'],
                    header=['Servicio', 'N° Tiquet', 'Descripción', 'Mes/Año', 'Minutos Dedicados']
                )
                
                # Formateo común
                libro = writer.book
                estilo_encabezado = libro.add_format({
                    'bold': True,
                    'bg_color': '#2E75B6',
                    'font_color': 'white',
                    'border': 1,
                    'align': 'center'
                })
                estilo_minutos = libro.add_format({
                    'num_format': '#,##0',
                    'align': 'center'
                })
                
                # Configuración de columnas actualizada
                for sheet_name, config in [
                    ('Resumen', [
                        ('A:A', 35, 'left'),
                        ('B:B', 20, estilo_minutos),
                        ('C:C', 15, 'center')
                    ]),
                    ('Horas por Persona', [
                        ('A:A', 35, 'left'),
                        ('B:B', 15, 'center'),
                        ('C:C', 20, estilo_minutos)
                    ]),
                    ('Detalle Tiquets', [
                        ('A:A', 35, 'left'),     # Servicio
                        ('B:B', 25, 'center'),   # Tiquets
                        ('C:C', 50, 'left'),     # Descripción
                        ('D:D', 15, 'center'),   # Mes/Año
                        ('E:E', 20, estilo_minutos)  # Minutos
                    ])
                ]:
                    hoja = writer.sheets[sheet_name]
                    for col_config in config:
                        formato = libro.add_format({'align': col_config[2]}) if isinstance(col_config[2], str) else col_config[2]
                        hoja.set_column(col_config[0], col_config[1], formato)
                    
                    for col_num, value in enumerate(hoja.header):
                        hoja.write(0, col_num, value, estilo_encabezado)

            print(f"\n✅ Excel generado correctamente con 3 hojas: Resumen, Horas por Persona y Detalle Agrupado")
            return output_path
        except Exception as e:
            print(f"\n❌ Error crítico: {str(e)}")
            exit()

    exportar_datos_consolidados()
    return 'informe_consolidado.xlsx'  # Retornar la ruta del archivo

# Interfaz de Streamlit
st.title("🔄 Automatizador de Informes OPLES")

uploaded_tiquets = st.file_uploader("Sube Tiquets.xlsx", type="xlsx")
uploaded_detalle = st.file_uploader("Sube Detalle.xlsx", type="xlsx")

if uploaded_tiquets and uploaded_detalle and st.button("Generar Informe"):
    with st.spinner("Procesando..."):
        # Paso 1: Combinar archivos
        combined_df = combinar_archivos(uploaded_tiquets, uploaded_detalle)
        
        # Paso 2: Insertar en PostgreSQL
        insertar_en_bd(combined_df)  # Pasar el DataFrame directamente
        
        # Paso 3: Generar informe final
        output_file = generar_informe()  # Ahora retorna la ruta
        
        if output_file and os.path.exists(output_file):
            # Descargar el archivo
            with open(output_file, "rb") as f:
                st.download_button(
                    label="📥 Descargar Informe Consolidado",
                    data=f,
                    file_name="informe_consolidado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            st.success("✅ Proceso completado!")
        else:
            st.error("❌ No se pudo generar el informe")
