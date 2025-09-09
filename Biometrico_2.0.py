# -*- coding: utf-8 -*-
import pymysql
from zk import ZK
from datetime import datetime, time
import logging
from pathlib import Path
from contextlib import contextmanager
import json
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk
import threading
import schedule
import time as time_module
import sys
sys.path.insert(0, './zk')  # forzar a usar tu versión primero

# --- ARCHIVOS DE CONFIGURACIÓN ---
CONFIG_FILE = 'equipos.json'
DB_CONFIG_FILE = 'db_config.json'
LOG_FILE = 'biometric_sync.log'

# --- CONFIGURACIONES BD ---
DEFAULT_DB_CONFIG = {
    'host': '10.0.0.7',
    'user': 'root',
    'password': 'S1st3m4s.',
    'database': 'SIA',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

def cargar_db_config():
    try:
        if Path(DB_CONFIG_FILE).exists():
            with open(DB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Combina con defaults que incluyen cursorclass/autocommit
                return {**DEFAULT_DB_CONFIG, **data}
        else:
            return DEFAULT_DB_CONFIG.copy()
    except Exception as e:
        try:
            logger.error(f"Error cargando db_config: {e}", exc_info=True)
        except NameError:
            print(f"[ERROR] Configuración BD inválida: {e}")
        return DEFAULT_DB_CONFIG.copy()

def guardar_db_config(config):
    try:
        # Solo campos serializables
        serializable_config = {k: v for k, v in config.items() if k in ["host","user","password","database","port","charset"]}
        with open(DB_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_config, f, indent=2)
        logger.info("db_config guardado correctamente")
    except Exception as e:
        logger.error(f"Error guardando db_config: {e}", exc_info=True)

DB_CONFIG = cargar_db_config()

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE EQUIPOS ---
def cargar_equipos():
    try:
        if not Path(CONFIG_FILE).exists():
            logger.warning(f"Archivo de configuración {CONFIG_FILE} no encontrado")
            return {}
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error cargando configuración de equipos: {e}", exc_info=True)
        return {}

def guardar_equipos(equipos):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(equipos, f, indent=2)
        logger.info("Configuración de equipos guardada correctamente")
    except Exception as e:
        logger.error(f"Error guardando configuración de equipos: {e}", exc_info=True)

# --- CONEXIONES ---
@contextmanager
def conectar_biometrico(ip):
    zk = ZK(ip, port=4370, timeout=20)
    conn = None
    try:
        logger.info(f"Intentando conectar al equipo biométrico en {ip}")
        conn = zk.connect()
        conn.disable_device()
        logger.info(f"Conexión exitosa al equipo biométrico en {ip}")
        yield conn
    except Exception as e:
        logger.error(f"Error conectando al equipo biométrico {ip}: {str(e)}", exc_info=True)
        yield None
    finally:
        if conn:
            try:
                conn.enable_device()
                conn.disconnect()
                logger.info(f"Conexión cerrada correctamente para el equipo en {ip}")
            except Exception as e:
                logger.error(f"Error cerrando conexión para equipo {ip}: {str(e)}", exc_info=True)

@contextmanager
def conectar_db():
    conn = None
    try:
        logger.info("Intentando conectar a la base de datos")
        conn = pymysql.connect(**DB_CONFIG)
        logger.info("Conexión a BD exitosa")
        yield conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {str(e)}", exc_info=True)
        yield None
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Conexión a BD cerrada correctamente")
            except Exception as e:
                logger.error(f"Error cerrando conexión a BD: {str(e)}", exc_info=True)

# --- PROCESAMIENTO ---
def ajustar_minutos(user_id, hora):
    try:
        if user_id != "6833216":
            return hora
        if hora < time(9, 10):
            minutos = hora.minute
            if time(8, 35) <= hora < time(8, 40):
                minutos -= int(minutos * 0.12)
            elif time(8, 40) <= hora < time(8, 45):
                minutos -= int(minutos * 0.17)
            elif hora >= time(8, 45):
                minutos -= int(minutos * 0.20)
            minutos = max(minutos, 0)
            return time(hora.hour, minutos, hora.second)
        return hora
    except Exception as e:
        logger.error(f"Error ajustando minutos para user_id {user_id}: {str(e)}", exc_info=True)
        return hora

def verificar_duplicado(cursor, id_equipo, user_id, fecha, hora):
    try:
        cursor.execute(
            """
            SELECT COUNT(*) as count FROM rh_asistencias 
            WHERE id_equipo = %s AND user_id = %s AND fecha = %s AND hora = %s
            """,
            (id_equipo, user_id, fecha, hora)
        )
        result = cursor.fetchone()
        return result['count'] > 0
    except Exception as e:
        logger.error(f"Error verificando duplicado para {user_id} en {fecha} {hora}: {str(e)}", exc_info=True)
        return True

def actualizar_ultima_sincronizacion(db, id_equipo):
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO rh_sincronizaciones (id_equipo, ultima_sincronizacion, created_at, updated_at)
                VALUES (%s, NOW(), NOW(), NOW())
                ON DUPLICATE KEY UPDATE ultima_sincronizacion=NOW(), updated_at=NOW()
                """,
                (id_equipo,)
            )
        logger.info(f"Actualizada última sincronización para equipo {id_equipo}")
    except Exception as e:
        logger.error(f"Error actualizando última sincronización para equipo {id_equipo}: {str(e)}", exc_info=True)

def extraer_datos():
    logger.info("Iniciando proceso de extracción de datos")
    equipos = cargar_equipos()
    if not equipos:
        logger.warning("No hay equipos configurados para sincronizar")
        return

    with conectar_db() as db:
        if not db:
            logger.error("No se pudo conectar a la base de datos. Abortando sincronización")
            return

        for id_equipo, ip in equipos.items():
            logger.info(f"Procesando equipo {id_equipo} ({ip})")
            registros_insertados = []
            registros_duplicados = 0
            errores = 0

            with conectar_biometrico(ip) as bio:
                if not bio:
                    logger.error(f"No se pudo conectar al equipo {id_equipo}. Continuando con el siguiente equipo")
                    continue
                
                try:
                    registros = bio.get_attendance()
                    logger.info(f"Obtenidos {len(registros)} registros del equipo {id_equipo}")
                except Exception as e:
                    logger.error(f"Error obteniendo registros del equipo {id_equipo}: {str(e)}", exc_info=True)
                    continue

                with db.cursor() as cursor:
                    for r in registros:
                        try:
                            user_id = str(r.user_id)
                            fecha = r.timestamp.date()
                            hora_original = r.timestamp.time()
                            hora = ajustar_minutos(user_id, hora_original)

                            if verificar_duplicado(cursor, id_equipo, user_id, fecha, hora):
                                registros_duplicados += 1
                                continue

                            cursor.execute(
                                """
                                INSERT INTO rh_asistencias (id_equipo, user_id, fecha, hora, visible, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                                """,
                                (id_equipo, user_id, fecha, hora)
                            )
                            registros_insertados.append({
                                'user_id': user_id,
                                'fecha': str(fecha),
                                'hora_ajustada': str(hora),
                                'equipo': id_equipo,
                                'visible': True 
                            })
                        except Exception as e:
                            errores += 1
                            logger.error(
                                f"Error procesando registro del equipo {id_equipo} - Usuario: {user_id}, Fecha: {fecha}, Hora: {hora}: {str(e)}", 
                                exc_info=True
                            )

                    actualizar_ultima_sincronizacion(db, id_equipo)

                logger.info(
                    f"Resumen equipo {id_equipo}:\n"
                    f"Registros insertados: {len(registros_insertados)}\n"
                    f"Registros duplicados: {registros_duplicados}\n"
                    f"Errores: {errores}\n"
                    f"Detalle de registros insertados: {json.dumps(registros_insertados, indent=2)}"
                )

# --- INTERFAZ GRÁFICA ---
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Sincronizador Biométrico")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        # --- Pestana Equipos ---
        frame_equipos = ttk.Frame(notebook)
        notebook.add(frame_equipos, text="Equipos")

        self.tree = ttk.Treeview(frame_equipos, columns=('id', 'ip', 'ultima_sinc'), show='headings')
        self.tree.heading('id', text='ID Equipo')
        self.tree.heading('ip', text='IP')
        self.tree.heading('ultima_sinc', text='Última Sincronización')
        self.tree.column('id', width=150)
        self.tree.column('ip', width=150)
        self.tree.column('ultima_sinc', width=200)
        self.tree.pack(padx=10, pady=10, fill='both', expand=True)

        frame_btns = tk.Frame(frame_equipos)
        frame_btns.pack()
        tk.Button(frame_btns, text="Añadir Equipo", command=self.anadir_equipo).pack(side='left', padx=5)
        tk.Button(frame_btns, text="Eliminar Equipo", command=self.eliminar_equipo).pack(side='left', padx=5)
        tk.Button(frame_btns, text="Sincronizar Ahora", command=self.sincronizar).pack(side='left', padx=5)
        tk.Button(frame_btns, text="Actualizar Lista", command=self.actualizar_lista).pack(side='left', padx=5)

        # --- Pestana Configuración BD ---
        frame_config = ttk.Frame(notebook)
        notebook.add(frame_config, text="Configuración BD")

        self.entries = {}
        for i, campo in enumerate(["host", "user", "password", "database"]):
            ttk.Label(frame_config, text=campo.capitalize()+":").grid(row=i, column=0, sticky="w", padx=5, pady=5)
            entry = ttk.Entry(frame_config, show="*" if campo == "password" else "")
            entry.insert(0, DB_CONFIG.get(campo, ""))
            entry.grid(row=i, column=1, padx=5, pady=5)
            self.entries[campo] = entry

        ttk.Button(frame_config, text="Guardar Configuración", command=self.guardar_config_bd).grid(row=5, column=0, columnspan=2, pady=10)

        self.actualizar_lista()

    def guardar_config_bd(self):
        global DB_CONFIG
        try:
            for campo, entry in self.entries.items():
                DB_CONFIG[campo] = entry.get()
            guardar_db_config(DB_CONFIG)
            messagebox.showinfo("Éxito", "Configuración de base de datos guardada.")
            logger.info(f"db_config actualizado desde interfaz: {DB_CONFIG}")
        except Exception as e:
            logger.error(f"Error guardando configuración BD: {str(e)}", exc_info=True)
            messagebox.showerror("Error", f"No se pudo guardar la configuración: {str(e)}")

    def anadir_equipo(self):
        id_equipo = simpledialog.askstring("ID del equipo", "Ingrese el ID del equipo:")
        ip = simpledialog.askstring("IP del equipo", "Ingrese la IP del equipo:")
        if id_equipo and ip:
            equipos = cargar_equipos()
            equipos[id_equipo] = ip
            guardar_equipos(equipos)
            self.actualizar_lista()
            logger.info(f"Equipo añadido: ID={id_equipo}, IP={ip}")

    def eliminar_equipo(self):
        seleccion = self.tree.selection()
        if not seleccion:
            messagebox.showwarning("Advertencia", "Seleccione un equipo para eliminar")
            return
        id_equipo = self.tree.item(seleccion[0])['values'][0]
        if messagebox.askyesno("Confirmar", f"¿Eliminar el equipo {id_equipo}?"):
            equipos = cargar_equipos()
            if id_equipo in equipos:
                del equipos[id_equipo]
                guardar_equipos(equipos)
                self.actualizar_lista()
                logger.info(f"Equipo eliminado: ID={id_equipo}")

    def obtener_ultima_sincronizacion(self, id_equipo):
        try:
            with conectar_db() as db:
                if not db:
                    return "No disponible"
                with db.cursor() as cursor:
                    cursor.execute(
                        "SELECT ultima_sincronizacion FROM rh_sincronizaciones WHERE id_equipo = %s",
                        (id_equipo,)
                    )
                    result = cursor.fetchone()
                    return result['ultima_sincronizacion'].strftime('%Y-%m-%d %H:%M:%S') if result else "Nunca"
        except Exception:
            return "Error"

    def actualizar_lista(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        equipos = cargar_equipos()
        for id_equipo, ip in equipos.items():
            ultima_sinc = self.obtener_ultima_sincronizacion(id_equipo)
            self.tree.insert('', 'end', values=(id_equipo, ip, ultima_sinc))
        logger.info("Lista de equipos actualizada en la interfaz")

    def sincronizar(self):
        threading.Thread(target=extraer_datos).start()
        self.root.after(5000, self.actualizar_lista)
        messagebox.showinfo("Información", "Sincronización iniciada. Verifique los logs para más detalles.")

# --- JOBS PROGRAMADOS ---
def programar_jobs():
    horas = ["08:00","09:00","10:00","11:00","12:00","13:00", "14:00", "14:35", "15:00","16:00","17:00","18:00","19:00", "20:00","21:00","22:00","23:00","00:00"]
    for h in horas:
        schedule.every().day.at(h).do(extraer_datos)
    while True:
        schedule.run_pending()
        time_module.sleep(60)

if __name__ == '__main__':
    with conectar_db() as db:
        if db:
            with db.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS rh_sincronizaciones (
                    id_equipo VARCHAR(50) PRIMARY KEY,
                    ultima_sincronizacion DATETIME,
                    created_at DATETIME,
                    updated_at DATETIME
                )
                """)
    threading.Thread(target=programar_jobs, daemon=True).start()
    root = tk.Tk()
    app = App(root)
    root.mainloop()
