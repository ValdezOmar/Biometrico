# -*- coding: utf-8 -*-
import pymysql
from zk import ZK
from datetime import datetime, time
import logging
from pathlib import Path
from contextlib import contextmanager
import json
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk, scrolledtext
import threading
import schedule
import time as time_module
import sys
import os
import subprocess
import platform

# Configurar la ruta de zk primero
sys.path.insert(0, './zk')

# --- ARCHIVOS DE CONFIGURACIÓN ---
CONFIG_FILE = 'equipos.json'
DB_CONFIG_FILE = 'db_config.json'
SYNC_CONFIG_FILE = 'sync_config.json'
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

# --- CONFIGURACIÓN DE SINCRONIZACIÓN ---
DEFAULT_SYNC_CONFIG = {
    'horas_sincronizacion': ["08:00", "09:00", "10:00", "11:00", "12:00", "13:00", 
                            "14:00", "14:35", "15:00", "16:00", "17:00", "18:00",
                            "19:00", "20:00", "21:00", "22:00", "23:00", "00:00"],
    'iniciar_con_sistema': False
}

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

# --- FUNCIONES DE CONFIGURACIÓN ---
def cargar_config_archivo(archivo, default_config):
    """Carga configuración desde archivo JSON con manejo de errores robusto"""
    try:
        if Path(archivo).exists():
            with open(archivo, 'r', encoding='utf-8') as f:
                contenido = f.read().strip()
                if contenido:  # Verificar que no esté vacío
                    data = json.loads(contenido)
                    # Combinar con defaults
                    config_actualizada = {**default_config, **data}
                    logger.info(f"Configuración cargada desde {archivo}")
                    return config_actualizada
                else:
                    logger.warning(f"Archivo {archivo} está vacío. Usando configuración por defecto.")
                    return default_config.copy()
        else:
            logger.info(f"Archivo {archivo} no encontrado. Creando con configuración por defecto.")
            with open(archivo, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            return default_config.copy()
    except json.JSONDecodeError as e:
        logger.error(f"Error de JSON en {archivo}: {e}. Usando configuración por defecto.")
        # Crear archivo nuevo si está corrupto
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        return default_config.copy()
    except Exception as e:
        logger.error(f"Error cargando {archivo}: {e}", exc_info=True)
        return default_config.copy()

def cargar_db_config():
    return cargar_config_archivo(DB_CONFIG_FILE, DEFAULT_DB_CONFIG)

def cargar_sync_config():
    return cargar_config_archivo(SYNC_CONFIG_FILE, DEFAULT_SYNC_CONFIG)

def guardar_config_archivo(archivo, config):
    """Guarda configuración en archivo JSON"""
    try:
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        logger.info(f"Configuración guardada en {archivo}")
        return True
    except Exception as e:
        logger.error(f"Error guardando configuración en {archivo}: {e}", exc_info=True)
        return False

def guardar_db_config(config):
    # Solo campos serializables
    serializable_config = {k: v for k, v in config.items() 
                         if k in ["host", "user", "password", "database", "port", "charset"]}
    return guardar_config_archivo(DB_CONFIG_FILE, serializable_config)

def guardar_sync_config(config):
    return guardar_config_archivo(SYNC_CONFIG_FILE, config)

# Cargar configuraciones globales
DB_CONFIG = cargar_db_config()
SYNC_CONFIG = cargar_sync_config()

# --- CONFIGURACIÓN DE EQUIPOS ---
def cargar_equipos():
    return cargar_config_archivo(CONFIG_FILE, {})

def guardar_equipos(equipos):
    return guardar_config_archivo(CONFIG_FILE, equipos)

# --- CONFIGURACIÓN DE INICIO CON SISTEMA ---
def configurar_inicio_sistema(habilitar=True):
    """Configura la aplicación para iniciar con el sistema operativo"""
    sistema = platform.system()
    
    try:
        if sistema == "Windows":
            from winreg import OpenKey, SetValueEx, CloseKey, HKEY_CURRENT_USER, KEY_SET_VALUE
            from winreg import HKEYType, REG_SZ
            
            startup_key = r"Software\Microsoft\Windows\CurrentVersion\Run"
            app_name = "SincronizadorBiometrico"
            app_path = os.path.abspath(sys.argv[0])
            
            key = OpenKey(HKEY_CURRENT_USER, startup_key, 0, KEY_SET_VALUE)
            
            if habilitar:
                SetValueEx(key, app_name, 0, REG_SZ, f'"{app_path}"')
                logger.info("Aplicación configurada para iniciar con Windows")
            else:
                try:
                    from winreg import DeleteValue
                    DeleteValue(key, app_name)
                    logger.info("Aplicación removida del inicio de Windows")
                except WindowsError:
                    pass
            
            CloseKey(key)
            
        elif sistema == "Linux":
            autostart_dir = os.path.expanduser("~/.config/autostart")
            os.makedirs(autostart_dir, exist_ok=True)
            
            desktop_file = os.path.join(autostart_dir, "sincronizador-biometrico.desktop")
            app_path = os.path.abspath(sys.argv[0])
            
            if habilitar:
                desktop_content = f"""[Desktop Entry]
Type=Application
Name=Sincronizador Biométrico
Exec=python3 "{app_path}"
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
"""
                with open(desktop_file, 'w') as f:
                    f.write(desktop_content)
                os.chmod(desktop_file, 0o755)
                logger.info("Aplicación configurada para iniciar con Linux")
            else:
                if os.path.exists(desktop_file):
                    os.remove(desktop_file)
                    logger.info("Aplicación removida del inicio de Linux")
        
        elif sistema == "Darwin":  # macOS
            launchd_dir = os.path.expanduser("~/Library/LaunchAgents")
            os.makedirs(launchd_dir, exist_ok=True)
            
            plist_file = os.path.join(launchd_dir, "com.sincronizador.biometrico.plist")
            app_path = os.path.abspath(sys.argv[0])
            
            if habilitar:
                plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.sincronizador.biometrico</string>
    <key>ProgramArguments</key>
    <array>
        <string>python3</string>
        <string>{app_path}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
</dict>
</plist>"""
                with open(plist_file, 'w') as f:
                    f.write(plist_content)
                logger.info("Aplicación configurada para iniciar con macOS")
            else:
                if os.path.exists(plist_file):
                    os.remove(plist_file)
                    logger.info("Aplicación removida del inicio de macOS")
        
        return True
    except Exception as e:
        logger.error(f"Error configurando inicio con sistema: {e}", exc_info=True)
        return False

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

class ResultadoSincronizacion:
    """Clase para almacenar resultados de sincronización"""
    def __init__(self):
        self.exitoso = False
        self.total_equipos = 0
        self.equipos_procesados = 0
        self.registros_insertados = 0
        self.registros_duplicados = 0
        self.errores = 0
        self.detalle_equipos = []
        self.detalle_registros = []
        self.mensaje = ""
        self.timestamp = datetime.now()

def extraer_datos():
    """Función principal de extracción de datos con resultados detallados"""
    resultado = ResultadoSincronizacion()
    
    try:
        logger.info("Iniciando proceso de extracción de datos")
        equipos = cargar_equipos()
        resultado.total_equipos = len(equipos)
        
        if not equipos:
            resultado.mensaje = "No hay equipos configurados para sincronizar"
            logger.warning(resultado.mensaje)
            return resultado

        with conectar_db() as db:
            if not db:
                resultado.mensaje = "No se pudo conectar a la base de datos"
                logger.error(resultado.mensaje)
                return resultado

            for id_equipo, ip in equipos.items():
                resultado.equipos_procesados += 1
                logger.info(f"Procesando equipo {id_equipo} ({ip})")
                
                registros_insertados = 0
                registros_duplicados = 0
                errores_equipo = 0
                registros_detalle = []

                with conectar_biometrico(ip) as bio:
                    if not bio:
                        resultado.errores += 1
                        resultado.detalle_equipos.append({
                            'equipo': id_equipo,
                            'ip': ip,
                            'estado': 'ERROR_CONEXION',
                            'registros_insertados': 0,
                            'registros_duplicados': 0,
                            'errores': 1
                        })
                        logger.error(f"No se pudo conectar al equipo {id_equipo}")
                        continue
                    
                    try:
                        registros = bio.get_attendance()
                        logger.info(f"Obtenidos {len(registros)} registros del equipo {id_equipo}")
                    except Exception as e:
                        resultado.errores += 1
                        resultado.detalle_equipos.append({
                            'equipo': id_equipo,
                            'ip': ip,
                            'estado': 'ERROR_LECTURA',
                            'registros_insertados': 0,
                            'registros_duplicados': 0,
                            'errores': 1
                        })
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
                                    resultado.registros_duplicados += 1
                                    continue

                                cursor.execute(
                                    """
                                    INSERT INTO rh_asistencias (id_equipo, user_id, fecha, hora, visible, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
                                    """,
                                    (id_equipo, user_id, fecha, hora)
                                )
                                
                                registros_insertados += 1
                                resultado.registros_insertados += 1
                                
                                registros_detalle.append({
                                    'user_id': user_id,
                                    'fecha': str(fecha),
                                    'hora_original': str(hora_original),
                                    'hora_ajustada': str(hora),
                                    'equipo': id_equipo
                                })
                                
                            except Exception as e:
                                errores_equipo += 1
                                resultado.errores += 1
                                logger.error(
                                    f"Error procesando registro del equipo {id_equipo} - Usuario: {user_id}, Fecha: {fecha}, Hora: {hora}: {str(e)}", 
                                    exc_info=True
                                )

                        actualizar_ultima_sincronizacion(db, id_equipo)

                    # Agregar detalle del equipo procesado
                    resultado.detalle_equipos.append({
                        'equipo': id_equipo,
                        'ip': ip,
                        'estado': 'COMPLETADO',
                        'registros_insertados': registros_insertados,
                        'registros_duplicados': registros_duplicados,
                        'errores': errores_equipo
                    })
                    
                    # Agregar registros insertados
                    resultado.detalle_registros.extend(registros_detalle)

                    logger.info(
                        f"Resumen equipo {id_equipo}:\n"
                        f"Registros insertados: {registros_insertados}\n"
                        f"Registros duplicados: {registros_duplicados}\n"
                        f"Errores: {errores_equipo}"
                    )

        resultado.exitoso = True
        resultado.mensaje = "Sincronización completada exitosamente"
        logger.info(f"Proceso completado: {resultado.registros_insertados} registros insertados, {resultado.registros_duplicados} duplicados, {resultado.errores} errores")
        
    except Exception as e:
        resultado.exitoso = False
        resultado.mensaje = f"Error en sincronización: {str(e)}"
        logger.error(f"Error en proceso de extracción: {str(e)}", exc_info=True)
    
    return resultado

# --- INTERFAZ GRÁFICA MEJORADA ---
class ModernButton(tk.Button):
    """Botón con estilo moderno"""
    def __init__(self, master=None, **kwargs):
        kwargs.setdefault('bg', '#4CAF50')
        kwargs.setdefault('fg', 'white')
        kwargs.setdefault('font', ('Arial', 10, 'bold'))
        kwargs.setdefault('relief', 'flat')
        kwargs.setdefault('bd', 0)
        kwargs.setdefault('padx', 15)
        kwargs.setdefault('pady', 8)
        kwargs.setdefault('cursor', 'hand2')
        super().__init__(master, **kwargs)
        
        # Efectos hover
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        
    def _on_enter(self, e):
        self['bg'] = '#45a049'
        
    def _on_leave(self, e):
        self['bg'] = '#4CAF50'

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Sincronizador Biométrico")
        self.root.geometry("1000x700")
        
        # Establecer icono si existe
        try:
            if platform.system() == "Windows":
                self.root.iconbitmap("icon.ico")
        except:
            pass
        
        # Configurar colores
        self.colors = {
            'primary': '#2C3E50',
            'secondary': '#34495E',
            'accent': '#3498DB',
            'success': '#27AE60',
            'warning': '#F39C12',
            'danger': '#E74C3C',
            'light': '#ECF0F1',
            'dark': '#2C3E50'
        }
        
        # Configurar estilos
        self.setup_styles()
        
        # Frame principal
        self.main_frame = tk.Frame(root, bg=self.colors['light'])
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Header
        self.create_header()
        
        # Notebook (pestañas)
        self.create_notebook()
        
        # Panel de resultados
        self.create_results_panel()
        
        # Estado
        self.create_status_bar()
        
        # Cargar configuraciones
        self.actualizar_lista()
        self.cargar_sync_config()
        
        # Resultado de sincronización actual
        self.current_result = None

    def setup_styles(self):
        """Configurar estilos para widgets"""
        style = ttk.Style()
        
        # Configurar tema
        style.theme_use('clam')
        
        # Configurar colores de Treeview
        style.configure("Treeview",
            background=self.colors['light'],
            foreground=self.colors['dark'],
            rowheight=25,
            fieldbackground=self.colors['light'],
            font=('Arial', 10))
        
        style.configure("Treeview.Heading",
            background=self.colors['primary'],
            foreground='white',
            relief='flat',
            font=('Arial', 11, 'bold'))
        
        style.map("Treeview.Heading",
            background=[('active', self.colors['secondary'])])
        
        # Configurar Notebook
        style.configure("TNotebook",
            background=self.colors['light'],
            tabmargins=[2, 5, 2, 0])
        
        style.configure("TNotebook.Tab",
            background=self.colors['light'],
            foreground=self.colors['dark'],
            padding=[15, 5],
            font=('Arial', 10))
        
        style.map("TNotebook.Tab",
            background=[('selected', self.colors['accent'])],
            foreground=[('selected', 'white')])

    def create_header(self):
        """Crear encabezado de la aplicación"""
        header_frame = tk.Frame(self.main_frame, bg=self.colors['primary'], height=80)
        header_frame.pack(fill="x", pady=(0, 10))
        
        # Título
        title_label = tk.Label(header_frame,
            text="⚙️ Sincronizador Biométrico",
            font=('Arial', 20, 'bold'),
            fg='white',
            bg=self.colors['primary'])
        title_label.pack(side="left", padx=20, pady=20)
        
        # Botón de sincronización rápida
        sync_btn = ModernButton(header_frame,
            text="🔄 Sincronizar Ahora",
            command=self.sincronizar)
        sync_btn.pack(side="right", padx=20, pady=20)

    def create_notebook(self):
        """Crear pestañas principales"""
        notebook = ttk.Notebook(self.main_frame)
        notebook.pack(fill="both", expand=True, pady=(0, 10))
        
        # Pestaña 1: Equipos
        self.create_equipos_tab(notebook)
        
        # Pestaña 2: Configuración BD
        self.create_config_tab(notebook)
        
        # Pestaña 3: Sincronización
        self.create_sync_tab(notebook)

    def create_equipos_tab(self, notebook):
        """Crear pestaña de gestión de equipos"""
        frame = tk.Frame(notebook, bg=self.colors['light'])
        notebook.add(frame, text="📱 Equipos")
        
        # Frame para controles
        controls_frame = tk.Frame(frame, bg=self.colors['light'])
        controls_frame.pack(fill="x", padx=10, pady=10)
        
        # Botones
        btn_frame = tk.Frame(controls_frame, bg=self.colors['light'])
        btn_frame.pack(side="right")
        
        ModernButton(btn_frame, text="➕ Añadir", command=self.anadir_equipo).pack(side="left", padx=5)
        ModernButton(btn_frame, text="🗑️ Eliminar", command=self.eliminar_equipo, bg=self.colors['danger']).pack(side="left", padx=5)
        ModernButton(btn_frame, text="🔄 Actualizar", command=self.actualizar_lista).pack(side="left", padx=5)
        
        # Treeview para equipos
        tree_frame = tk.Frame(frame, bg=self.colors['light'])
        tree_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        self.tree = ttk.Treeview(tree_frame, columns=('id', 'ip', 'ultima_sinc', 'estado'), show='headings')
        
        # Configurar columnas
        columns = [
            ('id', 'ID Equipo', 150),
            ('ip', 'Dirección IP', 150),
            ('ultima_sinc', 'Última Sincronización', 200),
            ('estado', 'Estado', 100)
        ]
        
        for col_id, heading, width in columns:
            self.tree.heading(col_id, text=heading)
            self.tree.column(col_id, width=width)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def create_config_tab(self, notebook):
        """Crear pestaña de configuración de BD"""
        frame = tk.Frame(notebook, bg=self.colors['light'])
        notebook.add(frame, text="⚙️ Configuración BD")
        
        # Contenedor principal con padding
        container = tk.Frame(frame, bg=self.colors['light'], padx=20, pady=20)
        container.pack(fill="both", expand=True)
        
        # Campos de configuración
        self.entries = {}
        campos = [
            ("host", "Servidor", False),
            ("user", "Usuario", False),
            ("password", "Contraseña", True),
            ("database", "Base de Datos", False)
        ]
        
        for i, (campo, label, is_password) in enumerate(campos):
            # Frame para cada campo
            field_frame = tk.Frame(container, bg=self.colors['light'], pady=5)
            field_frame.pack(fill="x")
            
            # Etiqueta
            tk.Label(field_frame,
                text=label + ":",
                font=('Arial', 10, 'bold'),
                bg=self.colors['light'],
                fg=self.colors['dark'],
                width=15,
                anchor="w").pack(side="left")
            
            # Campo de entrada
            entry = tk.Entry(field_frame,
                font=('Arial', 10),
                show="*" if is_password else "",
                relief="solid",
                bd=1,
                width=30)
            entry.insert(0, DB_CONFIG.get(campo, ""))
            entry.pack(side="left", padx=(10, 0))
            
            self.entries[campo] = entry
        
        # Botón de guardar
        save_btn = ModernButton(container,
            text="💾 Guardar Configuración",
            command=self.guardar_config_bd,
            bg=self.colors['accent'])
        save_btn.pack(pady=20)
        
        # Botón de test
        test_btn = ModernButton(container,
            text="🔍 Probar Conexión",
            command=self.probar_conexion_db)
        test_btn.pack()

    def create_sync_tab(self, notebook):
        """Crear pestaña de configuración de sincronización"""
        frame = tk.Frame(notebook, bg=self.colors['light'])
        notebook.add(frame, text="⏰ Sincronización")
        
        container = tk.Frame(frame, bg=self.colors['light'], padx=20, pady=20)
        container.pack(fill="both", expand=True)
        
        # Horas de sincronización
        tk.Label(container,
            text="Horas de Sincronización (HH:MM):",
            font=('Arial', 11, 'bold'),
            bg=self.colors['light'],
            fg=self.colors['dark']).pack(anchor="w", pady=(0, 10))
        
        # Text area para horas
        self.horas_text = scrolledtext.ScrolledText(container,
            height=8,
            font=('Consolas', 10),
            relief="solid",
            bd=1)
        self.horas_text.pack(fill="x", pady=(0, 10))
        
        # Botón para guardar horas
        save_horas_btn = ModernButton(container,
            text="💾 Guardar Horas",
            command=self.guardar_horas_sincronizacion,
            bg=self.colors['accent'])
        save_horas_btn.pack(pady=10)
        
        # Separador
        tk.Frame(container, height=2, bg=self.colors['dark']).pack(fill="x", pady=20)
        
        # Inicio con sistema
        self.inicio_sistema_var = tk.BooleanVar()
        
        inicio_frame = tk.Frame(container, bg=self.colors['light'])
        inicio_frame.pack(fill="x")
        
        tk.Checkbutton(inicio_frame,
            text="Iniciar con sistema operativo",
            variable=self.inicio_sistema_var,
            font=('Arial', 10),
            bg=self.colors['light'],
            fg=self.colors['dark'],
            command=self.toggle_inicio_sistema).pack(anchor="w")
        
        # Info label
        self.info_label = tk.Label(container,
            text="",
            font=('Arial', 9),
            bg=self.colors['light'],
            fg=self.colors['dark'])
        self.info_label.pack(pady=10)

    def create_results_panel(self):
        """Crear panel de resultados de sincronización"""
        results_frame = tk.LabelFrame(self.main_frame,
            text="📊 Resultados de Sincronización",
            font=('Arial', 11, 'bold'),
            bg=self.colors['light'],
            fg=self.colors['primary'],
            relief="solid",
            bd=1)
        results_frame.pack(fill="x", pady=(0, 10))
        
        # Contenedor de resultados
        self.results_container = tk.Frame(results_frame, bg=self.colors['light'], padx=10, pady=10)
        self.results_container.pack(fill="both", expand=True)
        
        # Inicializar con mensaje vacío
        self.result_label = tk.Label(self.results_container,
            text="No hay sincronizaciones recientes",
            font=('Arial', 10),
            bg=self.colors['light'],
            fg=self.colors['dark'])
        self.result_label.pack()

    def create_status_bar(self):
        """Crear barra de estado"""
        self.status_bar = tk.Frame(self.main_frame, bg=self.colors['dark'], height=25)
        self.status_bar.pack(fill="x", side="bottom")
        
        self.status_label = tk.Label(self.status_bar,
            text="Listo",
            fg='white',
            bg=self.colors['dark'],
            font=('Arial', 9))
        self.status_label.pack(side="left", padx=10)
        
        # Hora de última sincronización
        self.last_sync_label = tk.Label(self.status_bar,
            text="Última sincronización: Nunca",
            fg='white',
            bg=self.colors['dark'],
            font=('Arial', 9))
        self.last_sync_label.pack(side="right", padx=10)

    def update_status(self, message):
        """Actualizar barra de estado"""
        self.status_label.config(text=message)
        self.root.update()

    def guardar_config_bd(self):
        """Guardar configuración de BD"""
        try:
            for campo, entry in self.entries.items():
                DB_CONFIG[campo] = entry.get()
            
            if guardar_db_config(DB_CONFIG):
                messagebox.showinfo("Éxito", "✅ Configuración de base de datos guardada correctamente")
                self.update_status("Configuración BD guardada")
            else:
                messagebox.showerror("Error", "❌ No se pudo guardar la configuración")
                
        except Exception as e:
            messagebox.showerror("Error", f"❌ Error: {str(e)}")

    def probar_conexion_db(self):
        """Probar conexión a la base de datos"""
        self.update_status("Probando conexión a BD...")
        
        def test():
            try:
                with conectar_db() as db:
                    if db:
                        with db.cursor() as cursor:
                            cursor.execute("SELECT 1 as test")
                            result = cursor.fetchone()
                            if result:
                                self.root.after(0, lambda: messagebox.showinfo("Conexión Exitosa", 
                                    "✅ Conexión a la base de datos establecida correctamente"))
                            else:
                                self.root.after(0, lambda: messagebox.showerror("Error", 
                                    "❌ No se pudo verificar la conexión"))
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", 
                            "❌ No se pudo conectar a la base de datos"))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", 
                    f"❌ Error de conexión: {str(e)}"))
            finally:
                self.root.after(0, lambda: self.update_status("Listo"))
        
        threading.Thread(target=test, daemon=True).start()

    def cargar_sync_config(self):
        """Cargar configuración de sincronización en la interfaz"""
        try:
            # Horas
            horas_text = "\n".join(SYNC_CONFIG.get('horas_sincronizacion', []))
            self.horas_text.delete(1.0, tk.END)
            self.horas_text.insert(1.0, horas_text)
            
            # Inicio con sistema
            self.inicio_sistema_var.set(SYNC_CONFIG.get('iniciar_con_sistema', False))
            self.update_info_label()
            
        except Exception as e:
            logger.error(f"Error cargando configuración de sincronización: {e}")

    def guardar_horas_sincronizacion(self):
        """Guardar horas de sincronización"""
        try:
            horas_text = self.horas_text.get(1.0, tk.END).strip()
            horas = [h.strip() for h in horas_text.split('\n') if h.strip()]
            
            # Validar formato HH:MM
            for hora in horas:
                try:
                    datetime.strptime(hora, "%H:%M")
                except ValueError:
                    messagebox.showerror("Error", f"❌ Formato de hora inválido: {hora}\nUse formato HH:MM")
                    return
            
            SYNC_CONFIG['horas_sincronizacion'] = horas
            
            if guardar_sync_config(SYNC_CONFIG):
                messagebox.showinfo("Éxito", "✅ Horas de sincronización guardadas")
                self.update_status("Horas de sincronización actualizadas")
                # Reiniciar scheduler
                self.reiniciar_scheduler()
            else:
                messagebox.showerror("Error", "❌ No se pudo guardar la configuración")
                
        except Exception as e:
            messagebox.showerror("Error", f"❌ Error: {str(e)}")

    def toggle_inicio_sistema(self):
        """Alternar inicio con sistema"""
        try:
            habilitar = self.inicio_sistema_var.get()
            
            if configurar_inicio_sistema(habilitar):
                SYNC_CONFIG['iniciar_con_sistema'] = habilitar
                guardar_sync_config(SYNC_CONFIG)
                
                estado = "habilitado" if habilitar else "deshabilitado"
                self.update_status(f"Inicio con sistema {estado}")
                self.update_info_label()
            else:
                self.inicio_sistema_var.set(not habilitar)
                messagebox.showerror("Error", "❌ No se pudo configurar el inicio con sistema")
                
        except Exception as e:
            logger.error(f"Error alternando inicio con sistema: {e}")
            messagebox.showerror("Error", f"❌ Error: {str(e)}")

    def update_info_label(self):
        """Actualizar etiqueta de información"""
        if self.inicio_sistema_var.get():
            self.info_label.config(text="✅ La aplicación se iniciará automáticamente con el sistema")
        else:
            self.info_label.config(text="❌ La aplicación no se iniciará automáticamente")

    def anadir_equipo(self):
        """Añadir nuevo equipo"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Añadir Equipo")
        dialog.geometry("400x250")
        dialog.configure(bg=self.colors['light'])
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Centrar ventana
        dialog.geometry(f"+{self.root.winfo_rootx()+200}+{self.root.winfo_rooty()+200}")
        
        # Contenido
        tk.Label(dialog,
            text="Añadir Nuevo Equipo",
            font=('Arial', 14, 'bold'),
            bg=self.colors['light'],
            fg=self.colors['primary']).pack(pady=20)
        
        # Campo ID
        tk.Label(dialog,
            text="ID del Equipo:",
            font=('Arial', 10),
            bg=self.colors['light'],
            fg=self.colors['dark']).pack(pady=(0, 5))
        
        id_entry = tk.Entry(dialog, font=('Arial', 10), width=30)
        id_entry.pack(pady=(0, 15))
        id_entry.focus()
        
        # Campo IP
        tk.Label(dialog,
            text="Dirección IP:",
            font=('Arial', 10),
            bg=self.colors['light'],
            fg=self.colors['dark']).pack(pady=(0, 5))
        
        ip_entry = tk.Entry(dialog, font=('Arial', 10), width=30)
        ip_entry.pack(pady=(0, 20))
        
        def guardar():
            id_equipo = id_entry.get().strip()
            ip = ip_entry.get().strip()
            
            if not id_equipo or not ip:
                messagebox.showerror("Error", "❌ Todos los campos son obligatorios")
                return
            
            equipos = cargar_equipos()
            if id_equipo in equipos:
                messagebox.showerror("Error", "❌ El ID del equipo ya existe")
                return
            
            equipos[id_equipo] = ip
            guardar_equipos(equipos)
            self.actualizar_lista()
            self.update_status(f"Equipo {id_equipo} añadido")
            dialog.destroy()
        
        # Botones
        btn_frame = tk.Frame(dialog, bg=self.colors['light'])
        btn_frame.pack(pady=10)
        
        ModernButton(btn_frame,
            text="💾 Guardar",
            command=guardar,
            bg=self.colors['success']).pack(side="left", padx=5)
        
        ModernButton(btn_frame,
            text="❌ Cancelar",
            command=dialog.destroy,
            bg=self.colors['danger']).pack(side="left", padx=5)

    def eliminar_equipo(self):
        """Eliminar equipo seleccionado"""
        seleccion = self.tree.selection()
        if not seleccion:
            messagebox.showwarning("Advertencia", "⚠️ Seleccione un equipo para eliminar")
            return
        
        id_equipo = self.tree.item(seleccion[0])['values'][0]
        
        if messagebox.askyesno("Confirmar", f"¿Eliminar el equipo {id_equipo}?", icon='warning'):
            equipos = cargar_equipos()
            if id_equipo in equipos:
                del equipos[id_equipo]
                guardar_equipos(equipos)
                self.actualizar_lista()
                self.update_status(f"Equipo {id_equipo} eliminado")
                logger.info(f"Equipo eliminado: {id_equipo}")

    def obtener_ultima_sincronizacion(self, id_equipo):
        """Obtener última sincronización de un equipo"""
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
        except Exception as e:
            logger.error(f"Error obteniendo última sincronización: {e}")
            return "Error"

    def actualizar_lista(self):
        """Actualizar lista de equipos en el Treeview"""
        for i in self.tree.get_children():
            self.tree.delete(i)
        
        equipos = cargar_equipos()
        
        for id_equipo, ip in equipos.items():
            ultima_sinc = self.obtener_ultima_sincronizacion(id_equipo)
            estado = "✅ Conectado" if ultima_sinc != "Nunca" else "❌ Sin sincronizar"
            self.tree.insert('', 'end', values=(id_equipo, ip, ultima_sinc, estado))
        
        self.update_status(f"{len(equipos)} equipos cargados")

    def mostrar_resultados_sincronizacion(self, resultado):
        """Mostrar resultados de sincronización en el panel"""
        self.current_result = resultado
        
        # Limpiar panel anterior
        for widget in self.results_container.winfo_children():
            widget.destroy()
        
        # Crear nuevo contenido
        if resultado.exitoso:
            # Frame para resumen
            summary_frame = tk.Frame(self.results_container, bg=self.colors['light'])
            summary_frame.pack(fill="x", pady=(0, 10))
            
            # Icono y título
            tk.Label(summary_frame,
                text="✅ SINCRONIZACIÓN EXITOSA",
                font=('Arial', 12, 'bold'),
                bg=self.colors['light'],
                fg=self.colors['success']).pack(anchor="w")
            
            # Resumen estadísticas
            stats_text = f"""
            📊 RESUMEN DE SINCRONIZACIÓN
            ──────────────────────────────
            • Equipos procesados: {resultado.equipos_procesados} de {resultado.total_equipos}
            • Registros insertados: {resultado.registros_insertados}
            • Registros duplicados: {resultado.registros_duplicados}
            • Errores: {resultado.errores}
            • Hora: {resultado.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            tk.Label(summary_frame,
                text=stats_text,
                font=('Consolas', 9),
                bg=self.colors['light'],
                fg=self.colors['dark'],
                justify="left").pack(anchor="w", pady=5)
            
            # Detalle por equipo (expandible)
            self.create_detalle_equipos(resultado.detalle_equipos)
            
            # Detalle de registros (expandible)
            if resultado.detalle_registros:
                self.create_detalle_registros(resultado.detalle_registros)
        
        else:
            # Error
            tk.Label(self.results_container,
                text="❌ ERROR EN SINCRONIZACIÓN",
                font=('Arial', 12, 'bold'),
                bg=self.colors['light'],
                fg=self.colors['danger']).pack(anchor="w")
            
            tk.Label(self.results_container,
                text=resultado.mensaje,
                font=('Arial', 10),
                bg=self.colors['light'],
                fg=self.colors['dark'],
                wraplength=800,
                justify="left").pack(anchor="w", pady=5)
        
        # Actualizar barra de estado
        self.last_sync_label.config(
            text=f"Última sincronización: {resultado.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

    def create_detalle_equipos(self, detalle_equipos):
        """Crear sección de detalle por equipo"""
        frame = tk.LabelFrame(self.results_container,
            text="📱 Detalle por Equipo",
            font=('Arial', 10, 'bold'),
            bg=self.colors['light'],
            fg=self.colors['primary'])
        frame.pack(fill="x", pady=5)
        
        for detalle in detalle_equipos:
            equipo_frame = tk.Frame(frame, bg=self.colors['light'])
            equipo_frame.pack(fill="x", padx=5, pady=2)
            
            # Icono según estado
            if detalle['estado'] == 'COMPLETADO':
                icon = "✅"
                color = self.colors['success']
            elif detalle['estado'] == 'ERROR_CONEXION':
                icon = "🔌"
                color = self.colors['danger']
            else:
                icon = "⚠️"
                color = self.colors['warning']
            
            tk.Label(equipo_frame,
                text=f"{icon} {detalle['equipo']} ({detalle['ip']}):",
                font=('Arial', 9, 'bold'),
                bg=self.colors['light'],
                fg=color,
                width=30,
                anchor="w").pack(side="left")
            
            tk.Label(equipo_frame,
                text=f"Insertados: {detalle['registros_insertados']} | Duplicados: {detalle['registros_duplicados']} | Errores: {detalle['errores']}",
                font=('Arial', 9),
                bg=self.colors['light'],
                fg=self.colors['dark']).pack(side="left")

    def create_detalle_registros(self, detalle_registros):
        """Crear sección de detalle de registros (expandible)"""
        # Frame colapsable
        self.registros_frame = tk.LabelFrame(self.results_container,
            text="📝 Registros Insertados (Click para expandir/contraer)",
            font=('Arial', 10, 'bold'),
            bg=self.colors['light'],
            fg=self.colors['primary'])
        self.registros_frame.pack(fill="x", pady=5)
        
        # Treeview para registros
        self.registros_tree = ttk.Treeview(self.registros_frame,
            columns=('equipo', 'user_id', 'fecha', 'hora_original', 'hora_ajustada'),
            show='headings',
            height=min(5, len(detalle_registros)))
        
        columns = [
            ('equipo', 'Equipo', 80),
            ('user_id', 'Usuario', 80),
            ('fecha', 'Fecha', 100),
            ('hora_original', 'Hora Original', 100),
            ('hora_ajustada', 'Hora Ajustada', 100)
        ]
        
        for col_id, heading, width in columns:
            self.registros_tree.heading(col_id, text=heading)
            self.registros_tree.column(col_id, width=width)
        
        # Insertar datos
        for reg in detalle_registros:
            self.registros_tree.insert('', 'end', values=(
                reg['equipo'],
                reg['user_id'],
                reg['fecha'],
                reg['hora_original'],
                reg['hora_ajustada']
            ))
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(self.registros_frame, orient="vertical", command=self.registros_tree.yview)
        self.registros_tree.configure(yscrollcommand=scrollbar.set)
        
        self.registros_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Evento para expandir/contraer
        self.registros_frame.bind('<Button-1>', self.toggle_registros_tree)

    def toggle_registros_tree(self, event):
        """Alternar visibilidad del treeview de registros"""
        if self.registros_tree.winfo_ismapped():
            self.registros_tree.pack_forget()
        else:
            self.registros_tree.pack(side="left", fill="both", expand=True)

    def sincronizar(self):
        """Iniciar sincronización"""
        self.update_status("Iniciando sincronización...")
        
        def ejecutar_sincronizacion():
            resultado = extraer_datos()
            self.root.after(0, lambda: self.mostrar_resultados_sincronizacion(resultado))
            self.root.after(0, self.actualizar_lista)
            self.root.after(0, lambda: self.update_status("Sincronización completada"))
        
        threading.Thread(target=ejecutar_sincronizacion, daemon=True).start()

    def reiniciar_scheduler(self):
        """Reiniciar el scheduler con nuevas horas"""
        schedule.clear()
        
        for hora in SYNC_CONFIG.get('horas_sincronizacion', []):
            try:
                schedule.every().day.at(hora).do(extraer_datos)
                logger.info(f"Programada sincronización a las {hora}")
            except Exception as e:
                logger.error(f"Error programando hora {hora}: {e}")

# --- JOBS PROGRAMADOS ---
def programar_jobs():
    """Programar trabajos de sincronización"""
    for hora in SYNC_CONFIG.get('horas_sincronizacion', []):
        try:
            schedule.every().day.at(hora).do(extraer_datos)
            logger.info(f"Programada sincronización a las {hora}")
        except Exception as e:
            logger.error(f"Error programando hora {hora}: {e}")
    
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"Error ejecutando job programado: {e}")
        time_module.sleep(60)

# --- INICIALIZACIÓN DE BD ---
def inicializar_bd():
    """Inicializar tablas de base de datos si no existen"""
    try:
        with conectar_db() as db:
            if db:
                with db.cursor() as cursor:
                    # Tabla de sincronizaciones
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rh_sincronizaciones (
                        id_equipo VARCHAR(50) PRIMARY KEY,
                        ultima_sincronizacion DATETIME,
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                    """)
                    
                    # Tabla de asistencias
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rh_asistencias (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        id_equipo VARCHAR(50),
                        user_id VARCHAR(50),
                        fecha DATE,
                        hora TIME,
                        visible BOOLEAN DEFAULT 1,
                        created_at DATETIME,
                        updated_at DATETIME,
                        INDEX idx_equipo_usuario_fecha_hora (id_equipo, user_id, fecha, hora)
                    )
                    """)
                    
                    db.commit()
                    logger.info("Tablas de base de datos verificadas/creadas")
    except Exception as e:
        logger.error(f"Error inicializando base de datos: {e}", exc_info=True)

# --- MAIN ---
if __name__ == '__main__':
    # Configurar inicio con sistema si está habilitado
    if SYNC_CONFIG.get('iniciar_con_sistema', False):
        configurar_inicio_sistema(True)
    
    # Inicializar base de datos
    inicializar_bd()
    
    # Iniciar scheduler en segundo plano
    scheduler_thread = threading.Thread(target=programar_jobs, daemon=True)
    scheduler_thread.start()
    
    # Iniciar interfaz gráfica
    root = tk.Tk()
    app = App(root)
    
    # Manejar cierre de ventana
    def on_closing():
        if messagebox.askokcancel("Salir", "¿Desea salir del sincronizador?\nLas sincronizaciones programadas se detendrán."):
            logger.info("Aplicación cerrada por el usuario")
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        logger.info("Aplicación interrumpida por el usuario")
        root.destroy()