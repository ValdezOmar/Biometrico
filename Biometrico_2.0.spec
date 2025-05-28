# Biometrico_2.0.spec

block_cipher = None

a = Analysis(
    ['Biometrico_2.0.py'],
    pathex=['.', r'D:\Proyectos\Biometrico\venv\Lib\site-packages'],
    binaries=[],
    datas=[
        ('equipos.json', '.'),
        ('favicon.ico', '.'),
        ('zk', 'zk'),
    ],
    hiddenimports=['schedule', 'zk'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Biometrico_2.0',
    debug=False,  # debuguer a flase cuando ya funcione
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # Para ver errores al ejecutar
    icon='favicon.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Biometrico_2.0'
)
