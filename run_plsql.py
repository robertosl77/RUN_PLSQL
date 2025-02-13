import time
import cx_Oracle    # pip install cx_Oracle
from datetime import datetime, timedelta

from circuito_138_gisqa02_gelec import CIRCUITO_138_GISQA02_GELEC
from circuito_103_gisqa02 import CIRCUITO_103_GISQA02
from circuito_103_gispr03_reactivact import CIRCUITO_103_GISPR03_REACTIVACT

def connect_to_database():
    """Conexión a la base de datos Oracle."""
    connection = cx_Oracle.connect(
        "SVC_GELEC_QA",  # Usuario de base de datos
        "Tr@stien.daQA02",  # Contraseña de base de datos
        "ltronexusbdqa01.pro.edenor:1530/GISQA02"  # Conexión a la base de datos
    )
    return connection

def get_next_execution_time(interval_minutes):
    """Calcula la próxima hora de ejecución en múltiplos de 5 minutos."""
    now = datetime.now()
    minutes_to_next_interval = interval_minutes - (now.minute % interval_minutes)
    next_run = now + timedelta(minutes=minutes_to_next_interval)
    return next_run.replace(second=0, microsecond=0)

def schedule_execution(interval_minutes=5):
    """Programa la ejecución del bloque PL/SQL en intervalos."""
    while True:
        next_run = get_next_execution_time(interval_minutes)
        print(f"Próxima ejecución programada para: {next_run.strftime('%H:%M:%S')}")
        time_to_sleep = (next_run - datetime.now()).total_seconds()
        time.sleep(max(0, time_to_sleep))
        # 
        schedule_execution_inmediate()
        
def schedule_execution_inmediate():
    """Ejecución del bloque PL/SQL."""
    # 
    # print("\nCIRCUITO 103 GISQA02...")
    # nexus_gis_instance = CIRCUITO_103_GISQA02()
    # nexus_gis_instance.schedule_execution_inmediate()
    # 
    print("\nCIRCUITO 138 GISQA02 GELEC...")
    gelec_instance = CIRCUITO_138_GISQA02_GELEC()
    gelec_instance.schedule_execution_inmediate()
    # 
    print("\nCIRCUITO 138 GISQA03 REACTIVA_CT...")
    gelec_instance = CIRCUITO_103_GISPR03_REACTIVACT()
    gelec_instance.ejecuta_sql_dinamico()
    # 
    print("\n")

if __name__ == "__main__":
    schedule_execution(interval_minutes=5)  # Cambia el valor si deseas otro intervalo
    # schedule_execution_inmediate()
