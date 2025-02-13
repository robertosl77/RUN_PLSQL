import time
import cx_Oracle
from datetime import datetime, timedelta

class CIRCUITO_103_GISQA02:
    def __init__(self):
        self.user = "NEXUS_GIS"
        self.password = "nexus_gis"
        self.dsn = "ltronexusbdqa01.pro.edenor:1530/GISQA02"

    def connect_to_database(self):
        """Conexión a la base de datos Oracle."""
        connection = cx_Oracle.connect(
            self.user,
            self.password,
            self.dsn
        )
        return connection

    def execute_plsql_procedure(self):
        """Ejecuta el bloque PL/SQL proporcionado."""
        plsql_code = """
            DECLARE
                v_ReturnValue  VARCHAR2(300);
            BEGIN
                v_ReturnValue := INFORMACION_ENREMTAT_DIN.INICIO();
                DBMS_OUTPUT.PUT_LINE('v_ReturnValue = ' || v_ReturnValue);
            END;
        """
        connection = self.connect_to_database()
        cursor = connection.cursor()
        try:
            cursor.execute(plsql_code)
            print("PL/SQL ejecutado correctamente.")
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(f"Error ejecutando PL/SQL: {error.message}")
        finally:
            cursor.close()
            connection.close()

    def get_next_execution_time(self, interval_minutes):
        """Calcula la próxima hora de ejecución en múltiplos de 5 minutos."""
        now = datetime.now()
        minutes_to_next_interval = interval_minutes - (now.minute % interval_minutes)
        next_run = now + timedelta(minutes=minutes_to_next_interval)
        return next_run.replace(second=0, microsecond=0)

    def schedule_execution(self, interval_minutes=5):
        """Programa la ejecución del bloque PL/SQL en intervalos."""
        while True:
            next_run = self.get_next_execution_time(interval_minutes)
            print(f"Próxima ejecución programada para: {next_run.strftime('%H:%M:%S')}")
            time_to_sleep = (next_run - datetime.now()).total_seconds()
            time.sleep(max(0, time_to_sleep))
            self.execute_plsql_procedure()

    def schedule_execution_inmediate(self):
        """Ejecución del bloque PL/SQL."""
        self.execute_plsql_procedure()

if __name__ == "__main__":
    nexus_gis_instance = CIRCUITO_103_GISQA02()
    nexus_gis_instance.schedule_execution(interval_minutes=5)  # Cambia el valor si deseas otro intervalo
    # nexus_gis_instance.schedule_execution_inmediate()
