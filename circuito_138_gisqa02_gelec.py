import time
import cx_Oracle
from datetime import datetime, timedelta

class CIRCUITO_138_GISQA02_GELEC:
    def __init__(self):
        self.user = "SVC_GELEC_QA"
        self.password = "Tr@stien.daQA02"
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
                P_USER_ID VARCHAR2(200);
                P_RESULTADO VARCHAR2(200);    
            BEGIN
                P_USER_ID := 'GELEC_BATCH';
                P_RESULTADO := NULL;

                GELEC.PKG_BATCH.buscar_doc_edp_afectados_nuevo (
                    P_USER_ID => P_USER_ID,
                    P_RESULTADO => P_RESULTADO
                );  
                DBMS_OUTPUT.PUT_LINE(P_RESULTADO);
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
    gelec_instance = CIRCUITO_138_GISQA02_GELEC()
    gelec_instance.schedule_execution(interval_minutes=5)  # Cambia el valor si deseas otro intervalo
    # gelec_instance.schedule_execution_inmediate()
