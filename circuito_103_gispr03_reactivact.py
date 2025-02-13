import cx_Oracle
import logging
from datetime import datetime

class CIRCUITO_103_GISPR03_REACTIVACT:
    def __init__(self):
        """Inicializa la conexión a la base de datos."""
        self.user = "NEXUS_GIS"
        self.password = "Alg0don"
        self.dsn = "LTRONXGISBDPR03.PRO.EDENOR:1528/GISPR03"

    def connect_to_database(self):
        """Establece una conexión a la base de datos."""
        return cx_Oracle.connect(user=self.user, password=self.password, dsn=self.dsn)

    def obtiene_sql_dinamico(self):
        """Ejecuta el bloque PL/SQL proporcionado y obtiene el resultado."""
        plsql_code = """
            DECLARE
                P_PROCEDIMIENTO VARCHAR2(200);
                P_CONFIRMA BOOLEAN;
                v_ReturnValue VARCHAR2(2000);
            BEGIN
                P_PROCEDIMIENTO := 'AGREGA_AFECTACIONES';
                P_CONFIRMA := FALSE;

                v_ReturnValue := INFORMACION_ENREMTAT_DIN.GET_SQL(P_PROCEDIMIENTO => P_PROCEDIMIENTO, P_CONFIRMA => P_CONFIRMA);
                DBMS_OUTPUT.PUT_LINE('v_ReturnValue = ' || v_ReturnValue);
            END;
        """
        connection = self.connect_to_database()
        cursor = connection.cursor()
        try:
            # Habilita DBMS_OUTPUT
            cursor.callproc("DBMS_OUTPUT.ENABLE")
            
            # Ejecuta el bloque PL/SQL
            cursor.execute(plsql_code)

            # Obtén los mensajes de DBMS_OUTPUT
            output = []
            status_var = cursor.var(cx_Oracle.NUMBER)
            line_var = cursor.var(cx_Oracle.STRING, 1000)
            while True:
                cursor.callproc("DBMS_OUTPUT.GET_LINE", (line_var, status_var))
                if status_var.getvalue() != 0:
                    break
                output.append(line_var.getvalue())
                
            # Extrae el resultado relevante
            sql = next((line.replace("v_ReturnValue = ", "").strip() 
                        for line in output if "v_ReturnValue" in line), "")

        except cx_Oracle.DatabaseError as e:
            sql = ""
            error, = e.args
            logging.error(f"Error ejecutando PL/SQL: {error.message}")
        finally:
            cursor.close()
            connection.close()
        return sql

    def obtiene_ct_desde_objectid(self, object_id):
        """Ejecuta el bloque PL/SQL proporcionado para obtener el CT desde el OBJECTID."""
        plsql_code = f"""
            DECLARE
                P_OBJECTID NUMBER;
                v_ReturnValue VARCHAR2(10);
            BEGIN
                P_OBJECTID := {object_id};

                v_ReturnValue := INFORMACION_ENREMTAT_DIN.CT_FROM_OBJECTID(P_OBJECTID => P_OBJECTID);
                DBMS_OUTPUT.PUT_LINE('v_ReturnValue = ' || v_ReturnValue);
            END;
        """
        connection = self.connect_to_database()
        cursor = connection.cursor()
        try:
            # Habilita DBMS_OUTPUT
            cursor.callproc("DBMS_OUTPUT.ENABLE")
            
            # Ejecuta el bloque PL/SQL
            cursor.execute(plsql_code)

            # Obtén los mensajes de DBMS_OUTPUT
            output = []
            status_var = cursor.var(cx_Oracle.NUMBER)
            line_var = cursor.var(cx_Oracle.STRING, 1000)
            while True:
                cursor.callproc("DBMS_OUTPUT.GET_LINE", (line_var, status_var))
                if status_var.getvalue() != 0:
                    break
                output.append(line_var.getvalue())
                
            # Extrae el resultado relevante
            ct_value = "0"  # Valor por defecto en caso de error o no encontrado
            for line in output:
                if "v_ReturnValue" in line:
                    # Limpia y extrae el valor después de '='
                    ct_value = line.split("=")[1].strip()  # Divide en "=" y toma la parte derecha
                    break


        except cx_Oracle.DatabaseError as e:
            ct_value = "0"  # Retorna 0 en caso de error
            error, = e.args
            logging.error(f"Error ejecutando PL/SQL: {error.message}")
        finally:
            cursor.close()
            connection.close()
        return ct_value

    def ejecuta_sql_dinamico(self):
        """Obtiene el SQL dinámico, lo ejecuta, recorre los registros y reporta campos específicos."""
        sql = self.obtiene_sql_dinamico()
        if not sql:
            print("No se obtuvo un SQL válido.")
            return
        
        connection = self.connect_to_database()
        cursor = connection.cursor()
        try:
            # Ejecuta el SQL
            cursor.execute(sql)

            # Obtiene todos los registros y la cantidad
            registros = cursor.fetchall()
            cantidad_registros = len(registros)
            # print(f"Cantidad de registros obtenidos: {cantidad_registros}")
            if cantidad_registros==0:
                print("Sin Registros")
                return
            
            # Defino la fecha de ahora como grupo de registros ingresados
            ahora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    
            # Procesa cada fila de resultados
            for row in registros:
                # Extrae los valores de las columnas relevantes
                id = row[0]  # ID
                nro_documento = row[1]  # NRO_DOCUMENTO
                origen = row[3]  # ORIGEN
                fecha_documento = row[4]  # FECHA_DOCUMENTO
                estado = row[5]  # ESTADO
                objectid = row[2]  # OBJECTID
                afectados = row[6]  # CANT_AFECTACIONES
                causa = 0 if row[11] is None else row[11]  # CAUSA
                repetido = row[13]  # REPETIDO: determina si agrega (0) o reactiva (>0)

                # Obtiene el CT desde el OBJECTID utilizando el método
                ct = self.obtiene_ct_desde_objectid(objectid)

                # Defino valores estaticos
                filtro_limite_horas = 120  # GET_FILTRO_LIMITE_HS('MODIFICA_DOCUMENTOS', TRUE)
                diferencia_horas = (datetime.now() - fecha_documento).total_seconds() / 3600

                # Analizo que ct reactivo
                if ct=='0':
                    print("Resultado: No se reactiva por ser CT 0.\n")
                elif origen=="PROGRAMADO" and causa in [0, 40, 78, 85]:
                    print("Resultado: No se reactiva por no poseer la causa correcta.\n")
                elif not (diferencia_horas < filtro_limite_horas or filtro_limite_horas == 0):
                    print("Resultado: No se reactiva por poseer tiempo vencido.\n")
                elif repetido==0:
                    print("Resultado: Corresponde Agregar CT")
                elif repetido>0:
                    # print("Resultado: Corresponde Reactivar CT (update)")
                    registros_a_actualizar = self.analiza_resultados_update(id, objectid)

                    # Construye el registro a guardar
                    registro = {
                        "PROCESO": ahora,
                        "ID": id,
                        "DOCUMENTO": nro_documento,
                        "ORIGEN": origen,
                        "FECHA": fecha_documento.strftime("%d/%m/%Y %H:%M:%S"),
                        "ESTADO": estado,
                        "OBJECTID": objectid,
                        "CT": ct,
                        "AFECTACIONES": afectados,
                        "CAUSA": causa,
                        "REPETIDO": repetido,
                        "CANT_UPDATE": registros_a_actualizar,  # Este valor sigue siendo válido
                    }      
                
                    self.exporta(registro, "reactiva_ct.txt")

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logging.error(f"Error ejecutando SQL dinámico: {error.message}")
        finally:
            cursor.close()
            connection.close()

    def analiza_resultados_update(self, v_id, v_objectid):
        """Ejecuta un SELECT basado en las condiciones del UPDATE para obtener la cantidad de registros a actualizar."""
        select_query = """
            SELECT COUNT(*)
            FROM NEXUS_GIS.TABLA_ENREMTAT_DET DT
            WHERE
                DT.ID = :v_id
                AND DT.ELEMENT_ID = :v_objectid
        """
        connection = self.connect_to_database()
        cursor = connection.cursor()
        try:
            # Ejecuta el SELECT con las condiciones del UPDATE
            cursor.execute(select_query, v_id=v_id, v_objectid=v_objectid)
            count = cursor.fetchone()[0]  # Obtiene la cantidad de registros
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logging.error(f"Error ejecutando SELECT para análisis del UPDATE: {error.message}")
            count = 0  # Retorna 0 si ocurre un error
        finally:
            cursor.close()
            connection.close()
        return count

    def exporta(self, registro, archivo):
        """Guarda un registro en el archivo reactiva_ct.txt."""
        # archivo = "reactiva_ct.txt"
        try:
            with open(archivo, "a") as file:
                file.write(f"{registro}\n")
        except Exception as e:
            logging.error(f"Error escribiendo en el archivo {archivo}: {str(e)}")

# -----------------------------------------------------------------------------------
# Ejemplo de uso:
executor = CIRCUITO_103_GISPR03_REACTIVACT()
# sql= executor.obtiene_sql_dinamico()
# print(sql)
# ct= executor.obtiene_ct_desde_objectid(61767707)
# print(ct)
# analiza= executor.analiza_resultados_update(12745509,61702769)
# print(analiza)
# executor.ejecuta_sql_dinamico()
# executor.prueba_exporta()
