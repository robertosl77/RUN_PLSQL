import cx_Oracle

class Radio_Calles:
    def __init__(self):
        # self.user = "NEXUS_GIS"
        # self.password = "Alg0don"
        self.user = "rsleiva"
        self.password = "Alg0don"
        self.dsn = "LTRONXGISBDPR03.PRO.EDENOR:1528/GISPR03"

    def connect_to_database(self):
        """Establece la conexión con la base de datos Oracle."""
        try:
            connection = cx_Oracle.connect(
                self.user,
                self.password,
                self.dsn
            )
            return connection
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(f"Error al conectar a la base de datos: {error.message}")
            return None

    def valida_ct(self, ct):
        """Valida si un CT existe en la base de datos."""
        query = """
        SELECT 1
        FROM NEXUS_CCYB.CLIENTES_CCYB
        WHERE ct = :ct
        FETCH FIRST 1 ROWS ONLY
        """
        connection = self.connect_to_database()
        if connection is None:
            return None  # Indica que no se pudo conectar a la base de datos

        cursor = connection.cursor()

        try:
            cursor.execute(query, ct=ct)
            result = cursor.fetchone()
            return result is not None
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(f"Error validando el CT: {error.message}")
            return False
        finally:
            cursor.close()
            connection.close()

    def fetch_borders(self, cts):
        """Obtiene los límites combinados para múltiples CTs."""
        query = """
        WITH extremos AS (
            SELECT 
                ROUND(MIN(x), 4) AS x_min,
                ROUND(MAX(x), 4) AS x_max,
                ROUND(MIN(y), 4) AS y_min,
                ROUND(MAX(y), 4) AS y_max
            FROM NEXUS_CCYB.CLIENTES_CCYB
            WHERE ct IN ({})
        ),
        limites AS (
            SELECT DISTINCT 
                CASE 
                    WHEN ROUND(x, 4) = e.x_min AND ROUND(y, 4) = e.y_max THEN 'NOROESTE'
                    WHEN ROUND(x, 4) = e.x_max AND ROUND(y, 4) = e.y_max THEN 'NORESTE'
                    WHEN ROUND(x, 4) = e.x_min AND ROUND(y, 4) = e.y_min THEN 'SUROESTE'
                    WHEN ROUND(x, 4) = e.x_max AND ROUND(y, 4) = e.y_min THEN 'SURESTE'
                    WHEN ROUND(x, 4) = e.x_min THEN 'OESTE'
                    WHEN ROUND(x, 4) = e.x_max THEN 'ESTE'
                    WHEN ROUND(y, 4) = e.y_min THEN 'SUR'
                    WHEN ROUND(y, 4) = e.y_max THEN 'NORTE'
                END AS limite,
                c.calle
            FROM NEXUS_CCYB.CLIENTES_CCYB c
            CROSS JOIN extremos e
            WHERE c.ct IN ({})
              AND ((ROUND(x, 4) = e.x_min AND ROUND(y, 4) = e.y_max) OR 
                   (ROUND(x, 4) = e.x_max AND ROUND(y, 4) = e.y_max) OR 
                   (ROUND(x, 4) = e.x_min AND ROUND(y, 4) = e.y_min) OR 
                   (ROUND(x, 4) = e.x_max AND ROUND(y, 4) = e.y_min) OR 
                   ROUND(x, 4) = e.x_min OR
                   ROUND(x, 4) = e.x_max OR
                   ROUND(y, 4) = e.y_min OR
                   ROUND(y, 4) = e.y_max)
        )
        SELECT limite, calle
        FROM limites
        ORDER BY limite, calle
        """
        # Construir la lista de CTs para la query
        formatted_cts = ', '.join(f"'{ct}'" for ct in cts)
        query = query.format(formatted_cts, formatted_cts)

        connection = self.connect_to_database()
        if connection is None:
            # print("Error crítico: no se puede obtener límites porque no hay conexión a la base de datos.")
            return

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(f"Error ejecutando la consulta: {error.message}")
            return []
        finally:
            cursor.close()
            connection.close()

    def formato_resultado(self, results):
        """Formatea los resultados para incluir solo calles."""
        puntos_cardinales = [
            'NORTE', 'NORESTE', 'ESTE', 'SURESTE', 
            'SUR', 'SUROESTE', 'OESTE', 'NOROESTE'
        ]
        resultado_formateado = {punto: "" for punto in puntos_cardinales}

        for limite, calle in results:
            resultado_formateado[limite] = calle

        return resultado_formateado

    def obtiene_cts(self):
        """Permite al usuario agregar CTs uno a uno validando su existencia y evitando duplicados."""
        cts = []
        print("Ingrese los CTs uno a uno. Escriba 'fin' para terminar.")
        while True:
            ct = input("Ingrese un CT: ").strip()
            if ct.lower() == 'fin':
                break

            if ct in cts:
                print(f"CT {ct} ya fue agregado previamente. Ignorado.")
                continue

            is_valid = self.valida_ct(ct)
            if is_valid is None:  # Error crítico de conexión
                # print("Error crítico: no se puede continuar sin conexión a la base de datos.")
                return

            if is_valid:
                print(f"CT {ct} válido y agregado.")
                cts.append(ct)
            else:
                print(f"CT {ct} no existe en la base de datos.")

        return cts

if __name__ == "__main__":
    nexus_gis = Radio_Calles()

    # Interacción para ingresar CTs
    cts = nexus_gis.obtiene_cts()

    if cts:
        results = nexus_gis.fetch_borders(cts)
        formatted_results = nexus_gis.formato_resultado(results)

        print("\nResultados formateados:")
        for punto, calle in formatted_results.items():
            print(f"{punto}: {calle}")
    elif cts is None:
        pass
    else:
        print("No se ingresaron CTs válidos.")
