import psycopg2
from psycopg2 import Error

class QueriesPostgreSQL:
    def create_connection():
        connection = None
        try:
            connection = psycopg2.connect(
                host="localhost",
                database="pdvDB",  # Reemplaza con el nombre de tu base de datos
                user="your_username",  # Reemplaza con tu usuario
                password="your_password"  # Reemplaza con tu contraseña
            )
            print("Connection to PostgreSQL DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")

        return connection

    def execute_query(connection, query, data_tuple):
        cursor = connection.cursor()
        try:
            cursor.execute(query, data_tuple)
            connection.commit()
            print("Query executed successfully")
            return cursor.lastrowid
        except Error as e:
            print(f"The error '{e}' occurred")

    def execute_read_query(connection, query, data_tuple=()):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query, data_tuple)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"The error '{e}' occurred")

    def create_tables():
        connection = QueriesPostgreSQL.create_connection()

        tabla_productos = """
        CREATE TABLE IF NOT EXISTS productos(
         codigo TEXT PRIMARY KEY, 
         nombre TEXT NOT NULL, 
         precio REAL NOT NULL, 
         cantidad INTEGER NOT NULL
        );
        """

        tabla_usuarios = """
        CREATE TABLE IF NOT EXISTS usuarios(
         username TEXT PRIMARY KEY, 
         nombre TEXT NOT NULL, 
         password TEXT NOT NULL,
         tipo TEXT NOT NULL
        );
        """

        tabla_ventas = """
        CREATE TABLE IF NOT EXISTS ventas(
         id SERIAL PRIMARY KEY,  -- Cambié a SERIAL para autoincremento
         total REAL NOT NULL, 
         fecha TIMESTAMP,
         username TEXT NOT NULL, 
         FOREIGN KEY(username) REFERENCES usuarios(username)
        );
        """

        tabla_ventas_detalle = """
        CREATE TABLE IF NOT EXISTS ventas_detalle(
         id SERIAL PRIMARY KEY,  -- Cambié a SERIAL para autoincremento
         id_venta INTEGER NOT NULL, 
         precio REAL NOT NULL,
         producto TEXT NOT NULL,
         cantidad INTEGER NOT NULL,
         FOREIGN KEY(id_venta) REFERENCES ventas(id),
         FOREIGN KEY(producto) REFERENCES productos(codigo)
        );
        """

        QueriesPostgreSQL.execute_query(connection, tabla_productos, tuple()) 
        QueriesPostgreSQL.execute_query(connection, tabla_usuarios, tuple()) 
        QueriesPostgreSQL.execute_query(connection, tabla_ventas, tuple()) 
        QueriesPostgreSQL.execute_query(connection, tabla_ventas_detalle, tuple()) 


if __name__ == "__main__":
    from datetime import datetime, timedelta
    connection = QueriesPostgreSQL.create_connection()

    # Actualizar fecha de una venta
    fecha1 = datetime.today() - timedelta(days=5)
    nueva_data = (fecha1, 4)
    actualizar = """
    UPDATE
      ventas
    SET
      fecha = %s
    WHERE
      id = %s
    """

    QueriesPostgreSQL.execute_query(connection, actualizar, nueva_data)

    # Leer ventas
    select_ventas = "SELECT * from ventas"
    ventas = QueriesPostgreSQL.execute_read_query(connection, select_ventas)
    if ventas:
        for venta in ventas:
            print("type:", type(venta), "venta:", venta)

    # Leer detalles de ventas
    select_ventas_detalle = "SELECT * from ventas_detalle"
    ventas_detalle = QueriesPostgreSQL.execute_read_query(connection, select_ventas_detalle)
    if ventas_detalle:
        for venta in ventas_detalle:
            print("type:", type(venta), "venta:", venta)

    # Crear producto (ejemplo comentado)
    # crear_producto = """
    # INSERT INTO
    #   productos (codigo, nombre, precio, cantidad)
    # VALUES
    #     ('111', 'leche 1l', 20.0, 20),
    #     ('222', 'cereal 500g', 50.5, 15), 
    #     ('333', 'yogurt 1L', 25.0, 10)
    # """
    # QueriesPostgreSQL.execute_query(connection, crear_producto, tuple())

    # Leer productos
    select_products = "SELECT * from productos"
    productos = QueriesPostgreSQL.execute_read_query(connection, select_products)
    for producto in productos:
        print(producto)

    # Crear usuario (ejemplo comentado)
    # usuario_tuple = ('test', 'Persona 1', '123', 'admin')
    # crear_usuario = """
    # INSERT INTO
    #   usuarios (username, nombre, password, tipo)
    # VALUES
    #     (%s, %s, %s, %s);
    # """
    # QueriesPostgreSQL.execute_query(connection, crear_usuario, usuario_tuple)

    # Leer usuarios
    select_users = "SELECT * from usuarios"
    usuarios = QueriesPostgreSQL.execute_read_query(connection, select_users)
    for usuario in usuarios:
        print("type:", type(usuario), "usuario:", usuario)
