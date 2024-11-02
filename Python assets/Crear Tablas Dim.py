import pyodbc as pc
import pandas as pd
from sqlalchemy import create_engine
#creamos una variable donde guardemos la conexion al sv de SQL
server = 'MICHE\SQLEXPRESS'
db = 'DW_DataShop'
conexion = pc.connect(
driver='{SQL server}',
host = server ,
database = db,
autocommit=True)

#mostramos en pantalla si entramos
print("Conexion Existosa")

#creamos un cursor para almacenar info en una memoria
cursor = conexion.cursor()

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Cliente")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Cliente")
cursor.execute("DROP TABLE IF EXISTS Dim_Cliente")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_codCliente")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_codCliente")
cursor.execute("DROP TABLE IF EXISTS Dim_codCliente")

cursor.execute("DROP TABLE IF EXISTS STG_Fact_Ventas")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Ventas")
cursor.execute("DROP TABLE IF EXISTS Fact_Ventas")

cursor.execute("DROP TABLE IF EXISTS STG_Fact_Distribuidor")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Distribuidor")
cursor.execute("DROP TABLE IF EXISTS Fact_Distribuidor")

cursor.execute("DROP TABLE IF EXISTS STG_Fact_Reseller")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Reseller")
cursor.execute("DROP TABLE IF EXISTS Fact_Reseller")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Producto")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Producto")
cursor.execute("DROP TABLE IF EXISTS Dim_Producto")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Tiendas")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Tiendas")
cursor.execute("DROP TABLE IF EXISTS Dim_Tiendas")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Moneda")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Moneda")
cursor.execute("DROP TABLE IF EXISTS Dim_Moneda")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Pais")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Pais")
cursor.execute("DROP TABLE IF EXISTS Dim_Pais")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Tiempo")
cursor.execute("DROP TABLE IF EXISTS Dim_Tiempo")

##CREACION DE LAS TABLAS STG 

cursor.execute("""CREATE TABLE STG_Fact_ventas(
               Fecha_venta varchar(255),
               Codigo_Producto varchar (255), 
               Cantidad varchar(255),
               Precio_Venta varchar(255),
               Codigo_Cliente varchar(255),
               Codigo_Tienda varchar(255),
               Codigo_Pais varchar(255),
               Codigo_Moneda varchar (255),
               Reseller_Key varchar(255))
               """)
print("extraccion y colocacion existosa de fact ventas")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Fact_Distribuidor(
              Codigo_Producto varchar(255) ,
              Cantidad_Compra varchar(255),
              Precio_Compra varchar(255), 
              Fecha_Compra varchar(255), 
              Metodo_Pago varchar(255),
              Descuento_Aplicado varchar(255),
              Total_Compra varchar(255),
              Estado_Compra varchar(255))""")
print("extraccion y colocacion existosa de fact distribuidor")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Fact_Reseller(
              reseller_key VARCHAR(255),
              Reseller_Name VARCHAR(255),
              Codigo_Producto VARCHAR(255),
              Cantidad VARCHAR(255),
              FechaVenta VARCHAR(255),
              Codigo_Pais VARCHAR(255),
              Codigo_Moneda VARCHAR(255),
              Codigo_Cliente VARCHAR(255),
               )""")
print("extraccion y colocacion existosa de fact reseller")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_CodCliente(
              Codigo_Cliente varchar(255),
              Nombre varchar(255),
              Apellido varchar(255),
              Edad varchar(255),
              Codigo_Pais varchar(255))
""")
print("extraccion y colocacion existosa de dim codcliente")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Producto (
               codigo_Producto varchar(255),
               Descripcion varchar(255),
               Categoria varchar (255), 
               Marca varchar(255),
               Precio_Costo varchar(255),
               Precio_Venta_Sugerido varchar(255))
               """)


##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Tiendas (
               codigo_Tienda varchar(255),
               Codigo_Pais varchar(255),
               Descripcion varchar(255),
               Direccion varchar(255),
               Tipo_Tienda varchar(255))
               """)

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Tiempo (
            fecha varchar(255),
            anio varchar(255),
            mes varchar(255),
            nombre_mes varchar(255),
            trismestre varchar(255),
            dia varchar(255),
            nombre_dia varchar(255),
            numero_semana varchar (255))
               """)
print("extraccion y colocacion existosa de dim tiempo")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Pais (
               Codigo_Pais varchar(255),
               Pais varchar(255))
               """)
print("extraccion y colocacion existosa de dim pais")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Moneda (
              Codigo_Moneda varchar(255),
              Moneda varchar(255))
               """)
print("extraccion y colocacion existosa de dim moneda")

##CREACION DE LAS TABLAS STG 
cursor.execute("""CREATE TABLE STG_Dim_Cliente ( 
  Codigo_Cliente varchar(255),
  Codigo_Pais varchar(255),
  Telefono varchar(255),
  Mail varchar(255),
  Direccion varchar(255),
  Localidad varchar(255),
  Provincia varchar(255),
  CP varchar(255))
               """)
print("extraccion y colocacion existosa de Clientes")

# crear la tabla int_dim_clientes (intermedia) esta es la pre tabla a la final
cursor.execute("""CREATE TABLE INT_Dim_Cliente(
	Codigo_Cliente INT NOT NULL,
	Razon_Social varchar(255) NOT NULL,
	Telefono BIGINT NULL,
	Mail varchar(255) NOT NULL,
	Direccion varchar(255) NOT NULL,
	Localidad varchar(255) NOT NULL,
	Provincia varchar(255) NOT NULL,
	Cp int NOT NULL)
	""")
print("Tabla INT_dim_cliente creada con exito")

#CREAR TABLA FINAL Dim_Cliente
cursor.execute("""CREATE TABLE Dim_Cliente(
  Id_Cliente INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
	Codigo_Cliente INT NOT NULL,
	Razon_Social varchar(255) NOT NULL,
	Telefono BIGINT NOT NULL,
	Mail varchar(255) NOT NULL,
	Direccion varchar(255) NOT NULL,
	Localidad varchar(255) NOT NULL,
	Provincia varchar(255) NOT NULL,
	Cp int NOT NULL)
	""")
print("Tabla dim_cliente creada con exito")

#CREAR LA TABLA INT_dim_CodCliente (INTERMEDIA)
cursor.execute("""CREATE TABLE INT_Dim_codCliente( 
	Codigo_Cliente INT NOT NULL,
	Nombre varchar(255) NOT NULL,
	Apellido int NULL,
	Edad int NOT NULL,
	Codigo_Pais int NOT NULL)
	""")
print("Tabla INT_dim_codCliente creada con exito")

#CREAR LA TABLA dim_CodCliente (FINAL)
cursor.execute("""CREATE TABLE Dim_codCliente(
    Id_codCliente INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
	Codigo_Cliente INT NOT NULL,
	Nombre varchar(255) NOT NULL,
	Apellido int NULL,
	Edad int NOT NULL,
	Codigo_Pais int NOT NULL)
	""")
print("Tabla dim_codCliente creada con exito")

#CREAR LA TABLA INT_Fact_Ventas (INTERMEDIA)
cursor.execute("""CREATE TABLE INT_Fact_Ventas(
  Fecha_Venta DATE NOT NULL,
  Codigo_Producto INT NOT NULL,
  cantidad INT NOT NULL,
  Precio_Venta INT NOT NULL,
  Codigo_Cliente INT NOT NULL,
  Codigo_Tienda INT NOT NULL,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Reseller_Key INT NOT NULL)
	""")
print("Tabla INT_Fact_Ventas creada con exito")

#CREAR LA TABLA Fact_Ventas (FINAL)
cursor.execute(""" CREATE TABLE Fact_Ventas (
  ID_Venta INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  ID_Producto INT  NOT NULL,
  ID_Cliente INT NOT NULL,
  ID_Tienda INT NOT NULL,
  Fecha_Venta Date NOT NULL,
  Codigo_Producto INT NOT NULL,
  cantidad INT NOT NULL,
  Precio_Venta INT NOT NULL,
  Codigo_Cliente INT NOT NULL,
  Codigo_Tienda INT NOT NULL,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Reseller_Key INT NOT NULL,)
	""")
print("Tabla Fact_Ventas creada con exito")

#CREAR LA TABLA INT_Fact_Distribuidor (INTERMEDIA)
cursor.execute("""CREATE TABLE INT_Fact_Distribuidor (
  Codigo_Producto INT NOT NULL,
  Cantidad_Compra INT NOT NULL,
  Precio_Compra INT NOT NULL,
  Fecha_Compra DATE,
  Metodo_Pago VARCHAR(255) NOT NULL,
  Descuento_Aplicado VARCHAR(255) NULL,
  Total_Compra Decimal (10,2) NOT NULL,
  Estado_Compra varchar(255) NULL)
	""")
print("Tabla INT_Fact_Distribuidor creada con exito")

#CREAR LA TABLA Fact_Distribuidor (FINAL)
cursor.execute(""" CREATE TABLE Fact_Distribuidor (
  ID_Distribuidor INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  Codigo_Producto INT NOT NULL,
  Cantidad_Compra INT NOT NULL,
  Precio_Compra INT NOT NULL,
  Fecha_Compra DATE,
  Metodo_Pago Varchar(255) NOT NULL,
  Descuento_Aplicado varchar(255) NULL,
  Total_Compra INT NOT NULL,
  Estado_Compra varchar(255) NULL)
	""")
print("Tabla Fact_Distribuidor creada con exito")


#CREAR LA TABLA INT_Fact_Reseller (INTERMEDIA)
cursor.execute(""" CREATE TABLE INT_Fact_reseller (
  reseller_key int NOT NULL,
  Reseller_Name VARCHAR(255) NOT NULL,
  Codigo_Producto int NOT NULL,
  Cantidad int NOT NULL,
  FechaVenta DATE ,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Codigo_Cliente INT NOT NULL)
	""")
print("Tabla INT_Fact_Reseller creada con exito")

#CREAR LA TABLA Fact_Reseller (FINAL)
cursor.execute(""" CREATE TABLE Fact_Reseller(
  reseller_id INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  reseller_key int NOT NULL,
  Reseller_Name VARCHAR(255) NOT NULL,
  Codigo_Producto int NOT NULL,
  Cantidad int NOT NULL,
  FechaVenta DATE NOT NULL,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Codigo_Cliente INT NOT NULL)
	""")
print("Tabla Fact_Reseller creada con exito")

#CREAR LA TABLA INT_Dim_Productos (INTERMEDIA)
cursor.execute(""" CREATE TABLE INT_Dim_Producto  (
  Codigo_Producto INT NOT NULL,
  Descripcion varchar(255) NOT NULL,
  Categoria varchar(255) NOT NULL,
  Marca varchar (255) NOT NULL,
  Precio_Costo INT NOT NULL,
  Precio_Venta_Sugerido INT NOT NULL )
	""")
print("Tabla INT_Dim_Productos creada con exito")

#CREAR LA TABLA Dim_Productos (FINAL)
cursor.execute("""CREATE TABLE Dim_Producto (
  id_producto INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  Codigo_Producto INT NOT NULL,
  Descripcion varchar(255) NOT NULL,
  Categoria varchar(255) NOT NULL,
  Marca varchar (255) NOT NULL,
  Precio_Costo INT NOT NULL,
  Precio_Venta_Sugerido INT NOT NULL )
	""")
print("Tabla Dim_Productos creada con exito")

#CREAR LA TABLA INT_Dim_Tiendas (INTERMEDIA)
cursor.execute(""" CREATE TABLE INT_Dim_Tiendas (
 Codigo_Tienda INT NOT NULL,
 Codigo_Pais INT NOT NULL,
 Descripcion varchar(255) NULL,
 Direccion varchar(255) NULL,
 Tipo_Tienda varchar(255) NULL)
  """)
print("Tabla INT_Dim_Tiendas creada con exito")

#CREAR LA TABLA Dim_Tiendas (FINAL)
cursor.execute("""CREATE TABLE Dim_Tiendas (
 	id_Tienda INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
 	Codigo_Tienda INT NOT NULL,
 	Codigo_Pais INT NOT NULL,
 	Descripcion varchar(255),
 	Direccion varchar(255),
 	Tipo_Tienda varchar(255))
""")
print("Tabla DIM_Tiendas creada con exito")

#CREAR LA TABLA INT_Dim_Moneda (INTERMEDIA)
cursor.execute(""" CREATE TABLE INT_Dim_Moneda (
  Codigo_Moneda INT NOT NULL,
  Moneda varchar(255) NOT NULL)
  """)
print("Tabla INT_Dim_Moneda creada con exito")

#CREAR LA TABLA Dim_Moneda (FINAL)
cursor.execute("""CREATE TABLE Dim_Moneda (
  id_moneda INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Moneda varchar(255) NOT NULL)
""")
print("Tabla DIM_Moneda creada con exito")

#CREAR LA TABLA INT_Dim_Pais (INTERMEDIA)
cursor.execute(""" CREATE TABLE INT_Dim_Pais(
  Codigo_Pais INT NOT NULL,
  pais varchar(255) NOT NULL)
  """)
print("Tabla INT_Dim_Pais creada con exito")

#CREAR LA TABLA Dim_Pais (FINAL)
cursor.execute("""CREATE TABLE Dim_Pais (
  id_pais INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  Codigo_Pais INT NOT NULL,
  pais varchar(255) NOT NULL)
  """)
print("Tabla DIM_Pais creada con exito")


#CREAR LA TABLA Dim_Tiempo (FINAL)
cursor.execute("""CREATE TABLE Dim_Tiempo (
    Tiempo_Key smalldatetime PRIMARY KEY,
    Anio INT,
    Mes INT,
    Mes_Nombre varchar(20),
    Semestre INT,
    Trimestre INT,
    Semana_Anio INT,
    Semana_Nro_Mes INT,
    Dia INT,
    Dia_Nombre varchar(20),
    Dia_Semana_Nro INT)""")


#funcion commit (Guarda los datos hechos anteriormente)
conexion.commit()
#cerrar la conexion ((Siempre va Ultima))
conexion.close()