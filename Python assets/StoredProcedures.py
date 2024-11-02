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
#creamos un cursor para almacenar info en una memoria
cursor = conexion.cursor()

#STORED PROCEDURE DE LA INT DIM CLIENTE (HAY QUE HACER PARA TODAS LAS INT DE LA DB)
#INSERT INTO {NOMBRE TABLA INT} (CAMPOS FINALES DE LA TABLA INT)
#USAR LA FUNCION CAST EN EL SELECT PARA SE;ALIZAR EL TIPO DE DATO QUE QUEREMOS QUE TENGA LA COLUMNA (CAST(Razon_Social as varchar(255)) as RazonSocial)


#GENERAR LOS DROP DE LAS TABLAS 
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Fact_Ventas""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Fact_Reseller""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Fact_Distribuidor""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_Moneda""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_Pais""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_Productos""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_Tiendas""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_Cliente""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_INT_Dim_codCliente""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Fact_Ventas""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Fact_Reseller""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Fact_Distribuidor""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_Tiendas""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_Pais""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_Moneda""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_Producto""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_Cliente""")
cursor.execute(""" DROP PROCEDURE IF EXISTS SP_Carga_Dim_codCliente""")
cursor.execute("DROP PROCEDURE IF EXISTS Sp_Genera_Dim_Tiempo")

cursor.execute("DROP TABLE IF EXISTS STG_Dim_Cliente")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Cliente")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_codCliente")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_codCliente")


cursor.execute("DROP TABLE IF EXISTS STG_Fact_Ventas")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Ventas")


cursor.execute("DROP TABLE IF EXISTS STG_Fact_Distribuidor")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Distribuidor")


cursor.execute("DROP TABLE IF EXISTS STG_Fact_Reseller")
cursor.execute("DROP TABLE IF EXISTS INT_Fact_Reseller")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_Producto")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Producto")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_Tiendas")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Tiendas")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_Moneda")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Moneda")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_Pais")
cursor.execute("DROP TABLE IF EXISTS INT_Dim_Pais")


cursor.execute("DROP TABLE IF EXISTS STG_Dim_Tiempo")



# Eliminamos y cargamos stg/int
cursor.execute("""
    CREATE TABLE STG_Fact_ventas(
               Fecha_venta varchar(255),
               Codigo_Producto varchar (255), 
               Cantidad varchar(255),
               Precio_Venta varchar(255),
               Codigo_Cliente varchar(255),
               Codigo_Tienda varchar(255),
               Codigo_Pais varchar(255),
               Codigo_Moneda varchar (255),
               Reseller_Key varchar(255));

    CREATE TABLE INT_Fact_Ventas(
  Fecha_Venta DATE NOT NULL,
  Codigo_Producto INT NOT NULL,
  cantidad INT NOT NULL,
  Precio_Venta INT NOT NULL,
  Codigo_Cliente INT NOT NULL,
  Codigo_Tienda INT NOT NULL,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Reseller_Key INT NOT NULL);
  """)

fact_ventas= pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\ventas - ventas.csv")

lista_fact_ventas = fact_ventas.values.tolist()

cursor.executemany("INSERT INTO STG_Fact_ventas Values(?,?,?,?,?,?,?,?,?)", lista_fact_ventas)
print("extraccion y colocacion existosa de STG_Fact_Ventas")

#Cremos el SP_Carga_INT_Fact_Ventas 
cursor.execute("""
CREATE PROCEDURE SP_Carga_INT_Fact_Ventas
AS
BEGIN

    INSERT INTO INT_Fact_Ventas (Fecha_Venta, Codigo_Producto,cantidad,Precio_Venta,Codigo_Cliente,Codigo_Tienda,Codigo_Pais, Codigo_Moneda, Reseller_Key)
SELECT 
CAST(Fecha_venta as date) as FechaVenta,
CAST(Codigo_Producto as int) as Codigo_Producto ,
CAST(cantidad as int) as Cantidad ,
CAST(Precio_Venta as int) as PrecioVenta,
CAST(Codigo_Cliente as int) as Codigo_Cliente ,
CAST(Codigo_Tienda as int) as Codigo_Tienda,
CAST(Codigo_Pais as int) as Codigo_Pais,
CAST(Codigo_Moneda as int) as Codigo_Moneda,
CAST(Reseller_Key as int) as Reseller_Key
FROM STG_Fact_ventas
END 
               """)
print("SP_Carga_INT_Fact_Ventas creada con exito")

# Eliminamos y creamos tabls stg/int
cursor.execute("""
CREATE TABLE STG_Fact_Reseller(
              reseller_key VARCHAR(255),
              Reseller_Name VARCHAR(255),
              Codigo_Producto VARCHAR(255),
              Cantidad VARCHAR(255),
              FechaVenta VARCHAR(255),
              Codigo_Pais VARCHAR(255),
              Codigo_Moneda VARCHAR(255),
              Codigo_Cliente VARCHAR(255),
               );
    
CREATE TABLE INT_Fact_reseller (
  reseller_key int NOT NULL,
  Reseller_Name VARCHAR(255) NOT NULL,
  Codigo_Producto int NOT NULL,
  Cantidad int NOT NULL,
  FechaVenta DATE ,
  Codigo_Pais INT NOT NULL,
  Codigo_Moneda INT NOT NULL,
  Codigo_Cliente INT NOT NULL);""")

reseller= pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\reseller.csv")

lista_reseller =  reseller.values.tolist()

cursor.executemany("INSERT INTO STG_Fact_reseller Values (?,?,?,?,?,?,?,?)", lista_reseller)
print("extraccion y colocacion existosa de STG_Fact_Reseller")

#Cremos el SP_Carga_INT_Fact_reseller 
cursor.execute("""
CREATE PROCEDURE SP_Carga_INT_Fact_reseller
AS
BEGIN         
    INSERT INTO INT_Fact_reseller(FechaVenta,Reseller_Key, Reseller_Name,Codigo_Producto,Cantidad,Codigo_Pais,Codigo_Moneda,Codigo_Cliente)
SELECT 
CAST(FechaVenta as date) AS FechaVenta,
CAST(reseller_key as int) as resellerKey,
CAST(Reseller_Name as varchar(255)) as resellerName ,
CAST(Codigo_Producto as int) as codigoProducto ,
CAST(cantidad as int) as Cantidad,
CAST(Codigo_Pais as int) as codigoPais ,
CAST(Codigo_Moneda as int) as codigoMoneda,
CAST(Codigo_Cliente as int) as codigoCliente
FROM STG_Fact_reseller
END
         """)
print("SP_Carga_INT_Fact_Reseller creada con exito")


# Eliminamos y creamos tablas stg/int
cursor.execute("""
               CREATE TABLE STG_Fact_Distribuidor(
              Codigo_Producto varchar(255) ,
              Cantidad_Compra varchar(255),
              Precio_Compra varchar(255), 
              Fecha_Compra varchar(255), 
              Metodo_Pago varchar(255),
              Descuento_Aplicado varchar(255),
              Total_Compra varchar(255),
              Estado_Compra varchar(255));
               
            CREATE TABLE INT_Fact_Distribuidor (
            Codigo_Producto INT NOT NULL,
            Cantidad_Compra INT NOT NULL,
            Precio_Compra INT NOT NULL,
            Fecha_Compra DATE,
            Metodo_Pago VARCHAR(255) NOT NULL,
            Descuento_Aplicado VARCHAR(255) NULL,
            Total_Compra Decimal (10,2) NOT NULL,
            Estado_Compra varchar(255) NULL);
            """)

fact_distribuidor= pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\fact_distribuidor.csv")
lista_fact_distribuidor = fact_distribuidor.values.tolist() 
cursor.executemany("INSERT INTO STG_Fact_Distribuidor Values(?,?,?,?,?,?,?,?)", lista_fact_distribuidor)
print("extraccion y colocacion exitosa de STG_Fact_Distribuidor")

#Cremos el SP_Carga_INT_Fact_Distribuidor
cursor.execute("""
CREATE PROCEDURE SP_Carga_INT_Fact_Distribuidor
AS
BEGIN
           
    INSERT INTO INT_Fact_Distribuidor(Codigo_Producto, Cantidad_Compra,Precio_Compra,Fecha_Compra,Metodo_Pago,Descuento_Aplicado,Total_Compra,Estado_Compra)
SELECT 
CAST(Codigo_Producto as int) as codigoProducto,
CAST(Cantidad_Compra as int) as Cantidad_Compra ,
CAST(Precio_Compra as int) as Precio_Compra ,
CAST(Fecha_Compra as date) as Fecha_Compra,
CAST(Metodo_Pago as varchar(255)) as Metodo_Pago ,
CAST(Descuento_Aplicado as varchar(255)) as Descuento_Aplicado,
CAST(Total_Compra as Decimal (10,2)) as Total_Compra,
CAST(Estado_Compra as varchar(255)) as Estado_Compra
FROM STG_Fact_Distribuidor
END
 """)
print("SP_Carga_INT_Fact_Distribuidor creada con exito")


# Creamos y eliminamos tablas stg/int
cursor.execute("""
               CREATE TABLE STG_Dim_Moneda (
              Codigo_Moneda varchar(255),
              Moneda varchar(255));
               
    CREATE TABLE INT_Dim_Moneda (
  Codigo_Moneda INT NOT NULL,
  Moneda varchar(255) NOT NULL)
  """)

moneda = pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\moneda.csv")
lista_moneda = moneda.values.tolist()

cursor.executemany("INSERT INTO STG_Dim_Moneda VALUES (?,?)",lista_moneda)
print("extraccion y colocacion exitosa de la tabla STG_Dim_Moneda")

#Cremos el SP_Carga_INT_Dim_Moneda
cursor.execute(""" 
CREATE PROCEDURE SP_Carga_INT_Dim_Moneda
AS
BEGIN          
    INSERT INTO INT_Dim_Moneda(Codigo_Moneda, Moneda)
SELECT 
CAST(Codigo_Moneda as int) as Codigo_Moneda,
CAST(Moneda as varchar(255)) as Moneda
FROM STG_Dim_Moneda
END
""")
print("SP_Carga_INT_Dim_Moneda creada con exito")

#Eliminamos y creamos tablas stg/int
cursor.execute("""
               CREATE TABLE STG_Dim_Pais (
               Codigo_Pais varchar(255),
               Pais varchar(255));

               CREATE TABLE INT_Dim_Pais(
                Codigo_Pais INT NOT NULL,
                 pais varchar(255) NOT NULL);""")

pais= pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\pais.csv")
lista_pais = pais.values.tolist()
cursor.executemany("INSERT INTO STG_Dim_Pais VALUES (?,?)",lista_pais)
print("extraccion y colocacion exitosa de la tabla STG_Dim_Pais")

###falta el SP_CARGA_INT_Pais
cursor.execute(""" 
            CREATE PROCEDURE SP_Carga_INT_Dim_Pais
AS
BEGIN
    INSERT INTO INT_Dim_Pais(Codigo_Pais,Pais)
SELECT 
CAST(Codigo_Pais as int) as Codigo_Pais ,
CAST(Pais as varchar(255)) as Pais
FROM STG_Dim_Pais
END
""")
print("SP_Carga_INT_Dim_Pais creada con exito")

#Eliminamos y creamos tablas stg/int
cursor.execute("""
               CREATE TABLE STG_Dim_Producto (
               codigo_Producto varchar(255),
               Descripcion varchar(255),
               Categoria varchar (255), 
               Marca varchar(255),
               Precio_Costo varchar(255),
               Precio_Venta_Sugerido varchar(255));

    CREATE TABLE INT_Dim_Producto  (
    Codigo_Producto INT NOT NULL,
  Descripcion varchar(255) NOT NULL,
  Categoria varchar(255) NOT NULL,
  Marca varchar (255) NOT NULL,
  Precio_Costo INT NOT NULL,
  Precio_Venta_Sugerido INT NOT NULL );
  """)

dim_Producto = pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\dim_producto.csv")
lista_dim_producto = dim_Producto.values.tolist()
cursor.executemany("INSERT INTO STG_Dim_Producto Values (?,?,?,?,?,?)", lista_dim_producto)
print("extraccion y colocacion exitosa de STG_dim_Producto")

#CREAMOS EL SP_Carga_INT_Dim_Productos
cursor.execute("""
CREATE PROCEDURE SP_Carga_INT_Dim_Productos
AS
BEGIN

    INSERT INTO INT_Dim_Producto(Codigo_Producto, Descripcion,Categoria,Marca,Precio_Costo,Precio_Venta_Sugerido)
SELECT 
CAST(Codigo_Producto as int) as Codigo_Producto,
CAST(Descripcion as varchar(255)) as Descripcion ,
CAST(Categoria as varchar(255)) as Categoria ,
CAST(Marca as varchar(255)) as Marca,
CAST(Precio_Costo as int) as PrecioCosto ,
CAST(Precio_Venta_Sugerido as int) as Precio_venta_Sugerido
FROM STG_Dim_Producto
END
""")
print("SP_Carga_INT_Dim_Productos creada con exito")



#creamos la stg/Int de Dim_Tiendas
cursor.execute("""CREATE TABLE STG_Dim_Tiendas (
               codigo_Tienda varchar(255),
               Codigo_Pais varchar(255),
               Descripcion varchar(255),
               Direccion varchar(255),
               Tipo_Tienda varchar(255));

 CREATE TABLE INT_Dim_Tiendas (
 Codigo_Tienda INT NOT NULL,
 Codigo_Pais INT NOT NULL,
 Descripcion varchar(255) NULL,
 Direccion varchar(255) NULL,
 Tipo_Tienda varchar(255) NULL); """)
tiendas= pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\tiendas.csv")
lista_tiendas = tiendas.values.tolist()
cursor.executemany("INSERT INTO STG_Dim_Tiendas Values (?,?,?,?,?)", lista_tiendas)
print("Extraccion y colocacion existosa de la tabla STG_Dim_Tiendas ")

#CREAMOS EL SP_Carga_INT_Dim_Tiendas
cursor.execute(""" 
CREATE PROCEDURE SP_Carga_INT_Dim_Tiendas
AS
BEGIN          

    INSERT INTO INT_Dim_Tiendas (Codigo_Tienda,Codigo_Pais,Descripcion,Direccion,Tipo_Tienda)
SELECT 
CAST(codigo_Tienda as INT) as Codigo_Producto,
CAST(Codigo_Pais as INT) as Codigo_Pais ,
CAST(Descripcion as varchar(255)) as Descripcion,
CAST(Direccion as varchar(255)) as Direccion,
CAST(Tipo_Tienda as varchar(255)) as Tipo_Tienda
FROM STG_Dim_Tiendas
END
""")
print("SP_Carga_INT_Dim_Tiendas creada con exito")



#CREAMOS STG/INT Dim_Cliente
cursor.execute("""CREATE TABLE STG_Dim_Cliente ( 
  Codigo_Cliente varchar(255),
  Codigo_Pais varchar(255),
  Telefono varchar(255),
  Mail varchar(255),
  Direccion varchar(255),
  Localidad varchar(255),
  Provincia varchar(255),
  CP varchar(255));
               
               CREATE TABLE INT_Dim_Cliente(
	Codigo_Cliente INT NOT NULL,
	Razon_Social varchar(255) NOT NULL,
	Telefono BIGINT NULL,
	Mail varchar(255) NOT NULL,
	Direccion varchar(255) NOT NULL,
	Localidad varchar(255) NOT NULL,
	Provincia varchar(255) NOT NULL,
	Cp int NOT NULL);
               """)
clientes=pd.read_csv(r"C:\Users\Santimiche\Desktop\Python assets\Tablas\clientes.csv" )
lista_clientes = clientes.values.tolist()
cursor.executemany("INSERT INTO STG_Dim_Cliente Values (?,?,?,?,?,?,?,?)", lista_clientes)
print("extraccion y colocacion exitosa de STG_Dim_Cliente")

#CREAMOS EL SP_Carga_INT_Dim_Cliente
cursor.execute("""
CREATE PROCEDURE SP_Carga_INT_Dim_Cliente
AS
BEGIN

    INSERT INTO INT_Dim_Cliente (Codigo_Cliente , Razon_Social, Telefono, Mail, Direccion, Localidad, Provincia, Cp)
SELECT 
CAST(Codigo_Cliente as int) as Codigo_Cliente,
CAST(Codigo_Pais as varchar(255)) as Codigo_Pais ,
CAST(telefono as BIGINT) as Telefono ,
CAST(mail as varchar(255)) as Mail,
CAST(direccion as varchar(255)) as Direccion ,
CAST(localidad as varchar(255)) as Localidad,
CAST(provincia as varchar(255)) as Provincia,
CAST(cp as int) as Cp
FROM STG_Dim_Cliente
END""")
print("SP_Carga_INT_Dim_Cliente creada con exito")

### Cremos el SP_Carga_INT_Dim_codCliente 
#cursor.execute("""
#CREATE PROCEDURE SP_Carga_INT_Dim_CodCliente
#AS
#BEGIN
 #   INSERT INTO INT_Dim_CodCliente (Codigo_Cliente,Nombre,Apellido,Edad,Codigo_Pais)
#SELECT 
#CAST(Codigo_Cliente as int) as Codigo_Cliente ,
#CAST(Nombre as varchar(255)) as Nombre ,
#CAST(Apellido as varchar(255)) as Apellido,
#CAST(Edad as int) as Edad ,
#CaAST(Codigo_Pais as int) as Codigo_Pais
#FROM STG_Dim_CodCliente
#END
#""")
#print("SP_Carga_INT_Dim_codCliente creada con exito")

#CREAMOS EL SP DE LAS TABLAS FINALES:


print("SP_Carga_Fact_Ventas creada con exito")
#SP_CARGA_Fact_Reseller
cursor.execute("""CREATE PROCEDURE SP_CARGA_Fact_Reseller
AS
BEGIN
    -- Actualizar registros existentes en Fact_Reseller
    UPDATE fr
    SET 
	fr.Reseller_Key = ifr.Reseller_Key,
	fr.Reseller_Name = ifr.Reseller_Name,
	fr.Codigo_Producto = ifr.Codigo_Producto,
	fr.Cantidad = ifr.Cantidad,
	fr.FechaVenta = ifr.FechaVenta,
	fr.Codigo_Pais = ifr.Codigo_Pais,
	fr.Codigo_Moneda = ifr.Codigo_Moneda,
	fr.Codigo_Cliente = ifr.Codigo_Cliente
    FROM Fact_Reseller as fr
    INNER JOIN INT_Fact_reseller ifr ON fr.Codigo_Producto = ifr.Codigo_Producto;

    -- Insertar nuevos registros en Fact_Reseller
    INSERT INTO Fact_Reseller(Reseller_Key, Reseller_Name, Codigo_Producto, Cantidad, FechaVenta, Codigo_Pais, Codigo_Moneda, Codigo_Cliente)
    SELECT 
          Reseller_Key, Reseller_Name, Codigo_Producto, Cantidad, FechaVenta, Codigo_Pais, Codigo_Moneda, Codigo_Cliente
    FROM INT_Fact_reseller ifr
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Fact_Reseller fr
			WHERE fr.Codigo_Producto = ifr.Codigo_Producto
    );
END
                """)
print("SP_Carga_Fact_Reseller creada con exito")
#SP_CARGA_Fact_Distribuidor
cursor.execute("""
               CREATE PROCEDURE SP_CARGA_Fact_Distribuidor
AS
BEGIN
    -- Actualizar registros existentes en Fact_Distribuidor
    UPDATE fd
    SET 
	fd.Codigo_Producto = ifd.Codigo_Producto,
	fd.Cantidad_Compra = ifd.Cantidad_Compra,
	fd.Precio_Compra = ifd.Precio_Compra,
	fd.Fecha_Compra = ifd.Fecha_Compra,
	fd.Metodo_Pago = ifd.Metodo_Pago,
	fd.Descuento_Aplicado = ifd.Descuento_Aplicado,
	fd.Total_Compra = ifd.Total_Compra,
	fd.Estado_Compra  = ifd.Estado_Compra
    FROM Fact_Distribuidor as fd
    INNER JOIN INT_Fact_Distribuidor ifd ON fd.Codigo_Producto = ifd.Codigo_Producto;

    -- Insertar nuevos registros en Fact_Reseller
    INSERT INTO Fact_Distribuidor(Codigo_Producto, Cantidad_Compra, Precio_Compra, Fecha_Compra, Metodo_Pago, Descuento_Aplicado, Total_Compra, Estado_Compra)
    SELECT 
			Codigo_Producto, Cantidad_Compra, Precio_Compra, Fecha_Compra, Metodo_Pago, Descuento_Aplicado, Total_Compra, Estado_Compra
    FROM INT_Fact_Distribuidor ifd
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Fact_Distribuidor fd
			WHERE fd.Codigo_Producto = ifd.Codigo_Producto
    );
END""")
print("SP_Carga_Fact_Distribuidor creada con exito")
#SP_CARGA_Dim_Tiendas
cursor.execute("""CREATE PROCEDURE SP_CARGA_Dim_Tiendas
AS
BEGIN
    -- Actualizar registros existentes en Dim_Tiendas
    UPDATE dt
    SET 
        dt.Codigo_Tienda = idt.Codigo_Tienda,
		dt.Codigo_Pais = idt.Codigo_Pais,
		dt.Descripcion = idt.Descripcion,
		dt.Direccion = idt.Direccion,
		dt.Tipo_Tienda = idt.Tipo_Tienda
    FROM Dim_Tiendas dt
    INNER JOIN INT_Dim_Tiendas idt ON dt.Codigo_Tienda = idt.Codigo_Tienda

    -- Insertar nuevos registros en Dim_Tiendas
    INSERT INTO Dim_Tiendas (Codigo_Tienda, Codigo_Pais, Descripcion, Direccion, Tipo_Tienda)
    SELECT 
			Codigo_Tienda, Codigo_Pais, Descripcion, Direccion, Tipo_Tienda
    FROM INT_Dim_Tiendas idt
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Dim_Tiendas dt 
        WHERE dt.Codigo_Tienda = idt.Codigo_Tienda
    );
               END;
    """)
print("SP_Carga_Dim_Tiendas creada con exito")

#S -- Stored Procedure que genera la informacion para Dim_Tiempo 
cursor.execute("""
CREATE PROCEDURE Sp_Genera_Dim_Tiempo
@anio Int 
AS 
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT OFF;
    SET ARITHIGNORE ON;
    SET DATEFIRST 1;
    SET DATEFORMAT mdy; 
   
    DECLARE @dia smallint;
    DECLARE @mes smallint;
    DECLARE @f_txt varchar(10);
    DECLARE @fecha smalldatetime;
    DECLARE @key int;
    DECLARE @vacio smallint;
    DECLARE @fin smallint;
    DECLARE @fin_mes int;
    DECLARE @anioperiodicidad int;
    
    SELECT @dia = 1, @mes = 1;
    SELECT @f_txt = Convert(char(2), @mes) + '/' + Convert(char(2), @dia) + '/' + Convert(char(4), @anio);
    SELECT @fecha = Convert(smalldatetime, @f_txt);
    SELECT @anioperiodicidad = @anio;

    /************************************/
    /* Se chequea que el año a procesar */
    /* no exista en la tabla TIME       */
    /************************************/
    IF (SELECT Count(*) FROM dim_tiempo WHERE anio = @anio) > 0 
    BEGIN
        PRINT 'El año que ingresó ya existe en la tabla';
        PRINT 'Procedimiento CANCELADO.................';
        RETURN 0;
    END;

    /*************************/
    /* Se inserta día a día   */
    /* hasta terminar el año  */
    /*************************/
    SELECT @fin = @anio + 1;

    WHILE (Year(@fecha) < @fin) 
    BEGIN
        --Armo la fecha
        SET @f_txt = Convert(char(4), Datepart(yyyy, @fecha)) + 
                     RIGHT('0' + Convert(varchar(2), Datepart(mm, @fecha)), 2) +
                     RIGHT('0' + Convert(varchar(2), Datepart(dd, @fecha)), 2);
        
        --Calculo el último día del mes
        SET @fin_mes = Day(Dateadd(d, -1, Dateadd(m, 1, Dateadd(d, - Day(@fecha) + 1, @fecha))));
        
        --Inserto el registro
        INSERT Dim_Tiempo (Tiempo_Key, Anio, Mes, Mes_Nombre, Semestre, Trimestre, Semana_Anio ,Semana_Nro_Mes, Dia, Dia_Nombre, Dia_Semana_Nro)
        SELECT 
            tiempo_key = @fecha,
            anio = Datepart(yyyy, @fecha),
            mes = Datepart(mm, @fecha),
            mes_nombre = CASE Datename(mm, @fecha)
                WHEN 'January' THEN 'Enero'
                WHEN 'February' THEN 'Febrero'
                -- etc. (continúa para los demás meses)
                ELSE Datename(mm, @fecha)
            END,
            semestre = CASE WHEN Datepart(mm, @fecha) BETWEEN 1 AND 6 THEN 1 ELSE 2 END,
            trimestre = Datepart(qq, @fecha),
            semana_anio = Datepart(wk, @fecha),
            semana_nro_mes = Datepart(wk, @fecha) - Datepart(week, Dateadd(dd, -Day(@fecha)+1, @fecha)) + 1,
            dia = Datepart(dd, @fecha),
            dia_nombre = CASE Datename(dw, @fecha)
                WHEN 'Monday' THEN 'Lunes'
                WHEN 'Tuesday' THEN 'Martes'
                -- etc. (continúa para los demás días)
                ELSE Datename(dw, @fecha)
            END,
            dia_semana_nro = Datepart(dw, @fecha);
        
        -- Incremento la fecha
        SELECT @fecha = Dateadd(dd, 1, @fecha);
    END;
END; 
               """)

#SP_CARGA_Dim_Producto
cursor.execute("""CREATE PROCEDURE SP_CARGA_Dim_Producto
AS
BEGIN
    -- Actualizar registros existentes en Dim_Productos
    UPDATE dp
    SET 
        dp.Codigo_Producto = idp.Codigo_Producto,
		dp.Descripcion = idp.Descripcion ,
		dp.Categoria = idp.Categoria ,
		dp.Marca = idp.Marca,
		dp.Precio_Costo = idp.Precio_Costo,
		dp.Precio_Venta_Sugerido = idp.Precio_Venta_Sugerido
    FROM Dim_Producto dp
    INNER JOIN INT_Dim_Producto idp ON dp.Codigo_Producto = idp.Codigo_Producto

    -- Insertar nuevos registros en Dim_Tiendas
    INSERT INTO Dim_Producto(Codigo_Producto, Descripcion, Categoria, Marca, Precio_Costo, Precio_Venta_Sugerido)
    SELECT 
			Codigo_Producto, Descripcion, Categoria, Marca, Precio_Costo, Precio_Venta_Sugerido
    FROM INT_Dim_Producto idp
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Dim_Producto dp 
        WHERE dp.Codigo_Producto = idp.Codigo_Producto
    );
               END; 
               """)
#SP_CARGA_Dim_Pais
cursor.execute("""CREATE PROCEDURE SP_CARGA_Dim_Pais
AS
BEGIN
    -- Actualizar registros existentes en Dim_Pais
    UPDATE dp
    SET 
        dp.Codigo_Pais = idp.Codigo_Pais,
		dp.pais = idp.pais
    FROM Dim_Pais dp
    INNER JOIN INT_Dim_Pais idp ON dp.Codigo_Pais = idp.Codigo_Pais

    -- Insertar nuevos registros en Dim_Pais
    INSERT INTO Dim_Pais(Codigo_Pais, pais)
    SELECT 
			Codigo_Pais, pais
    FROM INT_Dim_Pais idp
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Dim_Pais dp 
        WHERE dp.Codigo_Pais = idp.Codigo_Pais
    );
               END;
                """)
print("SP_Carga_Dim_Pais creada con exito")

#SP_CARGA_Dim_Moneda
cursor.execute("""CREATE PROCEDURE SP_CARGA_Dim_Moneda
AS
BEGIN
    -- Actualizar registros existentes en Dim_Moneda
    UPDATE dm
    SET 
        dm.Codigo_Moneda = idm.Codigo_Moneda,
		dm.Moneda = idm.Moneda
    FROM Dim_Moneda dm
    INNER JOIN INT_Dim_Moneda idm ON dm.Codigo_Moneda = idm.Codigo_Moneda

    -- Insertar nuevos registros en Dim_Moneda
    INSERT INTO Dim_Moneda(Codigo_Moneda, Moneda)
    SELECT 
			Codigo_Moneda, Moneda
    FROM INT_Dim_Moneda idm
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Dim_Pais dm 
        WHERE dm.Codigo_Pais = idm.Codigo_Moneda
    );
               END; """
               )
print("SP_Carga_Dim_Moneda creada con exito")

#CREAR SP_CARGA_Dim_Cliente
cursor.execute("""CREATE PROCEDURE SP_CARGA_Dim_Cliente
AS
BEGIN
    -- Actualizar registros existentes en Dim_Cliente
    UPDATE dc
    SET 
	dc.Codigo_Cliente = idc.Codigo_Cliente,
	dc.Razon_Social = idc.Razon_Social,
	dc.Telefono = idc.Telefono,
	dc.Mail = idc.Mail,
	dc.Direccion = idc.Direccion,
	dc.Localidad = idc.Localidad,
	dc.Provincia = idc.Provincia,
	dc.Cp = idc.Cp 
    FROM Dim_Cliente dc
    INNER JOIN INT_Dim_Cliente idc ON idc.Codigo_Cliente = dc.Codigo_Cliente

    -- Insertar nuevos registros en Dim_Cliente
    INSERT INTO Dim_Cliente(Codigo_Cliente, Razon_Social,Telefono,Mail,Direccion,Localidad,Provincia,Cp)
    SELECT 
			Codigo_Cliente, Razon_Social,Telefono,Mail,Direccion,Localidad,Provincia,Cp
    FROM INT_Dim_Cliente idc
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Dim_Cliente dc
        WHERE dc.Codigo_Cliente = idc.Codigo_Cliente
    );
               END  """)

#SP_CARGA_Dim_codCliente
#cursor.execute("""
#               CREATE PROCEDURE SP_CARGA_Dim_codCliente
#AS
#BEGIN
#   -- Actualizar registros existentes en Dim_codCliente
#    UPDATE dcc
  #  SET 
   #     dcc.Codigo_Cliente = idcc.Codigo_Cliente,
  #      dcc.Nombre = idcc.Nombre,
	#	dcc.Apellido = idcc.Apellido,
	#	dcc.Edad = idcc.Edad,
	#	dcc.Codigo_Pais = idcc.Codigo_Pais
 #   FROM INT_Dim_codCliente dcc
  #  INNER JOIN INT_Dim_codCliente idcc ON dcc.Codigo_Cliente = idcc.Codigo_Cliente;
#
   # -- Insertar nuevos registros en Dim_Cliente
  #  INSERT INTO Dim_codCliente (Codigo_Cliente, Nombre, Apellido, Edad, Codigo_Pais)
  #  SELECT 
  #      Codigo_Cliente, Nombre, Apellido, Edad, Codigo_Pais
   # FROM INT_Dim_codCliente idcc
  #  WHERE NOT EXISTS (
   #     SELECT 1 
  #      FROM Dim_codCliente dcc
  #      WHERE dcc.Codigo_Cliente = idcc.Codigo_Cliente
 #   );
 #              END;
  #             """)
#print("SP_Carga_Dim_codCliente creada con exito")

#SP_CARGA_Dim_Cliente

####
cursor.execute("""CREATE PROCEDURE SP_CARGA_Fact_Ventas
AS
BEGIN
    -- Actualizar registros existentes en Dim_Ventas
    UPDATE fv
    SET 
	fv.Codigo_Producto =  ifv.Codigo_Producto,
	fv.cantidad = ifv.cantidad,
	fv.Precio_Venta = ifv.Precio_Venta,
	fv.Codigo_Cliente = ifv.Codigo_Cliente,
	fv.Codigo_Tienda = ifv.Codigo_Tienda,
	fv.Codigo_Pais = ifv.Codigo_Pais,
	fv.Codigo_Moneda = ifv.Codigo_Moneda,
	fv.Reseller_Key = ifv.Reseller_Key

    FROM Fact_Ventas as fv
    INNER JOIN INT_Fact_Ventas ifv ON fv.Codigo_Cliente = ifv.Codigo_Cliente;

    -- Insertar nuevos registros en Dim_Cliente
    INSERT INTO Fact_Ventas( ID_Producto, ID_Cliente , ID_Tienda , Fecha_Venta , cantidad, Precio_Venta, Codigo_Cliente, Codigo_Tienda, Codigo_Pais, Codigo_Moneda, Reseller_Key,Codigo_Producto)
    SELECT 
          id_producto, Id_Cliente,id_Tienda, ifv.Fecha_Venta, cantidad, Precio_Venta, ifv.Codigo_Cliente, ifv.Codigo_Tienda, ifv.Codigo_Pais, Codigo_Moneda, Reseller_Key,ifv.Codigo_Producto
    FROM INT_Fact_Ventas ifv
	LEFT JOIN Dim_Producto as dp on dp.Codigo_Producto = ifv.Codigo_Producto
	LEFT JOIN Dim_Cliente as dc on dc.Codigo_Cliente = ifv.Codigo_Cliente
	LEFT JOIN Dim_Tiendas as dt on dt.Codigo_Tienda = ifv.Codigo_Tienda
	LEFT JOIN Dim_Tiempo dimt on dimt.Tiempo_Key = ifv.Fecha_Venta
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Fact_Ventas fv
			WHERE fv.Codigo_Cliente = ifv.Codigo_Cliente
    );
END;
                """)

#EXEC DE TODOS LOS STORE PROCEDURES INT
cursor.execute("EXEC SP_Carga_INT_Fact_Ventas")
cursor.execute("EXEC SP_Carga_INT_Fact_Reseller")
cursor.execute("EXEC SP_Carga_INT_Fact_Distribuidor")
cursor.execute("EXEC SP_Carga_INT_Dim_Tiendas")
cursor.execute("EXEC SP_Carga_INT_Dim_Pais")
cursor.execute("EXEC SP_Carga_INT_Dim_Moneda")
cursor.execute("EXEC SP_Carga_INT_Dim_Productos")
cursor.execute("EXEC SP_Carga_INT_Dim_Cliente")
# cursor.execute("EXEC SP_Carga_INT_Dim_CodCliente")
print("SP INT CARGADOS CORRECTAMENTE")

#EXEC DE TODOS LOS STORE PROCEDURES FINALES
cursor.execute("EXEC Sp_Genera_Dim_Tiempo @anio = 2024")
cursor.execute("EXEC SP_Carga_Fact_Ventas")
cursor.execute("EXEC SP_Carga_Fact_Reseller")
cursor.execute("EXEC SP_Carga_Fact_Distribuidor")
cursor.execute("EXEC SP_Carga_Dim_Tiendas")
cursor.execute("EXEC SP_Carga_Dim_Pais")
cursor.execute("EXEC SP_Carga_Dim_Moneda")
cursor.execute("EXEC SP_Carga_Dim_Producto")
cursor.execute("EXEC SP_Carga_Dim_Cliente")
# cursor.execute("EXEC SP_Carga_Dim_codCliente")
print("SP FINALES CARGADOS Y ACTUALIZADOS CORRECTAMENTE")


#funcion commit (Guarda los datos hechos anteriormente)
conexion.commit()
#cerrar la conexion ((Siempre va Ultima))
conexion.close()