import pyodbc as pc
import pandas as pd
from sqlalchemy import create_engine
#creamos una variable donde guardemos la conexion al sv de SQL
server = 'TU PATH HACIA TU  USER DE SQL SERVER'
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

clientes=pd.read_csv(r".\Tablas\clientes.csv" )
codClientes=pd.read_csv(r".\Tablas\codClientes.csv")
dim_Producto = pd.read_csv(r".\Tablas\dim_producto.csv")
fact_distribuidor= pd.read_csv(r".\Tablas\fact_distribuidor.csv")
moneda = pd.read_csv(r".\Tablas\moneda.csv")
pais= pd.read_csv(r".\Tablas\pais.csv")
reseller= pd.read_csv(r".\Tablas\reseller.csv")
tiendas= pd.read_csv(r".\Tablas\tiendas.csv")
fact_ventas= pd.read_csv(r".\Tablas\ventas - ventas.csv")

#Creamos una lista con los valores de cada uno de los csv.
lista_clientes = clientes.values.tolist()
lista_codClientes = codClientes.values.tolist()
lista_dim_producto = dim_Producto.values.tolist()
lista_fact_distribuidor = fact_distribuidor.values.tolist()
lista_moneda = moneda.values.tolist()
lista_pais = pais.values.tolist()
lista_reseller =  reseller.values.tolist()
lista_tiendas = tiendas.values.tolist()
lista_fact_ventas = fact_ventas.values.tolist()



cursor.executemany("INSERT INTO STG_Fact_ventas Values(?,?,?,?,?,?,?,?,?)", lista_fact_ventas)
print("extraccion y colocacion existosa de STG_Fact_Ventas")
cursor.executemany("INSERT INTO STG_Fact_Distribuidor Values(?,?,?,?,?,?,?,?)", lista_fact_distribuidor)
print("extraccion y colocacion exitosa de STG_Fact_Distribuidor")
cursor.executemany("INSERT INTO STG_Fact_reseller Values (?,?,?,?,?,?,?,?)", lista_reseller)
print("extraccion y colocacion existosa de STG_Fact_Reseller")
cursor.executemany("INSERT INTO STG_Dim_Cliente Values (?,?,?,?,?,?,?,?)", lista_clientes)
print("extraccion y colocacion exitosa de STG_Dim_Cliente")
cursor.executemany("INSERT INTO STG_Dim_Producto Values (?,?,?,?,?,?)", lista_dim_producto)
print("extraccion y colocacion exitosa de STG_dim_Producto")
cursor.executemany("INSERT INTO STG_Dim_Tiendas Values (?,?,?,?,?)", lista_tiendas)
print("Extraccion y colocacion existosa de la tabla STG_Dim_Tiendas ")
cursor.executemany("INSERT INTO STG_Dim_Pais VALUES (?,?)",lista_pais)
print("extraccion y colocacion exitosa de la tabla STG_Dim_Pais")
cursor.executemany("INSERT INTO STG_Dim_Moneda VALUES (?,?)",lista_moneda)
print("extraccion y colocacion exitosa de la tabla STG_Dim_Moneda")

#funcion commit (Guarda los datos hechos anteriormente)
conexion.commit()
#cerrar la conexion ((Siempre va Ultima))
conexion.close()
