# BigData_HadoopCSV

Aplicación Java, que mediante Hadoop, halla el municipio de Huelva con mayor radiación media, y luego muestra las precipitaciones totales para ese municipio.

Los objetivos son:

a) Una lista de la medida de radiación para cada una de estos diez municipios, es decir, una
lista de la media de radiación media, en la que se muestren dos columnas: municipio y
media de la radiación media, de modo que observando esta información, tendremos una
aproximación al rendimiento que daría en ese municipio, nuestro huerto solar.

b) Queremos saber también, sólo para el municipio con mayor radiación obtenido en el
apartado anterior, las precipitaciones totales en cada año de los que contamos con datos
en el fichero.

La configuración empleada básica será la del tipo standalone.

El conjunto de datos se encuentra disponible en formato CSV para descargar desde la siguiente dirección:
http://www.juntadeandalucia.es/datosabiertos/portal/catalogo/detalle/49903.html

De todos estos datos, utilizaremos para nuestro estudio el de la radicación solar media de cada día, despreciando el resto de la información, salvo la del municipio, es decir, nuestro interés está en escoger entre las siguientes opciones solamente: Almonte (Subestación 10), Lepe (2), Gibraleón (3), Moguer (4), Niebla (5), Aroche (6), Puebla de Guzman (7), El Campillo (8) o La Palma del Condado (9).
