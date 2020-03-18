# -*- coding: utf-8 -*-


from scrapy.item import Item, Field

class MlibreItem(Item):

    id = Field()
    link = Field()
    producto = Field()
    marca = Field()
    modelo = Field()
    version = Field()
    precio = Field()
    km = Field()
    year = Field()
    puertas = Field()
    transmision = Field()
    direccion = Field()
    placa = Field()
    color = Field()
    vendedor = Field()
    tel_contacto = Field()
    ubicacion = Field()
    info_adicional = Field()
    descripcion = Field()
    fecha_scraping = Field()
