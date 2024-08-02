from pydantic import BaseModel, PositiveInt
from datetime import datetime

class Orders(BaseModel):

    data_entrega: datetime
    modal: str
    big_region: str
    logistic_region: str
    shift: str
    turno_g: str
    qtd_pedido: PositiveInt