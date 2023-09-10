from datetime import datetime


class Pase:

    def __init__(self):
        self.usuarios = {}
        self.cad_fecha_comienzo = ""
        self.cad_fecha_fin = ""
        self.fecha_comienzo = datetime.now()
        self.fecha_fin = datetime.now()
        self.impactos_dia = 0
        self.impactos_acumulados = 0
        self.usuarios_dia = 0
        self.usuarios_dia_dif = 0
        self.unicos_acumulados = 0

    def addImpactosDia(self, num):
        self.impactos_dia += num

    def addImpactosAcumulados(self, num):
        self.impactos_acumulados += num

    def addUsuariosDia(self, num):
        self.usuarios_dia += num

    def addUsuariosDiaDif(self, num):
        self.usuarios_dia_dif += num

    def addUnicosAcumulados(self, num):
        self.unicos_acumulados += num


class FilaRepo:

    def __init__(self):
        self.pases = []


class Repo:

    def __init__(self):
        self.filas = {}
        self.nombre_repo = ""
        self.campaign_id = 0
        self.tipo_repo = 1
        self.cad_fecha_comienzo = ""
        self.cad_fecha_fin = ""
        self.fecha_comienzo = datetime.now()
        self.fecha_fin = datetime.now()
        self.max_pases = 1
