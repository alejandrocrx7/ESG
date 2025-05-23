from __future__ import annotations

import copy
from typing import Any, Callable, Dict, Iterable, List, Optional
from datetime import date, timedelta


import requests

# Tipo de las funciones de procesamiento dinámico
Procesador = Callable[[requests.Response], Any]
endpoints = [
    {
        "url": "http://servapibi.xm.com.co/hourly",
        "json_body": {
            "MetricId": "DemaReal",
            "MetricName": "Demanda Real por Sistema",
            "StartDate": "2025-02-01",
            "EndDate": "2025-03-01",
            "Entity": "Sistema",
            "Filter": []
        }
    },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "Gene",
    #         "MetricName": "Generación por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": " factorEmisionCO2e",
    #         "MetricName": " Emisiones de CO2 Eq/kWh por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "PrecBolsNaci",
    #         "MetricName": "Precio Bolsa Nacional por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "PerdidasEner",
    #         "MetricName": "Perdidas de Energía por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/daily",
    #     "json_body": {
    #         "MetricId": "AporEner",
    #         "MetricName": "Aportes Energía por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/daily",
    #     "json_body": {
    #         "MetricId": "DemaSIN",
    #         "MetricName": "Demanda Energia SIN por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/daily",
    #     "json_body": {
    #         "MetricId": "PPPrecBolsNaci",
    #         "MetricName": "Precio Bolsa Nacional Ponderado por Sistema",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Sistema",
    #         "Filter": []
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "Gene",
    #         "MetricName": "Generación por Recurso",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Recurso",
    #         "Filter": ["Codigo Submercado Generación"]
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "DispoReal",
    #         "MetricName": "Disponibilidad Real por Recurso",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Recurso",
    #         "Filter": ["Codigo Submercado Generación"]
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "ConsCombustibleMBTU",
    #         "MetricName": "Consumo Combustible MBTU por Recurso",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Recurso",
    #         "Filter": ["Codigo Submercado Generación"]
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "EmisionesCO2",
    #         "MetricName": "Emisiones de CO2 por RecursoComb",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "RecursoComb",
    #         "Filter": ["Codigo Submercado Generación"]
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "EmisionesCO2Eq",
    #         "MetricName": "Emisiones de CO2 Eq por Recurso",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Recurso",
    #         "Filter": ["Codigo Submercado Generación"]
    #     }
    # },
    # {
    #     "url": "http://servapibi.xm.com.co/hourly",
    #     "json_body": {
    #         "MetricId": "DemaReal",
    #         "MetricName": "Demanda Real por Agente",
    #         "StartDate": "2025-02-01",
    #         "EndDate": "2025-03-01",
    #         "Entity": "Agente",
    #         "Filter": ["Codigo Comercializador"]
    #     }
    # }
]
def generar_periodos_mensuales(anio_objetivo: int, años_atras: int = 1) -> list[dict]:
    """
    Devuelve una lista de diccionarios con las claves:
        • StartDate – primer día del mes (YYYY-MM-DD)
        • EndDate   – primer día del mes siguiente (YYYY-MM-DD)

    El listado cubre ‘años_atras’ años completos que finalizan justo antes
    de enero del ‘anio_objetivo’.

    Ej.: anio_objetivo = 2025, años_atras = 3  →  periodos de jun-2022 a may-2025.
    """
    # Comenzamos en el 1-ene del año objetivo y retrocedemos
    fecha_inicio_mes = date(anio_objetivo, 1, 1)
    periodos = []
    meses_totales = años_atras * 12

    for _ in range(meses_totales):
        inicio = fecha_inicio_mes
        fin = (inicio + timedelta(days=32)).replace(day=1)  # 1.er día del mes siguiente
        periodos.append({"StartDate": inicio.isoformat(), "EndDate": fin.isoformat()})
        # Retroceder al 1.er día del mes anterior
        fecha_inicio_mes = (inicio - timedelta(days=1)).replace(day=1)

    # Orden cronológico ascendente
    periodos.reverse()
    return periodos


def fetch_endpoints_por_periodo(
    endpoints: List[Dict[str, Any]],
    periodos: Iterable[Dict[str, str]],
    *,
    headers: Optional[Dict[str, str]] = None,
    metodo: str = "POST",
    timeout: int = 30,
    raise_on_error: bool = True,
    procesadores: Optional[Dict[str, Procesador]] = None,
) -> Dict[str, List[Any]]:
    """
    Lanza peticiones HTTP para cada combinación (endpoint × periodo) y
    aplica un procesador específico por MetricId cuando se provee.

    Parámetros
    ----------
    endpoints
        Lista de dicts con las claves 'url' y 'json_body'.
    periodos
        Iterable de dicts {'StartDate': ..., 'EndDate': ...}.
    headers
        Cabeceras comunes a todas las solicitudes.
    metodo
        Verbo HTTP empleado (POST por defecto).
    timeout
        Tiempo máximo de espera por solicitud (segundos).
    raise_on_error
        Si es True, lanza excepción ante códigos ≥ 400.
    procesadores
        Dict opcional que asocia `MetricId` → función(response)→resultado.

    Devuelve
    --------
    Dict[str, List[Any]]
        Mapeo MetricId → lista de resultados (Response o procesado).
    """
    metodo = metodo.upper()
    if metodo not in {"POST", "PUT", "PATCH"}:
        raise ValueError("Método HTTP no soportado para peticiones con cuerpo")

    resultados: Dict[str, List[Any]] = {}

    for ep in endpoints:
        base_body = ep["json_body"]
        metric_id = base_body.get("MetricId", ep["url"])
        url = ep["url"]

        resultados.setdefault(metric_id, [])

        proc = (procesadores or {}).get(metric_id, lambda r: r)

        for fechas in periodos:
            # Construir nuevo cuerpo sin mutar el original
            cuerpo = copy.deepcopy(base_body)
            cuerpo.update(fechas)

            try:
                resp = requests.request(
                    metodo,
                    url,
                    json=cuerpo,
                    headers=headers,
                    timeout=timeout,
                )
                if raise_on_error:
                    resp.raise_for_status()
                resultados[metric_id].append(proc(resp))

            except Exception as exc:
                print(f"Error en {metric_id} para {fechas}: {exc}")
                if raise_on_error:
                    raise

    return resultados



if __name__ == "__main__":
    # 1) Importar / definir previamente generar_periodos_mensuales
    periodos = generar_periodos_mensuales(anio_objetivo=2025, años_atras=1)

    # 2) Definir cabeceras comunes (si aplica)
    CABECERAS = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # 3) Funciones de procesamiento específicas
    def procesa_demanda(resp: requests.Response) -> dict:
        datos = resp.json()
        print(datos)
        # return {
        #     "filas": len(datos),
        #     "promedio_valor": sum(d["valor"] for d in datos) / len(datos) if datos else 0,
        # }
        return{
            
        }

    def procesa_generacion(resp: requests.Response) -> dict:
        # Ejemplo: solo devolver el JSON íntegro
        return resp.json()

    PROCESADORES = {
        "DemaReal": procesa_demanda,  # para el primer endpoint
        "Gene": procesa_generacion,   # para ambos "Gene" si así lo desea
        # añadir más según sea necesario…
    }

    # 4) Ejecutar
    resultados = fetch_endpoints_por_periodo(
        endpoints=endpoints,
        periodos=periodos,
        headers=CABECERAS,
        procesadores=PROCESADORES,
        timeout=45,
    )

    # 5) Ejemplo de inspección
    # print("Demanda Real →", resultados["DemaReal"][:2])  # primeros dos meses procesados
    # print("Generación    →", resultados["Gene"][:1])      # primer mes
