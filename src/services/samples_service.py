"""Samples Service
Servicio para interactuar con la API de samples
"""
import os

# # Native # #
import requests
from typing import Dict, Any, List
from datetime import datetime
import json

# # Installed # #
import pandas as pd

# # Project # #
from src.logger import logger
from src.models.sample import Sample


class SamplesService:
    """
    Servicio para interactuar con la API de samples
    """

    def __init__(self):
        protocol = os.getenv("SAMPLES_API_PROTOCOL", "http")
        host = os.getenv("SAMPLES_API_HOST", "localhost")
        port = os.getenv("SAMPLES_API_PORT", "5002")
        self.base_url = f"{protocol}://{host}:{port}"

    def _serialize_samples(self, samples: List[Sample]) -> List[Dict[str, Any]]:
        """
        Convierte una lista de objetos Sample a un formato JSON serializable
        compatible con Samples API.
        """

        serialized_samples: List[Dict[str, Any]] = []

        for sample in samples:

            # Convertir fecha → timestamp en milisegundos
            if isinstance(sample.fecha, datetime):
                timestamp_ms = int(sample.fecha.timestamp() * 1000)
            else:
                # Si por raro motivo es string
                try:
                    dt = datetime.fromisoformat(str(sample.fecha))
                    timestamp_ms = int(dt.timestamp() * 1000)
                except Exception:
                    timestamp_ms = 0

            serialized = {
                "timestamp": timestamp_ms,                 # fecha → timestamp ms
                "value": str(sample.valor),               # valor → value (string)
                "variable_id": str(sample.variable_id),   # variable_id → variable_id
                "categoric": False                        # requerido por la API
            }

            serialized_samples.append(serialized)

        return serialized_samples



    def save_samples(self, samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Envía una lista de samples a la API para guardarlos
        
        Args:
            samples: Lista de samples a guardar (dicts con fecha, variable_id, valor)
            
        Returns:
            Dict con la respuesta de la API o información de error
        """
        try:
            url = f"{self.base_url}/samples"
            logger.info(f"Enviando {len(samples)} samples a: {url}")

            # Serializar los samples para que sean compatibles con JSON
            serialized_samples = self._serialize_samples(samples)
            
            # Preparar el payload JSON
            payload = serialized_samples
            
            # Realizar la petición POST
            response = requests.post(
                url,
                json=payload,
                headers={
                    'Content-Type': 'application/json'
                }
            )

            # Verificar si la respuesta fue exitosa
            if response.status_code in [200, 201]:
                logger.info(f"Samples guardados exitosamente. Status: {response.status_code}")
                return {
                    "success": True,
                    "message": "Samples guardados exitosamente",
                    "samples_saved": len(samples),
                    "data": response.json() if response.text else None,
                    "status_code": response.status_code
                }
            else:
                logger.error(f"Error al guardar samples: {response.status_code}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "samples_sent": len(samples),
                    "status_code": response.status_code
                }

        except requests.exceptions.ConnectionError:
            logger.error("Error de conexión al servicio de samples")
            return {
                "success": False,
                "error": f"No se pudo conectar al servicio de samples en {self.base_url}",
                "samples_sent": len(samples),
                "status_code": None
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la petición: {str(e)}")
            return {
                "success": False,
                "error": f"Error en la petición: {str(e)}",
                "samples_sent": len(samples),
                "status_code": None
            }
        except json.JSONEncodeError as e:
            logger.error(f"Error de serialización JSON: {str(e)}")
            return {
                "success": False,
                "error": f"Error de serialización JSON: {str(e)}",
                "samples_sent": len(samples),
                "status_code": None
            }
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "samples_sent": len(samples),
                "status_code": None
            }

    def save_kafka_samples(self, samples: List[Sample]) -> Dict[str, Any]:
        """
        Guarda una lista de samples provenientes de Kafka, usando la misma
        estructura de respuesta que save_csv_samples, pero adaptada a Kafka.
        """
        try:
            if not samples:
                return {
                    "message": "No hay samples de Kafka para guardar",
                    "data": {
                        "kafka": {
                            "success": False,
                            "error": "Lista de samples vacía",
                            "samples_sent": 0,
                            "status_code": None,
                        }
                    },
                }

            logger.info(f"Guardando {len(samples)} samples provenientes de Kafka")

            # Reutilizamos la lógica de guardado ya existente
            result = self.save_samples(samples)

            success = result.get("success", False)
            if success:
                global_message = "Samples de Kafka procesados correctamente"
            else:
                global_message = "Error procesando samples de Kafka"

            return {
                "message": global_message,
                "data": {
                    "kafka": result  # mismo estilo que antes: clave -> resultado
                },
            }

        except Exception as e:
            logger.error(f"Error en save_kafka_samples: {str(e)}")
            return {
                "message": "Error procesando samples de Kafka",
                "data": {
                    "kafka": {
                        "success": False,
                        "error": f"Error procesando samples de Kafka: {str(e)}",
                        "samples_sent": 0,
                        "status_code": None,
                    }
                },
            }
