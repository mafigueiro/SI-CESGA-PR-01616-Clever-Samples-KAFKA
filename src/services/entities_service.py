"""Entities Service
Servicio para interactuar con la API de entidades
"""
import os

# # Native # #
import requests
from typing import Dict, Any, List

# # Project # #
from src.logger import logger
import time
import httpx
class EntitiesService:
    """
    Servicio para interactuar con la API de entidades
    """

    def __init__(self, cache_ttl_seconds: int = 300):
        protocol = os.getenv("ENTITIES_API_PROTOCOL", "http")
        host = os.getenv("ENTITIES_API_HOST", "localhost")
        port = os.getenv("ENTITIES_API_PORT", "5001")
        self.base_url = f"{protocol}://{host}:{port}"
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: List[Any] | None = None
        self._cache_expires_at: float = 0.0

    def _fetch_entities_from_api(self) -> list[Any]:
        url = f"{self.base_url}/entities"  # o la ruta que toque
        logger.info("Fetching entities from API", extra={"url": url})
        resp = httpx.get(url, timeout=10.0)
        resp.raise_for_status()
        return resp.json()

    def get_entities(self, force_refresh: bool = False) -> list[Any]:
        now = time.time()

        if (
                not force_refresh
                and self._cache is not None
                and now < self._cache_expires_at
        ):
            # devolver cache
            return self._cache

        # cache caducada o no existe -> llamar a la API
        entities = self._fetch_entities_from_api()
        self._cache = entities
        self._cache_expires_at = now + self.cache_ttl_seconds
        logger.info(
            "Entities cache updated",
            extra={
                "valid_for_seconds": self.cache_ttl_seconds,
                "count": len(entities),
            },
        )
        return entities

    def get_entities_variables(self, entity_ids: List[str]) -> Dict[str, Any]:
        """
        Obtiene las variables de múltiples entidades desde la API
        
        Args:
            entity_ids: Lista de IDs de entidades para obtener sus variables
            
        Returns:
            Dict con las respuestas de todas las entidades o información de error
        """
        try:
            logger.info(f"Obteniendo variables para {len(entity_ids)} entidades")
            
            results = {}
            errors = {}
            successful_requests = 0
            
            for entity_id in entity_ids:
                try:
                    url = f"{self.base_url}/entities/{entity_id}/variables"
                    logger.info(f"Realizando petición GET a: {url}")
                    
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        results[entity_id] = response.json()
                        successful_requests += 1
                    else:
                        error_msg = f"HTTP {response.status_code}: {response.text}"
                        errors[entity_id] = {
                            "error": error_msg,
                            "status_code": response.status_code
                        }
                        logger.error(f"Error al obtener variables para entidad {entity_id}: {error_msg}")
                        
                except requests.exceptions.ConnectionError:
                    error_msg = "Error de conexión al servicio de entidades"
                    errors[entity_id] = {
                        "error": error_msg,
                        "status_code": None
                    }
                    logger.error(f"Error de conexión para entidad {entity_id}")
                    
                except requests.exceptions.RequestException as e:
                    error_msg = f"Error en la petición: {str(e)}"
                    errors[entity_id] = {
                        "error": error_msg,
                        "status_code": None
                    }
                    logger.error(f"Error en petición para entidad {entity_id}: {str(e)}")
                    
                except Exception as e:
                    error_msg = f"Error inesperado: {str(e)}"
                    errors[entity_id] = {
                        "error": error_msg,
                        "status_code": None
                    }
                    logger.error(f"Error inesperado para entidad {entity_id}: {str(e)}")
            
            # Determinar si la operación fue exitosa en general
            total_entities = len(entity_ids)
            success = successful_requests > 0

            
            return {
                "success": success,
                "total_entities": total_entities,
                "successful_requests": successful_requests,
                "failed_requests": len(errors),
                "results": results,
                "errors": errors
            }
            
        except Exception as e:
            logger.error(f"Error inesperado en get_entities_variables: {str(e)}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "total_entities": len(entity_ids) if entity_ids else 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "results": {},
                "errors": {}
            }

    def get_all_entities_with_variables(self) -> Dict[str, Any]:
        """
        Obtiene todas las entidades y luego sus variables
        
        Returns:
            Dict con las entidades y sus variables, o información de error
        """
        try:
            # Primero obtener las entidades
            entities_result = self.get_entities()
            
            if not entities_result["success"]:
                return {
                    "success": False,
                    "error": f"Error al obtener entidades: {entities_result['error']}",
                    "entities": None,
                    "variables": None
                }
            
            entities_data = entities_result["data"]
            
            # Extraer los entity_ids de la respuesta
            entity_ids = [entity["entity_id"] for entity in entities_data]
            logger.info(f"Extrayendo variables para {len(entity_ids)} entidades")
            
            # Obtener las variables de todas las entidades
            variables_result = self.get_entities_variables(entity_ids)
            
            return {
                "success": True,
                "entities": entities_data,
                "variables": variables_result,
                "entity_count": len(entities_data)
            }
            
        except Exception as e:
            logger.error(f"Error en get_all_entities_with_variables: {str(e)}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "entities": None,
                "variables": None
            }

    def get_entity_variables(self, entity_id: str) -> Dict[str, Any]:

        try:
            logger.info(f"Obteniendo variables para la entidad : {entity_id}")

            result = {}
            errors = {}
            successful_request = False


            try:
                url = f"{self.base_url}/entities/{entity_id}/variables"
                logger.info(f"Realizando petición GET a: {url}")

                response = requests.get(url)

                if response.status_code == 200:
                    result = response.json()
                    successful_request = True
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    errors[entity_id] = {
                        "error": error_msg,
                        "status_code": response.status_code
                    }
                    logger.error(f"Error al obtener variables para entidad {entity_id}: {error_msg}")

            except requests.exceptions.ConnectionError:
                error_msg = "Error de conexión al servicio de entidades"
                errors[entity_id] = {
                    "error": error_msg,
                    "status_code": None
                }
                logger.error(f"Error de conexión para entidad {entity_id}")

            except requests.exceptions.RequestException as e:
                error_msg = f"Error en la petición: {str(e)}"
                errors[entity_id] = {
                    "error": error_msg,
                    "status_code": None
                }
                logger.error(f"Error en petición para entidad {entity_id}: {str(e)}")

            except Exception as e:
                error_msg = f"Error inesperado: {str(e)}"
                errors[entity_id] = {
                    "error": error_msg,
                    "status_code": None
                }
                logger.error(f"Error inesperado para entidad {entity_id}: {str(e)}")


            success = successful_request


            return {
                "success": success,
                "failed_requests": len(errors),
                "result": result,
                "errors": errors
            }

        except Exception as e:
            logger.error(f"Error inesperado en get_entities_variables: {str(e)}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "result": {},
            }