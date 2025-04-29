import functions_framework
import google.auth.transport.requests
import google.oauth2.id_token
import requests
import os
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)

# --- Obtenha estas variáveis do ambiente da função ---
# Ex: https://<uuid>.<region>.composer.googleusercontent.com
COMPOSER_BASE_URL = os.environ.get("COMPOSER_BASE_URL")
# Ex: gcs_event_dataflow_pubsub
TARGET_DAG_ID = os.environ.get("TARGET_DAG_ID")
# Client ID do IAP que protege a UI do Composer
IAP_CLIENT_ID = os.environ.get("IAP_CLIENT_ID")
# --- Fim das variáveis de ambiente ---

if not all([COMPOSER_BASE_URL, TARGET_DAG_ID, IAP_CLIENT_ID]):
    logging.error("Variáveis de ambiente COMPOSER_BASE_URL, TARGET_DAG_ID, e IAP_CLIENT_ID são obrigatórias.")
    # Você pode querer levantar uma exceção aqui para indicar falha na inicialização
    # raise ValueError("Missing required environment variables")


@functions_framework.cloud_event
def trigger_composer_dag(cloud_event):
    """
    Cloud Function acionada por eventos GCS para chamar um DAG no Composer.

    Args:
        cloud_event: O objeto CloudEvent contendo dados do evento GCS.
                     Espera-se que contenha 'bucket' e 'name' em cloud_event.data.
    """
    try:
        # Extrai informações do arquivo do evento GCS
        bucket = cloud_event.data.get("bucket")
        name = cloud_event.data.get("name")

        if not bucket or not name:
            logging.error(f"Evento GCS não continha 'bucket' ou 'name': {cloud_event.data}")
            return "Dados do evento GCS incompletos.", 400

        logging.info(f"Evento recebido para arquivo: gs://{bucket}/{name}")

        # Monta a URL completa da API REST do Composer para acionar o DAG
        # Garante que não haja barras duplas se COMPOSER_BASE_URL já tiver uma no final
        composer_api_url = f"{COMPOSER_BASE_URL.rstrip('/')}/api/v1/dags/{TARGET_DAG_ID}/dagRuns"

        # Prepara o corpo da requisição com a configuração para o DAG
        # O DAG espera 'bucket' e 'name' dentro de 'conf'
        request_body = {
            "conf": {
                "bucket": bucket,
                "name": name
            }
        }

        # --- Autenticação para chamar a API do Composer (protegida por IAP) ---
        try:
            # Obtém um token de ID OIDC para a conta de serviço da função,
            # especificando o Client ID do IAP como público (audience).
            auth_req = google.auth.transport.requests.Request()
            id_token = google.oauth2.id_token.fetch_id_token(auth_req, IAP_CLIENT_ID)
            headers = {
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json"
            }
            logging.info(f"Token OIDC obtido para audience: {IAP_CLIENT_ID}")
        except Exception as e:
            logging.error(f"Falha ao obter token OIDC: {e}", exc_info=True)
            return f"Erro de autenticação: {e}", 500
        # --- Fim da Autenticação ---

        # Faz a requisição POST para acionar o DAG
        logging.info(f"Acionando DAG em: {composer_api_url}")
        response = requests.post(composer_api_url, headers=headers, json=request_body, timeout=60)

        # Verifica a resposta
        if response.status_code == 200:
            logging.info(f"DAG '{TARGET_DAG_ID}' acionado com sucesso. Resposta: {response.text}")
            return "DAG acionado com sucesso.", 200
        else:
            logging.error(f"Falha ao acionar DAG '{TARGET_DAG_ID}'. Status: {response.status_code}, Resposta: {response.text}")
            # Tenta decodificar a resposta JSON se possível para mais detalhes
            try:
                error_details = response.json()
                logging.error(f"Detalhes do erro da API Airflow: {error_details}")
            except json.JSONDecodeError:
                pass # A resposta não era JSON
            return f"Erro ao acionar DAG: {response.status_code}", 500

    except Exception as e:
        logging.error(f"Erro inesperado na função: {e}", exc_info=True)
        return f"Erro interno da função: {e}", 500

